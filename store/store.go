package store

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Firaz-Ilhan/distributed-kvstore/hashring"
)

const (
	ReplicationHeader = "X-Replication"
)

type Store struct {
	mu                sync.RWMutex
	data              map[string]string
	nodes             []string
	client            *http.Client
	ringManager       *hashring.HashRingManager
	replicationFactor int
}

type MultiError []error

func (me MultiError) Error() string {
	errs := make([]string, len(me))
	for i, err := range me {
		errs[i] = err.Error()
	}
	return strings.Join(errs, "; ")
}

func NewStore(nodes []string, replicationFactor int) *Store {
	s := &Store{
		data:              make(map[string]string),
		nodes:             nodes,
		client:            &http.Client{Timeout: 2 * time.Second},
		ringManager:       hashring.NewHashRingManager(nodes),
		replicationFactor: replicationFactor,
	}
	return s
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.data[key]
	return val, ok
}

func (s *Store) Set(key string, value string, skipReplication bool) error {
	if key == "" || value == "" {
		return errors.New("key or value cannot be empty")
	}

	s.mu.Lock()
	s.data[key] = value
	s.mu.Unlock()

	return s.handleReplication(skipReplication, "PUT", key, value)
}

func (s *Store) Delete(key string, skipReplication bool) error {
	if key == "" {
		return errors.New("key cannot be empty")
	}

	s.mu.Lock()
	delete(s.data, key)
	s.mu.Unlock()

	return s.handleReplication(skipReplication, "DELETE", key, "")
}

func (s *Store) handleReplication(skipReplication bool, method, key, value string) error {
	if !skipReplication {
		err := s.replicate(method, key, value)
		if err != nil {
			log.Printf("Failed to replicate %s operation for key %s: %v", method, key, err)
			return fmt.Errorf("failed to set value with replication: %v", err)
		}
	}
	return nil
}

func (s *Store) replicateNode(node, method, key, value string, errs chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()

	url := fmt.Sprintf("http://%s/%s", node, key)
	req, err := http.NewRequestWithContext(context.Background(), method, url, strings.NewReader(value))
	if err != nil {
		errs <- fmt.Errorf("failed to create request: %w", err)
		return
	}

	req.Header.Set(ReplicationHeader, "true")

	resp, err := s.client.Do(req)
	if err != nil {
		errs <- fmt.Errorf("failed to replicate to %s: %w", node, err)
		return
	}

	resp.Body.Close()

	if resp.StatusCode >= 400 {
		errs <- fmt.Errorf("failed to replicate to %s: status code %d", node, resp.StatusCode)
	}
}

func (s *Store) replicate(method, key, value string) error {
	var wg sync.WaitGroup
	errs := make(chan error, s.replicationFactor)

	hash := s.ringManager.HashStr(key)
	idx, err := s.ringManager.GetRingIndex(hash)
	if err != nil {
		return err
	}

	for i := 0; i < s.replicationFactor; i++ {
		nodeMap, err := s.ringManager.GetNodeMapForRingIndex((idx + i) % s.ringManager.Len())
		if err != nil {
			return err
		}
		node := nodeMap.Node
		wg.Add(1)
		go s.replicateNode(node, method, key, value, errs, &wg)
	}

	wg.Wait()
	close(errs)

	var multiErr MultiError
	for err := range errs {
		multiErr = append(multiErr, err)
	}

	if len(multiErr) > 0 {
		return multiErr
	}
	return nil
}
