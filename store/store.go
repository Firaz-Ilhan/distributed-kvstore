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
	readQuorum        int
	writeQuorum       int
}

type MultiError []error

func (me MultiError) Error() string {
	errs := make([]string, len(me))
	for i, err := range me {
		errs[i] = err.Error()
	}
	return strings.Join(errs, "; ")
}

/*
Initializes and returns a new Store instance. It sets the
read and write quorums based on the number of nodes,
and initializes the hashing ring for the nodes.
*/
func NewStore(nodes []string, replicationFactor int) *Store {
	halfNodes := len(nodes) / 2
	readQuorum := halfNodes + 1
	writeQuorum := halfNodes + 1

	if len(nodes) == 1 {
		readQuorum = 1
		writeQuorum = 1
		replicationFactor = 0
	}

	s := &Store{
		data:              make(map[string]string),
		nodes:             nodes,
		client:            &http.Client{Timeout: 2 * time.Second},
		ringManager:       hashring.NewHashRingManager(nodes),
		replicationFactor: replicationFactor,
		readQuorum:        readQuorum,
		writeQuorum:       writeQuorum,
	}
	return s
}

/*
Get retrieves a value from the store based on the provided key. It returns
the value and a boolean indicating if the key was found in the store.
*/
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.data[key]
	return val, ok
}

/*
Adds or updates a key-value pair in the store. If skipReplication is false, it will
attempt to replicate the operation to other nodes in the distributed system.
It will return an error if there's a problem with the operation or the replication.
*/
func (s *Store) Set(key string, value string, skipReplication bool) error {
	if key == "" || value == "" {
		return errors.New("key or value cannot be empty")
	}

	s.mu.Lock()
	s.data[key] = value
	s.mu.Unlock()

	return s.handleReplication(skipReplication, "PUT", key, value)
}

/*
Removes a key from the store. If skipReplication is false, it will
attempt to replicate the delete operation to other nodes in the distributed system.
It will return an error if there's a problem with the operation or the replication.
*/
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

/*
replicates a given operation for a specific key-value pair to a given node.
*/
func (s *Store) replicateNode(node, method, key, value string, errs chan<- error) {
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
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		errs <- fmt.Errorf("failed to replicate to %s: status code %d", node, resp.StatusCode)
		return
	}

	errs <- nil
}

/*
handles the replication of a given operation for a specific
key-value pair across the distributed nodes based on the replication factor.
*/
func (s *Store) replicate(method, key, value string) error {
	if s.replicationFactor == 0 {
		return nil
	}

	errs := make(chan error, s.replicationFactor)
	var wg sync.WaitGroup

	hash := s.ringManager.HashStr(key)
	idx, err := s.ringManager.GetRingIndex(hash)
	if err != nil {
		return err
	}

	selectedNodes := make(map[string]struct{})
	for i := 0; i < s.replicationFactor; {
		nodeMap, err := s.ringManager.GetNodeMapForRingIndex((idx + i) % s.ringManager.Len())
		if err != nil {
			return err
		}
		node := nodeMap.Node

		if _, alreadySelected := selectedNodes[node]; !alreadySelected {
			fmt.Printf("Replicating to: %s\n", node)
			selectedNodes[node] = struct{}{}
			wg.Add(1)
			go func(node string) {
				defer wg.Done()
				s.replicateNode(node, method, key, value, errs)
			}(node)
			i++
		} else {
			idx++
		}
	}

	go func() {
		wg.Wait()
		close(errs)
	}()

	var multiErr MultiError
	successCount := 0
	for err := range errs {
		if err == nil {
			successCount++
		} else {
			multiErr = append(multiErr, err)
		}
	}

	if successCount < s.writeQuorum {
		return fmt.Errorf("not enough replicas for write quorum: %d", successCount)
	}
	if len(multiErr) > 0 {
		return multiErr
	}
	return nil
}
