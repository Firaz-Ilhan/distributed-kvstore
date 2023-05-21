package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

type Store struct {
	mu    sync.RWMutex
	data  map[string]string
	nodes []string
}

func NewStore(nodes []string) *Store {
	return &Store{
		data:  make(map[string]string),
		nodes: nodes,
	}
}

type MultiError []error

func (me MultiError) Error() string {
	errs := make([]string, len(me))
	for i, err := range me {
		errs[i] = err.Error()
	}
	return strings.Join(errs, "; ")
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
	defer s.mu.Unlock()
	s.data[key] = value
	if !skipReplication {
		err := s.replicate("PUT", key, value)
		if err != nil {
			return fmt.Errorf("failed to set value with replication: %v", err)
		}
	}
	return nil
}

func (s *Store) Delete(key string, skipReplication bool) error {
	if key == "" {
		return errors.New("key cannot be empty")
	}

	s.mu.Lock()
	delete(s.data, key)
	s.mu.Unlock()

	if !skipReplication {
		go func() {
			if err := s.replicate("DELETE", key, ""); err != nil {
				log.Printf("Failed to replicate DELETE operation for key %s: %v", key, err)
			}
		}()
	}
	return nil
}

func (s *Store) replicate(method, key, value string) error {
	var wg sync.WaitGroup
	errs := make(chan error, len(s.nodes))

	client := &http.Client{
		Timeout: 2 * time.Second,
	}

	for _, node := range s.nodes {
		wg.Add(1)
		go func(node string) {
			defer wg.Done()

			url := fmt.Sprintf("http://%s/%s", node, key)
			req, err := http.NewRequestWithContext(context.Background(), method, url, strings.NewReader(value))
			if err != nil {
				errs <- fmt.Errorf("failed to create request: %w", err)
				return
			}

			req.Header.Set("X-Replication", "true")

			resp, err := client.Do(req)
			if err != nil {
				errs <- fmt.Errorf("failed to replicate to %s: %w", node, err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode >= 400 {
				errs <- fmt.Errorf("failed to replicate to %s: status code %d", node, resp.StatusCode)
			}
		}(node)
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

type handler struct {
	store *Store
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.handleGet(w, r)
	case http.MethodPut:
		h.handlePut(w, r)
	case http.MethodDelete:
		h.handleDelete(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *handler) handleGet(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimSpace(r.URL.Path[1:])
	value, ok := h.store.Get(key)
	if !ok {
		http.Error(w, "Not found", http.StatusNotFound)
		return
	}
	w.Write([]byte(value))
}

func (h *handler) handlePut(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimSpace(r.URL.Path[1:])
	value, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	skipReplication := r.Header.Get("X-Replication") == "true"
	err = h.store.Set(key, string(value), skipReplication)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (h *handler) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimSpace(r.URL.Path[1:])
	skipReplication := r.Header.Get("X-Replication") == "true"
	err := h.store.Delete(key, skipReplication)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func main() {
	var port int
	var nodesStr string
	flag.IntVar(&port, "port", 8080, "Port to listen on")
	flag.StringVar(&nodesStr, "nodes", "", "Comma-separated list of other nodes")
	flag.Parse()

	store := NewStore(strings.Split(nodesStr, ","))

	h := &handler{
		store: store,
	}

	http.Handle("/", h)

	log.Printf("Listening on port %d", port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", port), nil))
}
