package main

import (
	"context"
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
	var validNodes []string
	for _, node := range nodes {
		if node != "" {
			validNodes = append(validNodes, node)
		}
	}
	return &Store{
		data:  make(map[string]string),
		nodes: validNodes,
	}
}

type MultiError []error

func (me MultiError) Error() string {
	errs := []string{}
	for _, err := range me {
		errs = append(errs, err.Error())
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
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
	if !skipReplication {
		err := s.replicate("PUT", key, value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) Delete(key string, skipReplication bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
	if !skipReplication {
		err := s.replicate("DELETE", key, "")
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) replicate(method string, key string, value string) MultiError {
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

			if resp != nil && resp.Body != nil {
				defer resp.Body.Close()
			}

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
	if len(multiErr) == 0 {
		return nil
	}
	return multiErr
}

func handleGet(w http.ResponseWriter, r *http.Request, store *Store) {
	key := r.URL.Path[1:]
	value, ok := store.Get(key)
	if !ok {
		http.Error(w, "Not found", http.StatusNotFound)
		return
	}
	w.Write([]byte(value))
}

func handlePut(w http.ResponseWriter, r *http.Request, store *Store) {
	key := r.URL.Path[1:]
	value, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	skipReplication := r.Header.Get("X-Replication") == "true"
	err = store.Set(key, string(value), skipReplication)
	if err != nil {
		http.Error(w, "Failed to set value", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func handleDelete(w http.ResponseWriter, r *http.Request, store *Store) {
	key := r.URL.Path[1:]
	store.Delete(key, false)
}

func main() {
	var port int
	var nodesStr string
	flag.IntVar(&port, "port", 8080, "Port to listen on")
	flag.StringVar(&nodesStr, "nodes", "", "Comma-separated list of other nodes")
	flag.Parse()

	var validNodes []string
	for _, node := range strings.Split(nodesStr, ",") {
		if node != "" {
			validNodes = append(validNodes, node)
		}
	}

	store := NewStore(validNodes)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleGet(w, r, store)
		case http.MethodPut:
			handlePut(w, r, store)
		case http.MethodDelete:
			handleDelete(w, r, store)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	log.Printf("Listening on port %d", port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", port), nil))
}
