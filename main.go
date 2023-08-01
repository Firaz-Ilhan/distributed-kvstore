package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Firaz-Ilhan/distributed-kvstore/store"
)

type handler struct {
	store *store.Store
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
	skipReplication := r.Header.Get(store.ReplicationHeader) == "true"
	err = h.store.Set(key, string(value), skipReplication)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (h *handler) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimSpace(r.URL.Path[1:])
	skipReplication := r.Header.Get(store.ReplicationHeader) == "true"
	err := h.store.Delete(key, skipReplication)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("Request: %s %s", r.Method, r.URL)
		statusWriter := &statusResponseWriter{ResponseWriter: w}
		next.ServeHTTP(statusWriter, r)
		log.Printf("Response: %d", statusWriter.status)
	})
}

type statusResponseWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusResponseWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func main() {
	var port int
	var nodesStr string
	var replicationFactor int
	flag.IntVar(&port, "port", 8080, "Port to listen on")
	flag.StringVar(&nodesStr, "nodes", "", "Comma-separated list of other nodes")
	flag.IntVar(&replicationFactor, "replicationFactor", 2, "Replication factor")
	flag.Parse()

	store := store.NewStore(strings.Split(nodesStr, ","), replicationFactor)

	go store.HealthCheck()

	h := &handler{
		store: store,
	}

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "OK")
	})

	http.Handle("/", loggingMiddleware(h))

	server := &http.Server{
		Addr: fmt.Sprintf(":%d", port),
	}

	go func() {
		log.Printf("Listening on port %d", port)
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Printf("ListenAndServe(): %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	sig := <-quit
	log.Printf("Server is shutting down (%v)...", sig)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Could not gracefully shutdown the server: %v\n", err)
	}

	log.Printf("Server stopped.")
}
