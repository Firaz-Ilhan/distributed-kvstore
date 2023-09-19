package handler

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/Firaz-Ilhan/distributed-kvstore/store"
)

type ErrorResponse struct {
	Error string `json:"error"`
}

func writeJSONError(w http.ResponseWriter, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(ErrorResponse{Error: message})
}

type Storer interface {
	Get(key string) (value string, ok bool)
	Set(key, value string, skipReplication bool) error
	Delete(key string, skipReplication bool) error
}

type Handler struct {
	Store Storer
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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

func (h *Handler) handleGet(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimSpace(r.URL.Path[1:])
	value, ok := h.Store.Get(key)
	if !ok {
		writeJSONError(w, "Not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"value": value})
}

func (h *Handler) handlePut(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimSpace(r.URL.Path[1:])
	_, exists := h.Store.Get(key)

	value, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		writeJSONError(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	skipReplication := r.Header.Get(store.ReplicationHeader) == "true"
	err = h.Store.Set(key, string(value), skipReplication)
	if err != nil {
		writeJSONError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if exists {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusCreated)
	}
}

func (h *Handler) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimSpace(r.URL.Path[1:])
	skipReplication := r.Header.Get(store.ReplicationHeader) == "true"
	err := h.Store.Delete(key, skipReplication)
	if err != nil {
		writeJSONError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
