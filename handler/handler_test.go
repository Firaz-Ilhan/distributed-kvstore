package handler

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

type MockStore struct {
	data map[string]string
}

func (s *MockStore) Get(key string) (value string, ok bool) {
	value, ok = s.data[key]
	return
}

func (s *MockStore) Set(key, value string, skipReplication bool) error {
	s.data[key] = value
	return nil
}

func (s *MockStore) Delete(key string, skipReplication bool) error {
	delete(s.data, key)
	return nil
}

func NewMockStore() *MockStore {
	return &MockStore{
		data: make(map[string]string),
	}
}

func setupRequestAndRecorder(method, path, body string) (*http.Request, *httptest.ResponseRecorder) {
	req, _ := http.NewRequest(method, path, bytes.NewBufferString(body))
	return req, httptest.NewRecorder()
}

func assertStatusCode(t *testing.T, got, want int) {
	t.Helper()
	if got != want {
		t.Errorf("handler returned wrong status code: got %v want %v", got, want)
	}
}

func assertResponseBody(t *testing.T, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("handler returned unexpected body: got %v want %v", got, want)
	}
}

func TestHandler_ServeHTTP(t *testing.T) {
	h := &Handler{Store: NewMockStore()}

	req, rr := setupRequestAndRecorder(http.MethodPut, "/test", "value")
	h.ServeHTTP(rr, req)
	assertStatusCode(t, rr.Code, http.StatusCreated)

	req, rr = setupRequestAndRecorder(http.MethodGet, "/test", "")
	h.ServeHTTP(rr, req)
	assertStatusCode(t, rr.Code, http.StatusOK)

	var response map[string]string
	err := json.Unmarshal(rr.Body.Bytes(), &response)
	if err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	assertResponseBody(t, response["value"], "value")

	req, rr = setupRequestAndRecorder(http.MethodDelete, "/test", "")
	h.ServeHTTP(rr, req)
	assertStatusCode(t, rr.Code, http.StatusOK)

	req, rr = setupRequestAndRecorder(http.MethodGet, "/test", "")
	h.ServeHTTP(rr, req)
	assertStatusCode(t, rr.Code, http.StatusNotFound)

	var errorResponse ErrorResponse
	err = json.Unmarshal(rr.Body.Bytes(), &errorResponse)
	if err != nil {
		t.Fatalf("Failed to unmarshal error response: %v", err)
	}

	assertResponseBody(t, errorResponse.Error, "Not found")

	// Test unsupported method
	req, rr = setupRequestAndRecorder(http.MethodPost, "/test", "data")
	h.ServeHTTP(rr, req)
	assertStatusCode(t, rr.Code, http.StatusMethodNotAllowed)
}
