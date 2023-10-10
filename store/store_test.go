package store

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

type MockHttpClient struct {
	doFunc  func(req *http.Request) (*http.Response, error)
	getFunc func(url string) (*http.Response, error)
}

func (m *MockHttpClient) Do(req *http.Request) (*http.Response, error) {
	return m.doFunc(req)
}

func (m *MockHttpClient) Get(url string) (*http.Response, error) {
	return m.getFunc(url)
}

func assertEqual(t *testing.T, got, want interface{}, msg string) {
	t.Helper()
	if got != want {
		t.Errorf("%s: got %v, want %v", msg, got, want)
	}
}

func TestGet(t *testing.T) {
	t.Run("should get correct value for existing key", func(t *testing.T) {
		s := NewStore([]string{"node1", "node2", "node3"}, 1)

		_ = s.Set("key", "value", true)

		value, ok := s.Get("key")
		assertEqual(t, ok, true, "key existence check")
		assertEqual(t, value, "value", "retrieved value")
	})
}

func TestDelete(t *testing.T) {
	t.Run("should delete existing key", func(t *testing.T) {
		s := NewStore([]string{"node1", "node2", "node3"}, 1)

		_ = s.Set("key", "value", true)
		err := s.Delete("key", true)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		_, ok := s.Get("key")
		if ok {
			t.Fatalf("expected key to be deleted")
		}
	})

	t.Run("should not accept empty key", func(t *testing.T) {
		s := NewStore([]string{"node1", "node2", "node3"}, 1)

		err := s.Delete("", true)
		if err == nil || err.Error() != "key cannot be empty" {
			t.Fatalf("expected an error with the message 'key cannot be empty', got %v", err)
		}
	})
}

func TestSet(t *testing.T) {
	t.Run("should set key-value", func(t *testing.T) {
		s := NewStore([]string{"node1", "node2", "node3"}, 1)

		err := s.Set("key", "value", true)
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		value, ok := s.Get("key")
		assertEqual(t, ok, true, "key existence check after set")
		assertEqual(t, value, "value", "retrieved value after set")
	})

	t.Run("should not accept empty key", func(t *testing.T) {
		s := NewStore([]string{"node1", "node2", "node3"}, 1)

		err := s.Set("", "value", true)
		if err == nil || err.Error() != "key or value cannot be empty" {
			t.Errorf("expected an error with message 'key or value cannot be empty', got %v", err)
		}
	})

	t.Run("should not accept empty value", func(t *testing.T) {
		s := NewStore([]string{"node1", "node2", "node3"}, 1)

		err := s.Set("key", "", true)
		if err == nil || err.Error() != "key or value cannot be empty" {
			t.Errorf("expected an error with message 'key or value cannot be empty', got %v", err)
		}
	})
}

func TestNewStore(t *testing.T) {
	nodes := []string{"node1", "node2", "node3"}
	replicationFactor := 3
	s := NewStore(nodes, replicationFactor)

	if s == nil {
		t.Fatalf("expected Store to be created, got nil")
	}

	if len(s.nodes) != len(nodes) {
		t.Fatalf("expected nodes to be %v, got %v", nodes, s.nodes)
	}

	if s.replicationFactor != replicationFactor {
		t.Fatalf("expected replicationFactor to be %d, got %d", replicationFactor, s.replicationFactor)
	}

	if s.client == nil {
		t.Fatalf("expected client to be created, got nil")
	}

	if s.ringManager == nil {
		t.Fatalf("expected ringManager to be created, got nil")
	}
}

func TestQuorumCalculation(t *testing.T) {
	tests := []struct {
		nodes               []string
		expectedReadQuorum  int
		expectedWriteQuorum int
	}{
		{
			nodes:               []string{"node1"},
			expectedReadQuorum:  1,
			expectedWriteQuorum: 1,
		},
		{
			nodes:               []string{"node1", "node2"},
			expectedReadQuorum:  2,
			expectedWriteQuorum: 2,
		},
		{
			nodes:               []string{"node1", "node2", "node3"},
			expectedReadQuorum:  2,
			expectedWriteQuorum: 2,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("nodes: %v", test.nodes), func(t *testing.T) {
			s := NewStore(test.nodes, 1)
			if s.readQuorum != test.expectedReadQuorum {
				t.Errorf("expected readQuorum to be %d, got %d", test.expectedReadQuorum, s.readQuorum)
			}
			if s.writeQuorum != test.expectedWriteQuorum {
				t.Errorf("expected writeQuorum to be %d, got %d", test.expectedWriteQuorum, s.writeQuorum)
			}
		})
	}
}

func TestReplicate(t *testing.T) {
	t.Run("should replicate data to nodes successfully", func(t *testing.T) {
		nodes := []string{"node1", "node2"}
		s := NewStore(nodes, 2)

		s.client = &MockHttpClient{
			doFunc: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader([]byte{})),
				}, nil
			},
		}

		err := s.replicate("PUT", "key", "value")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})

	t.Run("should return error when replication fails", func(t *testing.T) {
		nodes := []string{"node1", "node2"}
		s := NewStore(nodes, 1)

		s.client = &MockHttpClient{
			doFunc: func(req *http.Request) (*http.Response, error) {
				return nil, fmt.Errorf("network error")
			},
		}

		err := s.replicate("PUT", "key", "value")
		if err == nil || !strings.Contains(err.Error(), "not enough replicas for write quorum") {
			t.Errorf("expected a 'not enough replicas for write quorum' error, got %v", err)
		}
	})
}

func TestHandleReplication(t *testing.T) {
	t.Run("should attempt replication when skipReplication is false", func(t *testing.T) {
		nodes := []string{"node1", "node2", "node3", "node4"}
		s := NewStore(nodes, 3)

		s.client = &MockHttpClient{
			doFunc: func(req *http.Request) (*http.Response, error) {
				return nil, fmt.Errorf("this error should be triggered")
			},
		}

		err := s.handleReplication(false, "PUT", "key", "value")
		if err == nil {
			t.Errorf("expected an error but got nil")
		} else if !strings.Contains(err.Error(), "this error should be triggered") &&
			!strings.Contains(err.Error(), "not enough replicas for write quorum") {
			t.Errorf("unexpected error, got %v", err)
		}
	})
}
