package store

import (
	"testing"
)

func TestGet(t *testing.T) {
	t.Run("should get correct value for existing key", func(t *testing.T) {
		s := NewStore([]string{"node1", "node2", "node3"}, 1)

		_ = s.Set("key", "value", true)

		value, ok := s.Get("key")
		if !ok {
			t.Fatalf("expected key to exist")
		}
		if value != "value" {
			t.Fatalf("expected value to be 'value', got %s", value)
		}
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
			t.Fatalf("expected an error with message 'key cannot be empty', got %v", err)
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
