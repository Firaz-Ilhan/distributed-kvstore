package hashring

import (
	"fmt"
	"testing"
)

func checkRingConsistency(t *testing.T, hrm *HashRingManager, node string, shouldBePresent bool) {
	for vn := 0; vn < VirtualNodesFactor; vn++ {
		virtualNodeKey := fmt.Sprintf("%s#%d", node, vn)
		hash := hrm.HashStr(virtualNodeKey)
		_, ok := hrm.hashMap[hash]
		if shouldBePresent && !ok {
			t.Errorf("Expected to find hash for %s, but it was missing", virtualNodeKey)
		} else if !shouldBePresent && ok {
			t.Errorf("Expected not to find hash for %s, but it was present", virtualNodeKey)
		}
	}
}

func TestNewHashRingManager(t *testing.T) {
	tests := []struct {
		nodes       []string
		expectedLen int
	}{
		{
			nodes:       []string{"node1", "node2", "node3"},
			expectedLen: 3 * VirtualNodesFactor,
		},
		{
			nodes:       []string{},
			expectedLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("TestNewHashRingManager with nodes: %v", tt.nodes), func(t *testing.T) {
			hrm := NewHashRingManager(tt.nodes)
			if hrm.Len() != tt.expectedLen {
				t.Errorf("Expected length %d, got %d", tt.expectedLen, hrm.Len())
			}

			for _, node := range tt.nodes {
				checkRingConsistency(t, hrm, node, true)
			}
		})
	}
}

func TestModifyNode(t *testing.T) {
	tests := []struct {
		desc            string
		initialNodes    []string
		modifyFunc      func(hrm *HashRingManager)
		nodeToCheck     string
		shouldBePresent bool
		expectedLen     int
	}{
		{
			desc:            "Add new node",
			initialNodes:    []string{"node1", "node2", "node3"},
			modifyFunc:      func(hrm *HashRingManager) { hrm.AddNode("node4") },
			nodeToCheck:     "node4",
			shouldBePresent: true,
			expectedLen:     4 * VirtualNodesFactor,
		},
		{
			desc:            "Remove non-existing node",
			initialNodes:    []string{"node1", "node2", "node3"},
			modifyFunc:      func(hrm *HashRingManager) { hrm.RemoveNode("node4") },
			nodeToCheck:     "node4",
			shouldBePresent: false,
			expectedLen:     3 * VirtualNodesFactor,
		},
		{
			desc:            "Remove existing node",
			initialNodes:    []string{"node1", "node2", "node3"},
			modifyFunc:      func(hrm *HashRingManager) { hrm.RemoveNode("node1") },
			nodeToCheck:     "node1",
			shouldBePresent: false,
			expectedLen:     2 * VirtualNodesFactor,
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			hrm := NewHashRingManager(tt.initialNodes)
			tt.modifyFunc(hrm)
			checkRingConsistency(t, hrm, tt.nodeToCheck, tt.shouldBePresent)
			if hrm.Len() != tt.expectedLen {
				t.Errorf("Expected length %d, got %d", tt.expectedLen, hrm.Len())
			}
		})
	}
}

func TestGetNodeMapForRingIndex(t *testing.T) {
	nodes := []string{"node1", "node2", "node3"}
	hrm := NewHashRingManager(nodes)

	nodeMap, err := hrm.GetNodeMapForRingIndex(0)
	if err != nil {
		t.Fatalf("Node map retrieval error for ring index 0: %v", err)
	}

	if !contains(nodes, nodeMap.Node) {
		t.Errorf("Node map contains unexpected node: %s", nodeMap.Node)
	}

	if nodeMap.VirtualNodeID < 0 || nodeMap.VirtualNodeID >= VirtualNodesFactor {
		t.Errorf("Virtual node ID out of range, received: %d", nodeMap.VirtualNodeID)
	}

	_, err = hrm.GetNodeMapForRingIndex(hrm.Len())
	if err == nil {
		t.Errorf("Expected error for out-of-range index, received: nil")
	}
}

func contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}
	return false
}

func TestGetRingIndex(t *testing.T) {
	nodes := []string{"node1", "node2", "node3"}
	hrm := NewHashRingManager(nodes)
	index, err := hrm.GetRingIndex(hrm.HashStr("node1#0"))
	if err != nil {
		t.Fatalf("Failed to get ring index: %v", err)
	}
	nodeMap := hrm.hashMap[hrm.ring[index]]
	if nodeMap.Node != "node1" || nodeMap.VirtualNodeID != 0 {
		t.Errorf("Expected {node1 0}, got {%s %d}", nodeMap.Node, nodeMap.VirtualNodeID)
	}
}

func TestHasNode(t *testing.T) {
	nodes := []string{"node1", "node2", "node3"}
	hrm := NewHashRingManager(nodes)
	if !hrm.HasNode("node1") || hrm.HasNode("node4") {
		t.Error("HasNode did not return the expected results")
	}
}

func TestRingConsistencyAfterAddRemove(t *testing.T) {
	nodes := []string{"node1", "node2"}
	hrm := NewHashRingManager(nodes)

	hrm.AddNode("node3")
	hrm.RemoveNode("node1")

	if hrm.HasNode("node1") {
		t.Errorf("Expected node1 to be removed from the hash ring manager")
	}

	if !hrm.HasNode("node3") {
		t.Errorf("Expected node3 to be added to the hash ring manager")
	}

	expectedLen := 2 * VirtualNodesFactor
	if hrm.Len() != expectedLen {
		t.Errorf("Mismatch in hash ring length, expected: %d, got: %d", expectedLen, hrm.Len())
	}
}

func TestAddExistingNode(t *testing.T) {
	nodes := []string{"node1", "node2", "node3"}
	hrm := NewHashRingManager(nodes)

	hrm.AddNode("node1")

	if hrm.Len() != len(nodes)*VirtualNodesFactor {
		t.Errorf("Mismatch in hash ring length after attempting to add an existing node, expected: %d, received: %d", len(nodes)*VirtualNodesFactor, hrm.Len())
	}
}
