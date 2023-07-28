package hashring

import (
	"fmt"
	"testing"
)

func TestNewHashRingManager(t *testing.T) {
	nodes := []string{"node1", "node2", "node3"}
	hrm := NewHashRingManager(nodes)

	if hrm.Len() != len(nodes)*VirtualNodesFactor {
		t.Errorf("Mismatch in hash ring length, expected: %d, received: %d", len(nodes)*VirtualNodesFactor, hrm.Len())
	}

	for _, node := range nodes {
		for vn := 0; vn < VirtualNodesFactor; vn++ {
			virtualNodeKey := fmt.Sprintf("%s#%d", node, vn)
			hash := hrm.HashStr(virtualNodeKey)
			nodeMap, ok := hrm.hashMap[hash]
			if !ok {
				t.Errorf("Node map missing for hash: %d", hash)
			}

			if nodeMap.Node != node {
				t.Errorf("Mismatch in node name, expected: %s, received: %s", node, nodeMap.Node)
			}

			if nodeMap.VirtualNodeID != vn {
				t.Errorf("Mismatch in virtual node ID, expected: %d, received: %d", vn, nodeMap.VirtualNodeID)
			}
		}
	}
}

func TestNewHashRingManagerWithEmptyNodes(t *testing.T) {
	nodes := []string{}
	hrm := NewHashRingManager(nodes)

	if hrm.Len() != 0 {
		t.Errorf("Expected length of 0 for empty hash ring, received: %d", hrm.Len())
	}
}

func TestRemoveNonExistingNode(t *testing.T) {
	nodes := []string{"node1", "node2", "node3"}
	hrm := NewHashRingManager(nodes)

	hrm.RemoveNode("node4")

	if hrm.Len() != len(nodes)*VirtualNodesFactor {
		t.Errorf("Hash ring length mismatch after attempting to remove a non-existent node, expected: %d, received: %d", len(nodes)*VirtualNodesFactor, hrm.Len())
	}

	for _, node := range nodes {
		for vn := 0; vn < VirtualNodesFactor; vn++ {
			virtualNodeKey := fmt.Sprintf("%s#%d", node, vn)
			hash := hrm.HashStr(virtualNodeKey)
			nodeMap, ok := hrm.hashMap[hash]
			if !ok {
				t.Errorf("Node map missing for hash: %d", hash)
			}

			if nodeMap.Node != node {
				t.Errorf("Mismatch in node name, expected: %s, received: %s", node, nodeMap.Node)
			}

			if nodeMap.VirtualNodeID != vn {
				t.Errorf("Mismatch in virtual node ID, expected: %d, received: %d", vn, nodeMap.VirtualNodeID)
			}
		}
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
		t.Errorf("Error getting ring index: %v", err)
	}

	nodeMap := hrm.hashMap[hrm.ring[index]]
	if nodeMap.Node != "node1" || nodeMap.VirtualNodeID != 0 {
		t.Errorf("Mismatch in node map, expected: {node1 0}, received: {%s %d}", nodeMap.Node, nodeMap.VirtualNodeID)
	}
}

func TestRemoveNode(t *testing.T) {
	testCases := []struct {
		desc           string
		initialNodes   []string
		nodeToRemove   string
		expectedLength int
	}{
		{
			desc:           "remove existing node",
			initialNodes:   []string{"node1", "node2", "node3"},
			nodeToRemove:   "node1",
			expectedLength: 2 * VirtualNodesFactor,
		},
		{
			desc:           "remove non-existing node",
			initialNodes:   []string{"node1", "node2", "node3"},
			nodeToRemove:   "node4",
			expectedLength: 3 * VirtualNodesFactor,
		},
	}

	for _, testcase := range testCases {
		t.Run(testcase.desc, func(t *testing.T) {
			hrm := NewHashRingManager(testcase.initialNodes)

			hrm.RemoveNode(testcase.nodeToRemove)

			if hrm.Len() != testcase.expectedLength {
				t.Errorf("Mismatch in hash ring length after removal, expected: %d, got: %d", testcase.expectedLength, hrm.Len())
			}

			for vn := 0; vn < VirtualNodesFactor; vn++ {
				virtualNodeKey := fmt.Sprintf("%s#%d", testcase.nodeToRemove, vn)
				hash := hrm.HashStr(virtualNodeKey)
				_, ok := hrm.hashMap[hash]
				if ok {
					t.Errorf("Node map still exists for removed node, hash: %d", hash)
				}
			}
		})
	}
}
