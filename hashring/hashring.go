/*
Implements a consistent hashing mechanism. It provides functionality to manage a ring
of hashed values both from actual nodes and virtual nodes.
*/
package hashring

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
)

const (
	VirtualNodesFactor = 100
)

type NodeMap struct {
	Node          string
	VirtualNodeID int
}

type HashRing []uint32

func (hr HashRing) Len() int {
	return len(hr)
}

func (hr HashRing) Less(i, j int) bool {
	return hr[i] < hr[j]
}

func (hr HashRing) Swap(i, j int) {
	hr[i], hr[j] = hr[j], hr[i]
}

type HashRingManager struct {
	hashMap     map[uint32]NodeMap
	ring        HashRing
	nodes       []string
	mutex       sync.RWMutex
	activeNodes map[string]struct{}
}

func NewHashRingManager(nodes []string) *HashRingManager {
	h := &HashRingManager{
		hashMap:     make(map[uint32]NodeMap),
		ring:        HashRing{},
		nodes:       nodes,
		activeNodes: make(map[string]struct{}),
	}

	for _, node := range nodes {
		h.activeNodes[node] = struct{}{}
	}

	h.generateHashRing()

	return h
}

func (h *HashRingManager) generateHashRing() {
	h.ring = make(HashRing, 0)

	for node := range h.activeNodes {
		for vn := 0; vn < VirtualNodesFactor; vn++ {
			virtualNodeKey := fmt.Sprintf("%s#%d", node, vn)
			hash := h.HashStr(virtualNodeKey)
			h.ring = append(h.ring, hash)
			h.hashMap[hash] = NodeMap{
				Node:          node,
				VirtualNodeID: vn,
			}
		}
	}

	sort.Sort(h.ring)
}

func (h *HashRingManager) HashStr(key string) uint32 {
	hsh := fnv.New32a()
	hsh.Write([]byte(key))
	return hsh.Sum32()
}

/*
Finds a matching node for the given hash.
It returns the first node that can accommodate the hash.
*/
func (h *HashRingManager) GetRingIndex(hash uint32) (int, error) {
	if len(h.ring) == 0 {
		return 0, fmt.Errorf("ring is empty")
	}

	i := sort.Search(len(h.ring), func(i int) bool {
		return h.ring[i] >= hash
	})

	if i < len(h.ring) {
		if i == len(h.ring)-1 && h.ring[i] < hash {
			return 0, nil
		}
		return i, nil
	} else {
		return 0, fmt.Errorf("hash is out of range")
	}
}

/*
Gracefully ejects a node as well as their associated virtual nodes.
*/
func (h *HashRingManager) RemoveNode(node string) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	delete(h.activeNodes, node)

	hashesToRemove := make(map[uint32]struct{})
	for vn := 0; vn < VirtualNodesFactor; vn++ {
		virtualNodeKey := fmt.Sprintf("%s#%d", node, vn)
		hash := h.HashStr(virtualNodeKey)
		hashesToRemove[hash] = struct{}{}
	}

	newRing := make(HashRing, 0, len(h.ring)-VirtualNodesFactor)
	for _, hash := range h.ring {
		if _, toRemove := hashesToRemove[hash]; !toRemove {
			newRing = append(newRing, hash)
		} else {
			delete(h.hashMap, hash)
		}
	}

	h.ring = newRing
}

/*
Adds a new node and its associated virtual nodes to the hash ring.
*/
func (h *HashRingManager) AddNode(node string) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	h.activeNodes[node] = struct{}{}

	for vn := 0; vn < VirtualNodesFactor; vn++ {
		virtualNodeKey := fmt.Sprintf("%s#%d", node, vn)
		hash := h.HashStr(virtualNodeKey)
		position := sort.Search(len(h.ring), func(i int) bool {
			return h.ring[i] >= hash
		})

		h.ring = append(h.ring, 0)
		copy(h.ring[position+1:], h.ring[position:])
		h.ring[position] = hash

		h.hashMap[hash] = NodeMap{
			Node:          node,
			VirtualNodeID: vn,
		}
	}
}

/*
Retrieves the node and virtual node ID for the given index in the hash ring.
*/
func (h *HashRingManager) GetNodeMapForRingIndex(index int) (NodeMap, error) {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	if index < 0 || index >= len(h.ring) {
		return NodeMap{}, fmt.Errorf("index out of range")
	}
	hash := h.ring[index]
	return h.hashMap[hash], nil
}

func (h *HashRingManager) Len() int {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	return len(h.ring)
}

/*
Checks if the specified node is present in the active nodes list.
*/
func (h *HashRingManager) HasNode(node string) bool {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	_, exists := h.activeNodes[node]
	return exists
}
