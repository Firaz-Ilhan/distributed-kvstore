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

func (h *HashRingManager) RemoveNode(node string) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	delete(h.activeNodes, node)

	for vn := 0; vn < VirtualNodesFactor; vn++ {
		virtualNodeKey := fmt.Sprintf("%s#%d", node, vn)
		hash := h.HashStr(virtualNodeKey)
		position := sort.Search(len(h.ring), func(i int) bool {
			return h.ring[i] >= hash
		})

		if position < len(h.ring) && h.ring[position] == hash {
			h.ring = append(h.ring[:position], h.ring[position+1:]...)
			delete(h.hashMap, hash)
		}
	}
}

func (h *HashRingManager) AddNode(node string) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	h.activeNodes[node] = struct{}{}

	hashesToAdd := make([]uint32, 0, VirtualNodesFactor)
	positionsToAdd := make([]int, 0, VirtualNodesFactor)

	for vn := 0; vn < VirtualNodesFactor; vn++ {
		virtualNodeKey := fmt.Sprintf("%s#%d", node, vn)
		hash := h.HashStr(virtualNodeKey)
		position := sort.Search(len(h.ring), func(i int) bool {
			return h.ring[i] >= hash
		})
		hashesToAdd = append(hashesToAdd, hash)
		positionsToAdd = append(positionsToAdd, position)
	}

	newRingSize := len(h.ring) + VirtualNodesFactor
	newRing := make([]uint32, newRingSize)
	currentIndex := 0

	for i, hash := range h.ring {
		for len(positionsToAdd) > 0 && i == positionsToAdd[0] {
			newRing[currentIndex] = hashesToAdd[0]
			h.hashMap[hashesToAdd[0]] = NodeMap{
				Node:          node,
				VirtualNodeID: currentIndex % VirtualNodesFactor,
			}
			currentIndex++
			positionsToAdd = positionsToAdd[1:]
			hashesToAdd = hashesToAdd[1:]
		}
		newRing[currentIndex] = hash
		currentIndex++
	}

	for _, hash := range hashesToAdd {
		newRing[currentIndex] = hash
		h.hashMap[hash] = NodeMap{
			Node:          node,
			VirtualNodeID: currentIndex % VirtualNodesFactor,
		}
		currentIndex++
	}

	h.ring = newRing
}

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

func (h *HashRingManager) HasNode(node string) bool {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	_, exists := h.activeNodes[node]
	return exists
}
