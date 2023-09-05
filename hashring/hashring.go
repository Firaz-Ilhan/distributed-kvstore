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
	hashMap map[uint32]NodeMap
	ring    HashRing
	nodes   []string
	mutex   sync.Mutex
}

func NewHashRingManager(nodes []string) *HashRingManager {
	h := &HashRingManager{
		hashMap: make(map[uint32]NodeMap),
		ring:    HashRing{},
		nodes:   nodes,
	}

	h.generateHashRing()

	return h
}

func (h *HashRingManager) generateHashRing() {
	ringSize := len(h.nodes) * VirtualNodesFactor
	h.ring = make(HashRing, 0, ringSize)

	for _, node := range h.nodes {
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

	removeHashes := make(map[uint32]struct{})

	for vn := 0; vn < VirtualNodesFactor; vn++ {
		virtualNodeKey := fmt.Sprintf("%s#%d", node, vn)
		hash := h.HashStr(virtualNodeKey)
		delete(h.hashMap, hash)
		removeHashes[hash] = struct{}{}
	}

	newRing := make(HashRing, 0, len(h.ring))

	for _, hash := range h.ring {
		if _, ok := removeHashes[hash]; !ok {
			newRing = append(newRing, hash)
		}
	}

	h.ring = newRing
}

func (h *HashRingManager) AddNode(node string) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	for vn := 0; vn < VirtualNodesFactor; vn++ {
		virtualNodeKey := fmt.Sprintf("%s#%d", node, vn)
		hash := h.HashStr(virtualNodeKey)
		h.ring = append(h.ring, hash)
		h.hashMap[hash] = NodeMap{
			Node:          node,
			VirtualNodeID: vn,
		}
	}

	sort.Sort(h.ring)
}

func (h *HashRingManager) GetNodeMapForRingIndex(index int) (NodeMap, error) {
	if index < 0 || index >= len(h.ring) {
		return NodeMap{}, fmt.Errorf("index out of range")
	}
	hash := h.ring[index]
	return h.hashMap[hash], nil
}

func (hr *HashRingManager) Len() int {
	return len(hr.ring)
}

func (h *HashRingManager) HasNode(node string) bool {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	for _, hash := range h.ring {
		if h.hashMap[hash].Node == node {
			return true
		}
	}
	return false
}
