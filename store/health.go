package store

import (
	"fmt"
	"log"
	"sync"
	"time"
)

func (s *Store) HealthCheck() {
	ticker := time.NewTicker(300 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		s.performHealthCheck()
	}
}

func (s *Store) performHealthCheck() {
	var wg sync.WaitGroup
	wg.Add(len(s.nodes))
	for _, node := range s.nodes {
		go s.checkNode(node, &wg)
	}
	wg.Wait()
}

func (s *Store) checkNode(node string, wg *sync.WaitGroup) {
	defer wg.Done()

	resp, err := s.client.Get(fmt.Sprintf("http://%s/health", node))
	if err != nil || resp.StatusCode != 200 {
		s.ringManager.RemoveNode(node)
		log.Printf("Node %s is down: %v", node, err)
	} else {
		log.Printf("Node %s is up", node)
	}

	// TODO: re-add node if it's back up
}
