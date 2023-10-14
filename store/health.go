package store

import (
	"fmt"
	"log"
	"sync"
	"time"
)

/*
Periodically checks the health of nodes.
*/
func (s *Store) HealthCheck() {
	ticker := time.NewTicker(300 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		s.performHealthCheck()
	}
}

/*
Initiates health checks for each node in the store.
It creates separate goroutines for each node to check its health in parallel.
*/
func (s *Store) performHealthCheck() {
	var wg sync.WaitGroup
	wg.Add(len(s.nodes))
	for _, node := range s.nodes {
		go s.checkNode(node, &wg)
	}
	wg.Wait()
}

/*
Sends a health check request to a specified node.
*/
func (s *Store) checkNode(node string, wg *sync.WaitGroup) {
	defer wg.Done()

	if node == "" {
		log.Printf("no node specified, skipping health check")
		return
	}

	resp, err := s.client.Get(fmt.Sprintf("http://%s/health", node))
	if err != nil || resp.StatusCode != 200 {
		s.ringManager.RemoveNode(node)
		log.Printf("Node %s is down: %v", node, err)
	} else {
		if !s.ringManager.HasNode(node) {
			s.ringManager.AddNode(node)
			log.Printf("Node %s has recovered and is added again", node)
		} else {
			log.Printf("Node %s is up", node)
		}
	}
}
