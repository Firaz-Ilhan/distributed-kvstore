package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Firaz-Ilhan/distributed-kvstore/handler"
	"github.com/Firaz-Ilhan/distributed-kvstore/store"
)

func main() {
	var port int
	var nodesStr string
	var replicationFactor int
	flag.IntVar(&port, "port", 8080, "Port to listen on")
	flag.StringVar(&nodesStr, "nodes", "", "Comma-separated list of other nodes")
	flag.IntVar(&replicationFactor, "replicationFactor", 2, "Replication factor")
	flag.Parse()

	store := store.NewStore(strings.Split(nodesStr, ","), replicationFactor)

	go store.HealthCheck()

	h := &handler.Handler{
		Store: store,
	}

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "OK")
	})

	http.Handle("/", handler.LoggingMiddleware(h))

	server := &http.Server{
		Addr: fmt.Sprintf(":%d", port),
	}

	go func() {
		log.Printf("Listening on port %d", port)
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Printf("ListenAndServe(): %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	sig := <-quit
	log.Printf("Server is shutting down (%v)...", sig)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Could not gracefully shutdown the server: %v\n", err)
	}

	log.Printf("Server stopped.")
}
