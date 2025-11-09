package main

import (
	"context"
	"dekvs/internal/api"
	"dekvs/internal/raft"
	"dekvs/internal/store"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	fmt.Println("Starting DeKVS server with Hashicorp Raft...")

	// Configuration
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		nodeID = "node-1"
	}

	raftAddr := os.Getenv("RAFT_ADDR")
	if raftAddr == "" {
		raftAddr = ":9090"
	}

	httpAddr := os.Getenv("HTTP_ADDR")
	if httpAddr == "" {
		httpAddr = ":8080"
	}

	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = "./raft-data-" + nodeID
	}

	// Create store
	store := store.NewMemoryStore()

	// Create Raft configuration
	raftConfig := &raft.Config{
		NodeID:   nodeID,
		RaftAddr: raftAddr,
		DataDir:  dataDir,
		Peers:    getPeersFromEnv(), // You can implement this function
	}

	// Create Raft node
	node, err := raft.NewNode(raftConfig, store)
	if err != nil {
		log.Fatal("Failed to create Raft node:", err)
	}
	defer node.Shutdown()

	// Wait for leadership or follower state
	fmt.Printf("Raft node started. ID: %s, Addr: %s\n", nodeID, raftAddr)

	// Start HTTP server
	httpServer := api.NewHTTPServer(node)

	server := &http.Server{
		Addr:    httpAddr,
		Handler: httpServer,
	}

	// Start server in a goroutine
	go func() {
		fmt.Printf("HTTP server starting on %s\n", httpAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Print cluster status periodically
	go printClusterStatus(node)

	// Wait for interrupt signal to gracefully shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit
	fmt.Println("Shutting down server...")

	// Create a deadline for graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}

	if err := store.Close(); err != nil {
		log.Fatal("Store close error:", err)
	}

	fmt.Println("Server stopped")
}

func printClusterStatus(node *raft.Node) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		state := node.State()
		leader := node.Leader()
		stats := node.Stats()

		fmt.Printf("Cluster status - State: %s, Leader: %s, Term: %s\n",
			state, leader, stats["term"])
	}
}

func getPeersFromEnv() []string {
	// Implement based on your discovery mechanism
	// For example: os.Getenv("RAFT_PEERS") could be "node1@localhost:9090,node2@localhost:9091"
	return []string{}
}
