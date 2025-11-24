package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/githubVladimirT/dekvs/internal/api"
	"github.com/githubVladimirT/dekvs/internal/raft"
	"github.com/githubVladimirT/dekvs/internal/store"
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
		raftAddr = "127.0.0.1:9090"
	}

	httpAddr := os.Getenv("HTTP_ADDR")
	if httpAddr == "" {
		httpAddr = ":8080"
	}

	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = "./raft-data-" + nodeID
	}

	peers := []string{}
	if peersEnv := os.Getenv("RAFT_PEERS"); peersEnv != "" {
		peers = splitPeers(peersEnv)
	}

	fmt.Printf("Starting node: %s\n", nodeID)
	fmt.Printf("  Raft address: %s\n", raftAddr)
	fmt.Printf("  HTTP address: %s\n", httpAddr)
	fmt.Printf("  Data directory: %s\n", dataDir)
	fmt.Printf("  Peers: %v\n", peers)

	store := store.NewMemoryStore()

	raftConfig := &raft.Config{
		NodeID:   nodeID,
		RaftAddr: raftAddr,
		DataDir:  dataDir,
		Peers:    peers,
	}

	node, err := raft.NewNode(raftConfig, store)
	if err != nil {
		log.Fatal("Failed to create Raft node:", err)
	}
	defer node.Shutdown()

	fmt.Printf("Raft node %s started successfully!\n", nodeID)

	httpServer := api.NewHTTPServer(node)

	server := &http.Server{
		Addr:    httpAddr,
		Handler: httpServer,
	}

	go func() {
		fmt.Printf("HTTP server starting on %s\n", httpAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	go printClusterStatus(node)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit
	fmt.Println("Shutting down server...")

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
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		state := node.State()
		leader := node.Leader()
		isLeader := node.IsLeader()

		status := "follower"
		if isLeader {
			status = "leader"
		}

		fmt.Printf("Node %s: %s, leader: %s\n",
			node.Config().NodeID, status, leader)
		fmt.Printf("STATE: %s\n", state)
	}
}

func splitPeers(peers string) []string {
	var result []string
	for _, peer := range strings.Split(peers, ",") {
		if peer != "" {
			result = append(result, peer)
		}
	}
	return result
}
