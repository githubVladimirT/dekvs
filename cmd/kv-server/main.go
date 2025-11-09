package main

import (
    "context"
    "dekvs/internal/api"
    "dekvs/internal/raft"
    "dekvs/internal/store"
    "dekvs/pkg/types"
    "fmt"
    "log"
    "net/http"
    "os"
    "os/signal"
    "syscall"
    "time"
)

func main() {
    fmt.Println("Starting DeKVS server...")
    
    // Create store
    store := store.NewMemoryStore()
    
    // Create raft node
    config := &raft.Config{
        NodeID:   "node-1", 
        RaftAddr: ":9090",
        DataDir:  "./data",
    }
    
    node, err := raft.NewNode(config, store)
    if err != nil {
        log.Fatal("Failed to create raft node:", err)
    }
    
    // Test basic operations
    cmd := types.Command{
        Op:    types.OpPut,
        Key:   "test-key",
        Value: []byte("test-value"),
    }
    
    if err := node.ApplyCommand(cmd); err != nil {
        log.Fatal("Failed to apply command:", err)
    }
    
    fmt.Println("Successfully stored value via Raft")
    
    // Test get directly from store
    resp, err := store.Get(context.Background(), "test-key")
    if err != nil {
        log.Fatal("Failed to get value:", err)
    }
    
    fmt.Printf("Got value: %s (version: %d)\n", string(resp.Value), resp.Version)
    
    // Start HTTP server
    httpServer := api.NewHTTPServer(node)
    
    // Configure HTTP server
    server := &http.Server{
        Addr:    ":8080",
        Handler: httpServer,
    }
    
    // Start server in a goroutine
    go func() {
        fmt.Println("HTTP server starting on :8080")
        if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
            log.Fatalf("HTTP server error: %v", err)
        }
    }()
    
    // Wait for interrupt signal to gracefully shutdown
    quit := make(chan os.Signal, 1)
    signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
    <-quit
    fmt.Println("Shutting down server...")
    
    // Create a deadline for graceful shutdown
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    
    if err := server.Shutdown(ctx); err != nil {
        log.Fatal("Server forced to shutdown:", err)
    }
    
    if err := store.Close(); err != nil {
        log.Fatal("Store close error:", err)
    }
    
    fmt.Println("Server stopped")
}
