package raft

import (
    "context"
    "dekvs/internal/store"
    "dekvs/pkg/types"
    "encoding/json"
    "fmt"
    // "io"
    "net"
    "os"
    "path/filepath"
    "time"

    "github.com/hashicorp/raft"
    "github.com/hashicorp/raft-boltdb"
)

// Node represents a Raft node
type Node struct {
    raft     *raft.Raft
    store    store.Store
    fsm      *FSM
    config   *Config
    isReady  bool
}

// Config holds Raft configuration
type Config struct {
    NodeID      string
    RaftAddr    string
    DataDir     string
    Peers       []string
    SnapshotInterval time.Duration
    SnapshotThreshold uint64
}

// DefaultConfig returns a default configuration
func DefaultConfig() *Config {
    return &Config{
        DataDir:          "./raft-data",
        SnapshotInterval: 30 * time.Second,
        SnapshotThreshold: 1000,
    }
}

// NewNode creates a new Raft node
func NewNode(config *Config, store store.Store) (*Node, error) {
    if config == nil {
        config = DefaultConfig()
    }

    // Create data directory if it doesn't exist
    if err := os.MkdirAll(config.DataDir, 0755); err != nil {
        return nil, fmt.Errorf("failed to create data directory: %v", err)
    }

    // Create FSM
    fsm := NewFSM(store)

    // Create Raft configuration
    raftConfig := raft.DefaultConfig()
    raftConfig.LocalID = raft.ServerID(config.NodeID)
    raftConfig.SnapshotInterval = config.SnapshotInterval
    raftConfig.SnapshotThreshold = config.SnapshotThreshold

    // Create transport
    addr, err := net.ResolveTCPAddr("tcp", config.RaftAddr)
    if err != nil {
        return nil, fmt.Errorf("failed to resolve address: %v", err)
    }

    transport, err := raft.NewTCPTransport(config.RaftAddr, addr, 3, 10*time.Second, os.Stderr)
    if err != nil {
        return nil, fmt.Errorf("failed to create transport: %v", err)
    }

    // Create log store and stable store
    logStorePath := filepath.Join(config.DataDir, "raft-log.bolt")
    boltDB, err := raftboltdb.NewBoltStore(logStorePath)
    if err != nil {
        return nil, fmt.Errorf("failed to create bolt store: %v", err)
    }

    // Create snapshot store
    snapshotsPath := filepath.Join(config.DataDir, "snapshots")
    if err := os.MkdirAll(snapshotsPath, 0755); err != nil {
        return nil, fmt.Errorf("failed to create snapshots directory: %v", err)
    }

    snapshotStore, err := raft.NewFileSnapshotStore(snapshotsPath, 1, os.Stderr)
    if err != nil {
        return nil, fmt.Errorf("failed to create snapshot store: %v", err)
    }

    // Create Raft instance
    r, err := raft.NewRaft(raftConfig, fsm, boltDB, boltDB, snapshotStore, transport)
    if err != nil {
        return nil, fmt.Errorf("failed to create raft: %v", err)
    }

    node := &Node{
        raft:   r,
        store:  store,
        fsm:    fsm,
        config: config,
    }

    // If this is the first node, bootstrap the cluster
    if len(config.Peers) == 0 {
        configuration := raft.Configuration{
            Servers: []raft.Server{
                {
                    ID:      raft.ServerID(config.NodeID),
                    Address: raft.ServerAddress(config.RaftAddr),
                },
            },
        }
        r.BootstrapCluster(configuration)
    } else {
        // Otherwise, wait to be added to existing cluster
        // In production, you'd use a proper discovery mechanism
        go node.waitForClusterJoin()
    }

    node.isReady = true
    return node, nil
}

// waitForClusterJoin waits to be added to an existing cluster
func (n *Node) waitForClusterJoin() {
    timeout := time.After(30 * time.Second)
    ticker := time.NewTicker(1 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-timeout:
            return
        case <-ticker.C:
            if n.IsLeader() {
                // If we became leader, try to add other peers
                n.addPeersToCluster()
                return
            }
        }
    }
}

// addPeersToCluster adds configured peers to the Raft cluster
func (n *Node) addPeersToCluster() {
    for _, peer := range n.config.Peers {
        // Parse peer address (format: "node-id@address:port")
        // For simplicity, we assume format is already correct
        serverID := raft.ServerID(peer)
        serverAddr := raft.ServerAddress(peer)
        
        future := n.raft.AddVoter(serverID, serverAddr, 0, 0)
        if err := future.Error(); err != nil {
            fmt.Printf("Failed to add peer %s: %v\n", peer, err)
        }
    }
}

// ApplyCommand applies a command through Raft consensus
func (n *Node) ApplyCommand(cmd types.Command) error {
    if n.raft.State() != raft.Leader {
        return types.ErrNotLeader
    }

    data, err := json.Marshal(cmd)
    if err != nil {
        return fmt.Errorf("failed to marshal command: %v", err)
    }

    future := n.raft.Apply(data, 10*time.Second)
    if err := future.Error(); err != nil {
        return fmt.Errorf("failed to apply command: %v", err)
    }

    // Check if there was an error applying the command
    if err, ok := future.Response().(error); ok && err != nil {
        return err
    }

    return nil
}

// Get retrieves a value directly from the store (linearizable read)
func (n *Node) Get(key string) (*types.Response, error) {
    ctx := context.Background()
    return n.store.Get(ctx, key)
}

// IsLeader returns true if this node is the Raft leader
func (n *Node) IsLeader() bool {
    return n.raft.State() == raft.Leader
}

// Leader returns the current leader address
func (n *Node) Leader() string {
    return string(n.raft.Leader())
}

// State returns the current Raft state
func (n *Node) State() string {
    return n.raft.State().String()
}

// Stats returns Raft statistics
func (n *Node) Stats() map[string]string {
    return n.raft.Stats()
}

// Shutdown gracefully shuts down the Raft node
func (n *Node) Shutdown() error {
    future := n.raft.Shutdown()
    return future.Error()
}

// AddVoter adds a new voter to the Raft cluster
func (n *Node) AddVoter(id, addr string) error {
    if !n.IsLeader() {
        return types.ErrNotLeader
    }

    future := n.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 0)
    return future.Error()
}

// RemoveServer removes a server from the Raft cluster
func (n *Node) RemoveServer(id string) error {
    if !n.IsLeader() {
        return types.ErrNotLeader
    }

    future := n.raft.RemoveServer(raft.ServerID(id), 0, 0)
    return future.Error()
}