package raft

import (
	"context"
	"dekvs/internal/store"
	"dekvs/pkg/types"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// Node represents a Raft node
type Node struct {
	raft    *raft.Raft
	store   store.Store
	fsm     *FSM
	config  *Config
	isReady bool
}

// Config holds Raft configuration
type Config struct {
	NodeID   string
	RaftAddr string
	DataDir  string
	Peers    []string
}

// DefaultConfig returns a default configuration
func DefaultConfig() *Config {
	return &Config{
		DataDir: "./raft-data",
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

	// Create Raft configuration with proper settings
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(config.NodeID)

	// Set proper snapshot settings
	raftConfig.SnapshotInterval = 30 * time.Second
	raftConfig.SnapshotThreshold = 2
	raftConfig.TrailingLogs = 10

	// Increase timeouts for stability
	raftConfig.ElectionTimeout = 1000 * time.Millisecond
	raftConfig.LeaderLeaseTimeout = 500 * time.Millisecond
	raftConfig.CommitTimeout = 50 * time.Millisecond

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

	// Bootstrap the cluster - ключевое изменение!
	// Если это первая нода или мы указали пиров, пытаемся bootstrap
	if len(config.Peers) == 0 {
		// Single-node cluster
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(config.NodeID),
					Address: raft.ServerAddress(config.RaftAddr),
				},
			},
		}
		r.BootstrapCluster(configuration)
		fmt.Printf("Node %s: Bootstrapped as single-node cluster\n", config.NodeID)
	} else {
		// Multi-node cluster - bootstrap с известными пирами
		servers := []raft.Server{
			{
				ID:      raft.ServerID(config.NodeID),
				Address: raft.ServerAddress(config.RaftAddr),
			},
		}

		// Добавляем все известные пиры в конфигурацию bootstrap
		for _, peer := range config.Peers {
			parts := strings.Split(peer, "@")
			if len(parts) == 2 {
				servers = append(servers, raft.Server{
					ID:      raft.ServerID(parts[0]),
					Address: raft.ServerAddress(parts[1]),
				})
			}
		}

		configuration := raft.Configuration{Servers: servers}
		r.BootstrapCluster(configuration)
		fmt.Printf("Node %s: Bootstrapped with peers: %v\n", config.NodeID, config.Peers)
	}

	node.isReady = true

	// Запускаем мониторинг состояния
	go node.monitorState()

	return node, nil
}

// monitorState отслеживает изменения состояния ноды
func (n *Node) monitorState() {
	lastState := raft.Follower

	for {
		time.Sleep(1 * time.Second)
		currentState := n.raft.State()

		if currentState != lastState {
			fmt.Printf("Node %s state changed: %s -> %s\n",
				n.config.NodeID, lastState, currentState)
			lastState = currentState

			// Если стали лидером, добавляем пиров в кластер
			if currentState == raft.Leader {
				n.addPeersToCluster()
			}
		}
	}
}

// addPeersToCluster добавляет пиров в Raft кластер
func (n *Node) addPeersToCluster() {
	fmt.Printf("Node %s (leader) adding peers to cluster: %v\n", n.config.NodeID, n.config.Peers)

	for _, peer := range n.config.Peers {
		parts := strings.Split(peer, "@")
		if len(parts) != 2 {
			fmt.Printf("Invalid peer format: %s\n", peer)
			continue
		}

		serverID := raft.ServerID(parts[0])
		serverAddr := raft.ServerAddress(parts[1])

		// Пропускаем себя
		if serverID == raft.ServerID(n.config.NodeID) {
			continue
		}

		// Проверяем текущую конфигурацию
		configFuture := n.raft.GetConfiguration()
		if err := configFuture.Error(); err != nil {
			fmt.Printf("Failed to get raft configuration: %v\n", err)
			continue
		}

		// Проверяем, существует ли уже сервер
		exists := false
		for _, server := range configFuture.Configuration().Servers {
			if server.ID == serverID {
				exists = true
				break
			}
		}

		if !exists {
			fmt.Printf("Adding voter %s at %s\n", serverID, serverAddr)
			future := n.raft.AddVoter(serverID, serverAddr, 0, 10*time.Second)
			if err := future.Error(); err != nil {
				fmt.Printf("Failed to add voter %s: %v\n", serverID, err)
			} else {
				fmt.Printf("Successfully added voter %s\n", serverID)
			}
		} else {
			fmt.Printf("Voter %s already exists in cluster\n", serverID)
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

// Config returns the node configuration
func (n *Node) Config() *Config {
	return n.config
}

// IsReady returns the readiness status
func (n *Node) IsReady() bool {
	return n.isReady
}
