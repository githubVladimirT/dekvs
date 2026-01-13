package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/githubVladimirT/dekvs/internal/store"
	"github.com/githubVladimirT/dekvs/pkg/types"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

type Node struct {
	raft    *raft.Raft
	store   store.Store
	fsm     *FSM
	config  *Config
	isReady bool
}

type Config struct {
	NodeID   string
	RaftAddr string
	DataDir  string
	Peers    []string
}

func DefaultConfig() *Config {
	return &Config{
		DataDir: "./raft-data",
	}
}

func NewNode(config *Config, store store.Store) (*Node, error) {
	if config == nil {
		config = DefaultConfig()
	}

	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %v", err)
	}

	fsm := NewFSM(store)

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(config.NodeID)

	raftConfig.SnapshotInterval = 30 * time.Second
	raftConfig.SnapshotThreshold = 2
	raftConfig.TrailingLogs = 10

	raftConfig.ElectionTimeout = 1000 * time.Millisecond
	raftConfig.LeaderLeaseTimeout = 500 * time.Millisecond
	raftConfig.CommitTimeout = 50 * time.Millisecond

	addr, err := net.ResolveTCPAddr("tcp", config.RaftAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve address: %v", err)
	}

	transport, err := raft.NewTCPTransport(config.RaftAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create transport: %v", err)
	}

	logStorePath := filepath.Join(config.DataDir, "raft-log.bolt")
	boltDB, err := raftboltdb.NewBoltStore(logStorePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create bolt store: %v", err)
	}

	snapshotsPath := filepath.Join(config.DataDir, "snapshots")
	if err := os.MkdirAll(snapshotsPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create snapshots directory: %v", err)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(snapshotsPath, 1, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot store: %v", err)
	}

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
		fmt.Printf("Node %s: Bootstrapped as single-node cluster\n", config.NodeID)
	} else {
		servers := []raft.Server{
			{
				ID:      raft.ServerID(config.NodeID),
				Address: raft.ServerAddress(config.RaftAddr),
			},
		}

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

	go node.monitorState()

	return node, nil
}

func (n *Node) monitorState() {
	lastState := raft.Follower

	for {
		time.Sleep(1 * time.Second)
		currentState := n.raft.State()

		if currentState != lastState {
			fmt.Printf("Node %s state changed: %s -> %s\n",
				n.config.NodeID, lastState, currentState)
			lastState = currentState

			if currentState == raft.Leader {
				n.addPeersToCluster()
			}
		}
	}
}

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

		if serverID == raft.ServerID(n.config.NodeID) {
			continue
		}

		configFuture := n.raft.GetConfiguration()
		if err := configFuture.Error(); err != nil {
			fmt.Printf("Failed to get raft configuration: %v\n", err)
			continue
		}

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

	if err, ok := future.Response().(error); ok && err != nil {
		return err
	}

	return nil
}

func (n *Node) Get(key string) (*types.Response, error) {
	ctx := context.Background()
	return n.store.Get(ctx, key)
}

func (n *Node) IsLeader() bool {
	return n.raft.State() == raft.Leader
}

func (n *Node) Leader() string {
	return string(n.raft.Leader())
}

func (n *Node) State() string {
	return n.raft.State().String()
}

func (n *Node) Stats() map[string]string {
	return n.raft.Stats()
}

func (n *Node) Shutdown() error {
	future := n.raft.Shutdown()
	return future.Error()
}

func (n *Node) Config() *Config {
	return n.config
}

func (n *Node) IsReady() bool {
	return n.isReady
}
