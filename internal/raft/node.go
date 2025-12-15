package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
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

func (n *Node) GetStore() store.Store {
	return n.store
}

func (n *Node) GetRaft() any { // interface{} --> any
	return n.raft
}

func NewNode(config *Config, store store.Store) (*Node, error) {
	if config == nil {
		config = DefaultConfig()
	}

	fmt.Printf("Node %s: Initializing with peers: %v\n", config.NodeID, config.Peers)

	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %v", err)
	}

	fsm := NewFSM(store)

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(config.NodeID)

	raftConfig.SnapshotInterval = 30 * time.Second
	raftConfig.SnapshotThreshold = 10
	raftConfig.TrailingLogs = 100

	fmt.Printf("Node %s: Snapshot config - Interval: %v, Threshold: %d\n",
		config.NodeID, raftConfig.SnapshotInterval, raftConfig.SnapshotThreshold)

	raftConfig.ElectionTimeout = 2000 * time.Millisecond
	raftConfig.LeaderLeaseTimeout = 1000 * time.Millisecond
	raftConfig.CommitTimeout = 100 * time.Millisecond

	raftConfig.LogOutput = os.Stdout
	raftConfig.LogLevel = "INFO"

	addr, err := net.ResolveTCPAddr("tcp", config.RaftAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve address: %v", err)
	}

	transport, err := raft.NewTCPTransport(config.RaftAddr, addr, 5, 30*time.Second, os.Stdout)
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

	snapshotStore, err := raft.NewFileSnapshotStore(snapshotsPath, 3, os.Stdout)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot store: %v", err)
	}

	if snapshots, err := snapshotStore.List(); err == nil {
		fmt.Printf("Node %s: Found %d snapshots in store:\n", config.NodeID, len(snapshots))
		for i, snap := range snapshots {
			fmt.Printf("  Snapshot %d: ID: %s-%d, Index: %d, Term: %d\n",
				i, snap.ID, snap.Index, snap.Index, snap.Term)
		}
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

	if err := node.initCluster(); err != nil {
		return nil, fmt.Errorf("failed to initialize cluster: %v", err)
	}

	node.isReady = true

	if hasSnapshot, err := snapshotStore.List(); err == nil && len(hasSnapshot) > 0 {
		fmt.Printf("Node %s: Found %d existing snapshots, will restore on startup\n",
			config.NodeID, len(hasSnapshot))
	}

	go node.enhancedClusterMonitor()

	return node, nil
}

func (n *Node) initCluster() error {
	hasExistingState, err := n.checkForExistingStateFiles()
	if err != nil {
		return fmt.Errorf("failed to check existing state: %v", err)
	}

	if hasExistingState {
		fmt.Printf("Node %s: Existing Raft state detected, skipping bootstrap\n", n.config.NodeID)
		return nil
	}

	if len(n.config.Peers) == 0 {
		// Single node cluster
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(n.config.NodeID),
					Address: raft.ServerAddress(n.config.RaftAddr),
				},
			},
		}
		fmt.Printf("Node %s: Bootstrapping as new single-node cluster\n", n.config.NodeID)
		return n.raft.BootstrapCluster(configuration).Error()
	} else {
		servers := []raft.Server{
			{
				ID:      raft.ServerID(n.config.NodeID),
				Address: raft.ServerAddress(n.config.RaftAddr),
			},
		}

		for _, peer := range n.config.Peers {
			parts := strings.Split(peer, "@")
			if len(parts) == 2 {
				servers = append(servers, raft.Server{
					ID:      raft.ServerID(parts[0]),
					Address: raft.ServerAddress(parts[1]),
				})
			}
		}

		configuration := raft.Configuration{Servers: servers}
		fmt.Printf("Node %s: Bootstrapping as new multi-node cluster with %d servers\n",
			n.config.NodeID, len(servers))
		return n.raft.BootstrapCluster(configuration).Error()
	}
}

func (n *Node) checkForExistingStateFiles() (bool, error) {
	snapshotsDir := filepath.Join(n.config.DataDir, "snapshots")

	if entries, err := os.ReadDir(snapshotsDir); err == nil && len(entries) > 0 {
		fmt.Printf("Node %s: Found %d snapshot files\n", n.config.NodeID, len(entries))

		var latestSnapshot string
		var latestIndex uint64 = 0

		for _, entry := range entries {
			if strings.HasSuffix(entry.Name(), ".snap") {
				parts := strings.Split(entry.Name(), "-")
				if len(parts) >= 1 {
					if index, err := strconv.ParseUint(parts[0], 10, 64); err == nil {
						if index > latestIndex {
							latestIndex = index
							latestSnapshot = entry.Name()
						}
					}
				}
			}
		}

		if latestSnapshot != "" {
			fmt.Printf("Node %s: Latest snapshot: %s (index: %d)\n",
				n.config.NodeID, latestSnapshot, latestIndex)
			return true, nil
		}
	}

	logFile := filepath.Join(n.config.DataDir, "raft-log.bolt")
	if _, err := os.Stat(logFile); err == nil {
		fmt.Printf("Node %s: Found existing log file\n", n.config.NodeID)
		return true, nil
	}

	return false, nil
}

func (n *Node) enhancedClusterMonitor() {
	fmt.Printf("Node %s: Starting enhanced cluster monitor\n", n.config.NodeID)

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	var lastLeader string
	var lastState raft.RaftState

	for range ticker.C {
		currentState := n.raft.State()
		currentLeader := n.raft.Leader()

		if currentState != lastState || currentLeader != raft.ServerAddress(lastLeader) {
			lastState = currentState
			lastLeader = string(currentLeader)
		}

		if currentState == raft.Leader {
			n.ensureAllPeersInCluster()
		}

		if currentState == raft.Follower && currentLeader == "" {
			fmt.Printf("Node %s: No leader detected, attempting to re-bootstrap\n", n.config.NodeID)

			time.AfterFunc(5*time.Second, func() {
				n.initCluster()
			})
		}

		configFuture := n.raft.GetConfiguration()
		if err := configFuture.Error(); err != nil {
			servers := configFuture.Configuration().Servers

			if len(servers) > 0 {
				fmt.Printf("Node %s: Current cluster configuration: %v\n", n.config.NodeID, servers)
			}
		}
	}
}

func (n *Node) ensureAllPeersInCluster() {
	fmt.Printf("Node %s (leader): Ensuring all peers are in cluster: %v\n", n.config.NodeID, n.config.Peers)

	configFuture := n.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		fmt.Printf("Node %s (leader): Ensuring all peers are in cluster: %v\n", n.config.NodeID, n.config.Peers)
		return
	}

	currentServers := configFuture.Configuration().Servers
	fmt.Printf("Node %s: Current servers in cluster: %v\n", n.config.NodeID, currentServers)

	serversToAdd := make(map[string]string)

	currentServerMap := make(map[raft.ServerID]bool)
	for _, server := range currentServers {
		currentServerMap[server.ID] = true
	}

	for _, peer := range n.config.Peers {
		parts := strings.Split(peer, "@")
		if len(parts) != 2 {
			fmt.Printf("Node %s: Invalid peer format: %s\n", n.config.NodeID, peer)
			continue
		}

		peerID := raft.ServerID(parts[0])
		peerAddr := raft.ServerAddress(parts[1])

		if peerID == raft.ServerID(n.config.NodeID) {
			continue
		}

		if !currentServerMap[peerID] {
			fmt.Printf("Node %s: Peer %s not in cluster, will add\n", n.config.NodeID, peerID)
			serversToAdd[string(peerID)] = string(peerAddr)
		} else {
			fmt.Printf("Node %s: Peer %s already in cluster\n", n.config.NodeID, peerID)
		}
	}

	for serverID, serverAddr := range serversToAdd {
		fmt.Printf("Node %s: Adding server %s at %s to cluster\n", n.config.NodeID, serverID, serverAddr)

		future := n.raft.AddVoter(
			raft.ServerID(serverID),
			raft.ServerAddress(serverAddr),
			0,
			30*time.Second,
		)

		if err := future.Error(); err != nil {
			fmt.Printf("Node %s: Failed to add server %s: %v\n", n.config.NodeID, serverID, err)

		} else {
			fmt.Printf("Node %s: Successfully added server %s\n", n.config.NodeID, serverID)

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

	if err, ok := future.Response().(error); ok && err != nil {
		return err
	}

	return nil
}

func (n *Node) autoSnapshot() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		if n.IsLeader() {
			stats := n.Stats()
			lastSnapshotIndex := stats["last_snapshot_index"]
			lastLogIndex := stats["last_log_index"]

			if lastSnapshotIndex != lastLogIndex {
				fmt.Printf("Node %s: log growth detected (snapshot: %s, log: %s)\n",
					n.config.NodeID, lastSnapshotIndex, lastLogIndex)
			}
		}
	}
}

func (n *Node) CreateSnapshot() error {
	if n.raft == nil {
		return fmt.Errorf("raft is not initialized")
	}

	future := n.raft.Snapshot()
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to create snapshot: %v", err)
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
