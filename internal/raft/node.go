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
	raftConfig.SnapshotThreshold = 1 // 1000
	raftConfig.TrailingLogs = 100    // 10000

	raftConfig.ElectionTimeout = 1000 * time.Millisecond
	raftConfig.LeaderLeaseTimeout = 500 * time.Millisecond
	raftConfig.CommitTimeout = 50 * time.Millisecond
	raftConfig.HeartbeatTimeout = 500 * time.Millisecond

	raftConfig.LogOutput = os.Stdout
	raftConfig.LogLevel = "INFO"

	fmt.Printf("Node %s: Snapshot config - Interval: %v, Threshold: %d, TrailingLogs: %d\n",
		config.NodeID, raftConfig.SnapshotInterval, raftConfig.SnapshotThreshold, raftConfig.TrailingLogs)

	addr, err := net.ResolveTCPAddr("tcp", config.RaftAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve address: %v", err)
	}

	transport, err := raft.NewTCPTransport(config.RaftAddr, addr, 5, 10*time.Second, os.Stdout)
	if err != nil {
		return nil, fmt.Errorf("failed to create transport: %v", err)
	}

	logStorePath := filepath.Join(config.DataDir, "raft-log.bolt")
	stableStorePath := filepath.Join(config.DataDir, "raft-stable.db")

	logStore, err := raftboltdb.NewBoltStore(logStorePath)
	// boltDB, err := raftboltdb.NewBoltStore(logStorePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create log store: %v", err)
	}

	stableStore, err := raftboltdb.NewBoltStore(stableStorePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create stable store: %v", err)
	}

	snapshotsPath := filepath.Join(config.DataDir, "snapshots")
	if err := os.MkdirAll(snapshotsPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create snapshots directory: %v", err)
	}

	fmt.Printf("Node %s: Snapshots will be stored in: %s\n", config.NodeID, snapshotsPath)

	snapshotStore, err := raft.NewFileSnapshotStore(snapshotsPath, 5, os.Stdout)
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

	r, err := raft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft: %v", err)
	}

	node := &Node{
		raft:   r,
		store:  store,
		fsm:    fsm,
		config: config,
	}

	if err := node.bootstrapCluster(); err != nil {
		return nil, fmt.Errorf("failed to bootstrap cluster: %v", err)
	}

	node.isReady = true
	fmt.Printf("Node %s: Raft node initialized successfully\n", config.NodeID)

	// if hasSnapshot, err := snapshotStore.List(); err == nil && len(hasSnapshot) > 0 {
	// 	fmt.Printf("Node %s: Found %d existing snapshots, will restore on startup\n",
	// 		config.NodeID, len(hasSnapshot))
	// }

	go node.monitorSnapshots()

	return node, nil
}

func (n *Node) bootstrapCluster() error {
	if n.hasExistingState() {
		fmt.Printf("Node %s: Existing Raft state found, skipping bootstrap\n", n.config.NodeID)
		return nil
	}

	fmt.Printf("Node %s: No existing state, bootstrapping cluster\n", n.config.NodeID)

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

	bootstrapFuture := n.raft.BootstrapCluster(configuration)
	if err := bootstrapFuture.Error(); err != nil {
		if err == raft.ErrCantBootstrap {
			fmt.Printf("Node %s: Cluster already bootstrapped\n", n.config.NodeID)
			return nil
		}
		return fmt.Errorf("bootstrap failed: %v", err)
	}

	fmt.Printf("Node %s: Cluster bootstrapped with %d servers\n", n.config.NodeID, len(servers))
	return nil
}

func (n *Node) hasExistingState() bool {
	snapshotsPath := filepath.Join(n.config.DataDir, "snapshots")
	logStorePath := filepath.Join(n.config.DataDir, "raft-log.db")
	stableStorePath := filepath.Join(n.config.DataDir, "raft-stable.db")

	if entries, err := os.ReadDir(snapshotsPath); err == nil && len(entries) > 0 {
		fmt.Printf("Node %s: Found %d snapshot files\n", n.config.NodeID, len(entries))
		return true
	}

	if _, err := os.Stat(logStorePath); err == nil {
		fmt.Printf("Node %s: Found log store file\n", n.config.NodeID)
		return true
	}

	if _, err := os.Stat(stableStorePath); err == nil {
		fmt.Printf("Node %s: Found stable store file\n", n.config.NodeID)
		return true
	}

	fmt.Printf("Node %s: No existing Raft state found\n", n.config.NodeID)
	return false

}

func (n *Node) monitorSnapshots() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		stats := n.raft.Stats()

		lastSnapshotIndex := stats["last_snapshot_index"]
		lastLogIndex := stats["last_log_index"]

		if lastSnapshotIndex != "" && lastLogIndex != "" {
			snapIdx, _ := strconv.ParseUint(lastSnapshotIndex, 10, 64)
			logIdx, _ := strconv.ParseUint(lastLogIndex, 10, 64)

			if logIdx > snapIdx && (logIdx-snapIdx) > 500 {
				fmt.Printf("Node %s: Log is %d entries ahead of snapshot, suggesting manual snapshot\n",
					n.config.NodeID, logIdx-snapIdx)
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

func (n *Node) CreateSnapshot() error {
	if n.raft == nil {
		return fmt.Errorf("raft is not initialized")
	}

	fmt.Printf("Node %s: Manually creating Raft snapshot...\n", n.config.NodeID)

	future := n.raft.Snapshot()
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to create snapshot: %v", err)
	}

	if snapshots, err := n.raft.GetSnapshotStore().List(); err == nil {
		fmt.Printf("Node %s: Now have %d Raft snapshots\n", n.config.NodeID, len(snapshots))
		if len(snapshots) > 0 {
			latest := snapshots[len(snapshots)-1]
			fmt.Printf("Node %s: Latest snapshot - Index: %d, Term: %d\n",
				n.config.NodeID, latest.Index, latest.Term)
		}
	}

	fmt.Printf("Node %s: Snapshot created successfully\n", n.config.NodeID)
	return nil
}

func (n *Node) GetSnapshotInfo() (map[string]interface{}, error) {
	result := map[string]interface{}{
		"node_id": n.config.NodeID,
	}

	if snapshotStore := n.raft.GetSnapshotStore(); snapshotStore != nil {
		if snapshots, err := snapshotStore.List(); err == nil {
			result["snapshot_count"] = len(snapshots)

			var snapshotsInfo []map[string]interface{}
			for _, snap := range snapshots {
				snapInfo := map[string]interface{}{
					"index": snap.Index,
					"term":  snap.Term,
					"id":    snap.ID,
				}
				snapshotsInfo = append(snapshotsInfo, snapInfo)
			}
			result["snapshots"] = snapshotsInfo
		}
	}

	// Статистика
	stats := n.raft.Stats()
	result["stats"] = stats

	return result, nil
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
	if n.raft == nil {
		return nil
	}

	fmt.Printf("Node %s: Shutting down Raft node\n", n.config.NodeID)
	future := n.raft.Shutdown()
	return future.Error()
}

func (n *Node) Config() *Config {
	return n.config
}

func (n *Node) IsReady() bool {
	return n.isReady
}
