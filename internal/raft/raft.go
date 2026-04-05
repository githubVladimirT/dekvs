package dekvsraft

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	// "github.com/VT0x00/dekvs/internal/store"
)

func NewRaft(nodeID string, addr string, fsm *FSM, join bool) (*raft.Raft, error) {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)
	config.ElectionTimeout = 1 * time.Second
	config.HeartbeatTimeout = 1 * time.Second
	config.CommitTimeout = 1 * time.Second

	logDir := filepath.Join(os.TempDir(), fmt.Sprintf("raft-log-%s", nodeID))
	os.MkdirAll(logDir, 0755)

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(logDir, "log.bolt"))
	if err != nil {
		return nil, err
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(logDir, "stable.bolt"))
	if err != nil {
		return nil, err
	}

	snapshots, err := raft.NewFileSnapshotStore(logDir, 3, os.Stdout)
	if err != nil {
		return nil, err
	}

	transport, err := raft.NewTCPTransport(addr, nil, 3, 0, os.Stdout)
	if err != nil {
		return nil, err
	}

	if join {
		log.Printf("Node %s joining existing cluster", nodeID)
		r, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshots, transport)
		if err != nil {
			return nil, err
		}
		return r, nil
	}

	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      config.LocalID,
				Address: transport.LocalAddr(),
			},
		},
	}

	r, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshots, transport)
	if err != nil {
		return nil, err
	}

	hasState, err := raft.HasExistingState(logStore, stableStore, snapshots)
	if err != nil {
		return nil, err
	}

	if !hasState {
		log.Println("Bootstrapping new cluster with single node...")
		future := r.BootstrapCluster(configuration)
		if err := future.Error(); err != nil {
			return nil, err
		}
	} else {
		log.Println("Found existing state, not bootstrapping.")
	}

	return r, nil
}
