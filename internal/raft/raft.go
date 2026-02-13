package dekvsraft

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

func NewRaft(nodeID string, addr string, fsm *FSM, join bool) (*raft.Raft, error) {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)

	// Настройка адаптеров хранения
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

	r, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshots, transport)
	if err != nil {
		return nil, err
	}

	if !join {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		r.BootstrapCluster(configuration)
	} else {
		log.Printf("Node %s joining existing cluster", nodeID)
	}

	return r, nil
}
