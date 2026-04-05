package dekvsraft

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	pb "github.com/VT0x00/dekvs/proto"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"google.golang.org/grpc"
)

func NewRaft(nodeID string, bindAddr string, advertiseAddr string, fsm *FSM, join bool, leaderAddrs string) (*raft.Raft, error) {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)
	// Optimized for high throughput
	config.ElectionTimeout = 1 * time.Second
	config.HeartbeatTimeout = 1 * time.Second
	config.LeaderLeaseTimeout = 500 * time.Millisecond
	config.CommitTimeout = 100 * time.Millisecond
	config.BatchApplyCh = true
	config.MaxAppendEntries = 256
	config.TrailingLogs = 10000
	config.SnapshotInterval = 60 * time.Second
	config.SnapshotThreshold = 10000

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

	// Parse advertise address for transport
	var advertiseNetAddr *net.TCPAddr
	if advertiseAddr != "" {
		advertiseNetAddr, err = net.ResolveTCPAddr("tcp", advertiseAddr)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve advertise address: %w", err)
		}
	}

	transport, err := raft.NewTCPTransport(bindAddr, advertiseNetAddr, 3, 0, os.Stdout)
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
		}
	} else {
		log.Printf("Node %s joining existing cluster via leaders: %s", nodeID, leaderAddrs)
		// Use advertiseAddr for joining, or bindAddr if not set
		raftAddr := advertiseAddr
		if raftAddr == "" {
			raftAddr = bindAddr
		}
		if err := joinCluster(nodeID, raftAddr, leaderAddrs); err != nil {
			return nil, fmt.Errorf("failed to join cluster: %w", err)
		}
	}

	return r, nil
}

func joinCluster(nodeID, raftAddr, leaderAddrs string) error {
	if leaderAddrs == "" {
		return fmt.Errorf("no leader addresses provided")
	}

	addrs := strings.Split(leaderAddrs, ",")
	for _, addr := range addrs {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}

		log.Printf("Attempting to join cluster via %s", addr)

		conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithTimeout(5*time.Second))
		if err != nil {
			log.Printf("Failed to connect to %s: %v", addr, err)
			continue
		}
		defer conn.Close()

		client := pb.NewKVServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		resp, err := client.Join(ctx, &pb.JoinRequest{
			NodeId:   nodeID,
			RaftAddr: raftAddr,
		})
		if err != nil {
			log.Printf("Failed to send join request to %s: %v", addr, err)
			continue
		}

		if !resp.Success {
			log.Printf("Join request rejected by %s: %s", addr, resp.Message)
			continue
		}

		log.Printf("Successfully joined cluster via %s", addr)
		return nil
	}

	return fmt.Errorf("failed to join cluster via any leader")
}
