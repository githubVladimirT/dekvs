package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

	dekvsraft "github.com/githubVladimirT/dekvs/internal/raft"
	dekvstransport "github.com/githubVladimirT/dekvs/internal/transport"
	pb "github.com/githubVladimirT/dekvs/proto"
)

func main() {
	nodeID := flag.String("id", "node1", "Node ID")
	raftAddr := flag.String("raft", ":12000", "Raft bind address")
	grpcAddr := flag.String("grpc", ":13000", "gRPC bind address")
	joinAddr := flag.String("join", "", "Join address")
	dataDir := flag.String("data", "./data", "Data directory")
	cacheSize := flag.Int("cache", 1000, "Cache size")

	flag.Parse()

	snapshotsDir := filepath.Join(*dataDir, "snapshots")
	raftDir := filepath.Join(*dataDir, "raft")

	for _, dir := range []string{snapshotsDir, raftDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Fatal(err)
		}
	}

	fsm := dekvsraft.NewFSM(*cacheSize)

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(*nodeID)
	config.SnapshotInterval = 30 * time.Second
	config.SnapshotThreshold = 1024

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft.db"))
	if err != nil {
		log.Fatal(err)
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "stable.db"))
	if err != nil {
		log.Fatal(err)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(snapshotsDir, 2, os.Stderr)
	if err != nil {
		log.Fatal(err)
	}

	addr, err := net.ResolveTCPAddr("tcp", *raftAddr)
	if err != nil {
		log.Fatal(err)
	}

	transport, err := raft.NewTCPTransport(*raftAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		log.Fatal(err)
	}

	raftNode, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		log.Fatal(err)
	}

	if *joinAddr != "" {
		joinCluster(*nodeID, *raftAddr, *joinAddr)
	} else {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		raftNode.BootstrapCluster(configuration)
	}

	go cleanupExpiredKeys(fsm)

	grpcServer := dekvstransport.NewServer(raftNode, fsm)
	s := grpc.NewServer()
	pb.RegisterDeKVSServer(s, grpcServer)

	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatal(err)
	}

	http.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(":9090", nil)

	log.Printf("Server starting: node=%s raft=%s grpc=%s", *nodeID, *raftAddr, *grpcAddr)
	if err := s.Serve(lis); err != nil {
		log.Fatal(err)
	}
}

func joinCluster(nodeID, raftAddr, joinAddr string) {
	conn, err := grpc.Dial(joinAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := pb.NewDeKVSClient(conn)
	_, err = client.Join(context.Background(), &pb.JoinRequest{
		NodeId:  nodeID,
		Address: raftAddr,
	})

	if err != nil {
		log.Fatal("Failed to join cluster:", err)
	}
	log.Println("Successfully joined cluster")
}

func cleanupExpiredKeys(fsm *dekvsraft.FSM) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		count := fsm.CleanupExpired()
		if count > 0 {
			log.Printf("Cleaned up %d expired keys", count)
		}
	}
}
