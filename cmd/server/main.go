package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

	dekvsraft "github.com/githubVladimirT/dekvs/internal/raft"
	dekvstransport "github.com/githubVladimirT/dekvs/internal/transport"
	pb "github.com/githubVladimirT/dekvs/proto"
)

func main() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("PANIC recovered: %v", r)
			debug.PrintStack()
		}
	}()

	nodeID := flag.String("id", "node1", "Node ID")
	raftAddr := flag.String("raft", ":12000", "Raft bind address")
	grpcAddr := flag.String("grpc", ":13000", "gRPC bind address")
	metricsAddr := flag.String("metrics", ":9090", "Metrics HTTP bind address")
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

	raftLogger := hclog.New(&hclog.LoggerOptions{
		Name:   "raft",
		Level:  hclog.Info, // или hclog.Debug для отладки
		Output: os.Stderr,
		Color:  hclog.AutoColor,
	})

	fsm := dekvsraft.NewFSM(*cacheSize)

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(*nodeID)
	config.SnapshotInterval = 30 * time.Second
	config.SnapshotThreshold = 1024

	// TODO: increase timings in production
	config.HeartbeatTimeout = 500 * time.Millisecond
	config.ElectionTimeout = 1000 * time.Millisecond
	config.LeaderLeaseTimeout = 250 * time.Millisecond
	config.CommitTimeout = 50 * time.Millisecond

	// config.MinBatchSize = 1
	config.MaxAppendEntries = 64
	config.TrailingLogs = 10240

	config.LogLevel = "DEBUG"
	config.Logger = raftLogger

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

	setupGracefulShutdown(raftNode, *nodeID, *joinAddr)

	grpcServer := dekvstransport.NewServer(raftNode, fsm)
	grpcServer.StartHealthMonitor()
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

	go monitorCluster(raftNode, config.LocalID)

	if *joinAddr != "" {
		log.Printf("Attempting to join cluster via %s", *joinAddr)
		joinCluster(*nodeID, *raftAddr, *joinAddr)
	} else {
		// bootstrap new cluster
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		raftNode.BootstrapCluster(configuration)
		log.Println("New cluster bootstrapped")
	}

	go cleanupExpiredKeys(fsm)

	go func() {
		log.Printf("Starting metrics server on %s", *metricsAddr)
		http.Handle("/metrics", promhttp.Handler())
		if err := http.ListenAndServe(*metricsAddr, nil); err != nil {
			log.Printf("Metrics server error: %v", err)
		}
	}()
}

func joinCluster(nodeID, raftAddr, joinAddr string) {
	conn, err := grpc.Dial(joinAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatal("Failed to connect to leader:", err)
	}
	defer conn.Close()

	client := pb.NewDeKVSClient(conn)

	// Important Raft addr, not gRPC addr!
	// Important: joinAddr - leader gRPC addr,
	// but we send raftAddr for Raft connection
	resp, err := client.Join(context.Background(), &pb.JoinRequest{
		NodeId:  nodeID,
		Address: raftAddr, // Raft addr, for example "127.0.0.1:12002"
	})

	if err != nil {
		log.Fatal("Failed to join cluster:", err)
	}

	if resp.Success {
		log.Println("Successfully joined cluster")
	} else {
		log.Println("Join request failed")
	}

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

func monitorCluster(raftNode *raft.Raft, nodeID raft.ServerID) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if raftNode.State() != raft.Leader {
			continue
		}

		configFuture := raftNode.GetConfiguration()
		if err := configFuture.Error(); err != nil {
			log.Printf("Failed to get config: %v", err)
			continue
		}

		config := configFuture.Configuration()

		// check everyone's accessability
		for _, server := range config.Servers {
			if server.ID == nodeID {
				continue
			}

			// try to ping through transport
			// if node isn't accessible for a long time, remove it
			// needs more complex logic in production
		}
	}
}

func removeFromCluster(nodeID string, raftNode *raft.Raft) {
	if raftNode.State() != raft.Leader {
		return
	}

	log.Printf("Removing node %s from cluster", nodeID)
	removeFuture := raftNode.RemoveServer(raft.ServerID(nodeID), 0, 0)
	if err := removeFuture.Error(); err != nil {
		log.Printf("Failed to remove node: %v", err)
	}
}

func setupGracefulShutdown(raftNode *raft.Raft, nodeID, joinAddr string) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		log.Printf("Received signal %v, shutting down gracefully...", sig)

		if joinAddr != "" && raftNode.State() == raft.Leader {
			log.Println("Transferring leadership before shutdown...")
			transferFuture := raftNode.LeadershipTransfer()
			if err := transferFuture.Error(); err != nil {
				log.Printf("Leadership transfer failed: %v", err)
			}
			time.Sleep(1 * time.Second)
		}

		if joinAddr != "" {
			log.Printf("Removing node %s from cluster...", nodeID)
			removeFuture := raftNode.RemoveServer(raft.ServerID(nodeID), 0, 5*time.Second)
			if err := removeFuture.Error(); err != nil {
				log.Printf("Failed to remove from cluster: %v", err)
			}
		}

		log.Println("Shutting down Raft...")
		shutdownFuture := raftNode.Shutdown()
		if err := shutdownFuture.Error(); err != nil {
			log.Printf("Raft shutdown error: %v", err)
		}

		log.Println("Shutdown complete")
		os.Exit(0)
	}()
}
