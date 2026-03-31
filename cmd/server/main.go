package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"

	dekvsraft "github.com/githubVladimirT/dekvs/internal/raft"
	"github.com/githubVladimirT/dekvs/internal/store"
	pb "github.com/githubVladimirT/dekvs/proto"
	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	nodeID        = flag.String("id", "node1", "Node ID")
	raftBindAddr  = flag.String("raft-bind-addr", "127.0.0.1:9091", "Raft bind address")
	raftAdvAddr   = flag.String("raft-adv-addr", "", "Raft advertise address (optional)")
	grpcPort      = flag.String("grpc-port", "8081", "gRPC port")
	join          = flag.Bool("join", false, "Join existing cluster")
	leaderAddrs   = flag.String("leader-addrs", "", "Comma-separated leader addresses to join")
	metricsPort   = flag.String("metrics-port", "9090", "Prometheus metrics port")
	enableMetrics = flag.Bool("enable-metrics", false, "Enable Prometheus metrics endpoint")
)

// Prometheus metrics
var (
	metricPutRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dekvs_put_requests_total",
			Help: "Total number of Put requests",
		},
		[]string{"status"},
	)
	metricGetRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dekvs_get_requests_total",
			Help: "Total number of Get requests",
		},
		[]string{"status"},
	)
	metricDeleteRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dekvs_delete_requests_total",
			Help: "Total number of Delete requests",
		},
		[]string{"status"},
	)
	metricBatchRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dekvs_batch_requests_total",
			Help: "Total number of Batch requests",
		},
		[]string{"type", "status"},
	)
	metricRaftState = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dekvs_raft_state",
			Help: "Current Raft state (0=Follower, 1=Candidate, 2=Leader, 3=Shutdown)",
		},
		[]string{"node_id"},
	)
	metricKeysCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "dekvs_keys_total",
			Help: "Total number of keys in the store",
		},
	)
	// Additional metrics for enhanced monitoring
	metricRequestLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "dekvs_request_latency_seconds",
			Help:    "Request latency in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"operation"},
	)
	metricLeaderChanges = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "dekvs_leader_changes_total",
			Help: "Total number of leader changes",
		},
	)
	metricRaftLogIndex = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dekvs_raft_log_index",
			Help: "Current Raft log index",
		},
		[]string{"node_id"},
	)
	metricRaftPendingRequests = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dekvs_raft_pending_requests",
			Help: "Number of pending Raft requests",
		},
		[]string{"node_id"},
	)
	metricUptime = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dekvs_uptime_seconds",
			Help: "Node uptime in seconds",
		},
		[]string{"node_id"},
	)
)

func init() {
	prometheus.MustRegister(metricPutRequests)
	prometheus.MustRegister(metricGetRequests)
	prometheus.MustRegister(metricDeleteRequests)
	prometheus.MustRegister(metricBatchRequests)
	prometheus.MustRegister(metricRaftState)
	prometheus.MustRegister(metricKeysCount)
	prometheus.MustRegister(metricRequestLatency)
	prometheus.MustRegister(metricLeaderChanges)
	prometheus.MustRegister(metricRaftLogIndex)
	prometheus.MustRegister(metricRaftPendingRequests)
	prometheus.MustRegister(metricUptime)
}

type server struct {
	pb.UnimplementedKVServiceServer
	store          *store.Store
	raft           *raft.Raft
	nodeID         string
	grpcAddr       string
	startTime      time.Time
	lastLeaderAddr raft.ServerAddress
}

func (s *server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	start := time.Now()
	defer func() {
		metricRequestLatency.WithLabelValues("put").Observe(time.Since(start).Seconds())
	}()

	cmd := &store.Command{
		Op:    "put",
		Key:   req.Key,
		Value: req.Value,
	}

	b, err := json.Marshal(cmd)
	if err != nil {
		metricPutRequests.WithLabelValues("error").Inc()
		return nil, err
	}

	f := s.raft.Apply(b, 3000)
	if e := f.Error(); e != nil {
		metricPutRequests.WithLabelValues("error").Inc()
		return nil, e
	}

	metricPutRequests.WithLabelValues("success").Inc()
	return &pb.PutResponse{Success: true}, nil
}

func (s *server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	start := time.Now()
	defer func() {
		metricRequestLatency.WithLabelValues("get").Observe(time.Since(start).Seconds())
	}()

	value, found := s.store.Get(req.Key)
	if found {
		metricGetRequests.WithLabelValues("found").Inc()
	} else {
		metricGetRequests.WithLabelValues("not_found").Inc()
	}
	return &pb.GetResponse{Value: value, Found: found}, nil
}

func (s *server) Join(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {
	if s.raft.State() != raft.Leader {
		return &pb.JoinResponse{
			Success: false,
			Message: "not leader",
		}, nil
	}

	future := s.raft.AddVoter(raft.ServerID(req.NodeId), raft.ServerAddress(req.RaftAddr), 0, 10*time.Second)
	if err := future.Error(); err != nil {
		return &pb.JoinResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	return &pb.JoinResponse{
		Success: true,
		Message: "joined cluster successfully",
	}, nil
}

func (s *server) Health(ctx context.Context, req *pb.HealthRequest) (*pb.HealthResponse, error) {
	leaderAddr := s.raft.Leader()
	leaderID := string(leaderAddr)

	return &pb.HealthResponse{
		Healthy:  true,
		NodeId:   s.nodeID,
		IsLeader: s.raft.State() == raft.Leader,
		LeaderId: leaderID,
	}, nil
}

func (s *server) Status(ctx context.Context, req *pb.StatusRequest) (*pb.StatusResponse, error) {
	cfg := s.raft.GetConfiguration().Configuration()
	leaderAddr := s.raft.Leader()
	leaderID := string(leaderAddr)

	// Track leader changes
	if leaderAddr != "" && leaderAddr != s.lastLeaderAddr {
		if s.lastLeaderAddr != "" {
			metricLeaderChanges.Inc()
		}
		s.lastLeaderAddr = leaderAddr
	}

	peerCount := len(cfg.Servers) - 1
	if peerCount < 0 {
		peerCount = 0
	}

	state := "unknown"
	switch s.raft.State() {
	case raft.Follower:
		state = "follower"
	case raft.Candidate:
		state = "candidate"
	case raft.Leader:
		state = "leader"
	case raft.Shutdown:
		state = "shutdown"
	}

	metricRaftState.WithLabelValues(s.nodeID).Set(float64(s.raft.State()))
	metricKeysCount.Set(float64(s.store.Count()))
	metricRaftLogIndex.WithLabelValues(s.nodeID).Set(float64(s.raft.LastIndex()))
	metricUptime.WithLabelValues(s.nodeID).Set(time.Since(s.startTime).Seconds())

	return &pb.StatusResponse{
		NodeId:       s.nodeID,
		RaftAddr:     *raftAdvAddr,
		GrpcAddr:     s.grpcAddr,
		IsLeader:     s.raft.State() == raft.Leader,
		LeaderId:     leaderID,
		LastLogIndex: s.raft.LastIndex(),
		LastLogTerm:  0,
		State:        state,
		PeerCount:    int32(peerCount),
	}, nil
}

func (s *server) BatchPut(ctx context.Context, req *pb.BatchPutRequest) (*pb.BatchPutResponse, error) {
	start := time.Now()
	defer func() {
		metricRequestLatency.WithLabelValues("batch_put").Observe(time.Since(start).Seconds())
	}()

	if len(req.Pairs) == 0 {
		return &pb.BatchPutResponse{Success: true}, nil
	}

	pairs := make([]store.KeyValue, len(req.Pairs))
	for i, p := range req.Pairs {
		pairs[i] = store.KeyValue{Key: p.Key, Value: p.Value}
	}

	cmd := &store.Command{
		Op:    "batchPut",
		Pairs: pairs,
	}

	b, err := json.Marshal(cmd)
	if err != nil {
		metricBatchRequests.WithLabelValues("put", "error").Inc()
		return nil, err
	}

	f := s.raft.Apply(b, 5000)
	if e := f.Error(); e != nil {
		metricBatchRequests.WithLabelValues("put", "error").Inc()
		return nil, e
	}

	metricBatchRequests.WithLabelValues("put", "success").Inc()
	return &pb.BatchPutResponse{
		Success:       true,
		InsertedCount: int32(len(req.Pairs)),
	}, nil
}

func (s *server) BatchGet(ctx context.Context, req *pb.BatchGetRequest) (*pb.BatchGetResponse, error) {
	start := time.Now()
	defer func() {
		metricRequestLatency.WithLabelValues("batch_get").Observe(time.Since(start).Seconds())
	}()

	values, notFound := s.store.GetBatch(req.Keys)

	metricBatchRequests.WithLabelValues("get", "success").Inc()

	return &pb.BatchGetResponse{
		Values:       values,
		NotFoundKeys: notFound,
	}, nil
}

func (s *server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	start := time.Now()
	defer func() {
		metricRequestLatency.WithLabelValues("delete").Observe(time.Since(start).Seconds())
	}()

	cmd := &store.Command{
		Op:  "delete",
		Key: req.Key,
	}

	b, err := json.Marshal(cmd)
	if err != nil {
		metricDeleteRequests.WithLabelValues("error").Inc()
		return nil, err
	}

	f := s.raft.Apply(b, 3000)
	if e := f.Error(); e != nil {
		metricDeleteRequests.WithLabelValues("error").Inc()
		return nil, e
	}

	existed := false
	if resp, ok := f.Response().(bool); ok {
		existed = resp
	}

	metricDeleteRequests.WithLabelValues("success").Inc()
	return &pb.DeleteResponse{
		Success: true,
		Existed: existed,
	}, nil
}

func startMetricsServer() {
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	log.Printf("Metrics server starting on :%s", *metricsPort)
	if err := http.ListenAndServe(":"+*metricsPort, nil); err != nil {
		log.Printf("Failed to start metrics server: %v", err)
	}
}

func main() {
	flag.Parse()

	if *enableMetrics {
		go startMetricsServer()
	}

	lis, err := net.Listen("tcp", ":"+*grpcPort)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	storeInstance := store.NewStore()

	// Create FSM with a hook to update metrics after each apply
	fsm := dekvsraft.NewFSM(storeInstance, nil, func() {
		metricKeysCount.Set(float64(storeInstance.Count()))
	})

	raftInstance, err := dekvsraft.NewRaft(*nodeID, *raftBindAddr, *raftAdvAddr, fsm, *join, *leaderAddrs)
	if err != nil {
		log.Fatalf("Failed to start raft: %v", err)
	}

	fsm.SetRaft(raftInstance)

	startTime := time.Now()

	// Start periodic metric collection
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			// Update pending requests metric (approximate from raft stats)
			stats := raftInstance.Stats()
			if pendingStr, ok := stats["pending"]; ok {
				if pending, err := strconv.ParseFloat(pendingStr, 64); err == nil {
					metricRaftPendingRequests.WithLabelValues(*nodeID).Set(pending)
				}
			}
			// Update Raft state, log index, and uptime
			metricRaftState.WithLabelValues(*nodeID).Set(float64(raftInstance.State()))
			metricRaftLogIndex.WithLabelValues(*nodeID).Set(float64(raftInstance.LastIndex()))
			metricUptime.WithLabelValues(*nodeID).Set(time.Since(startTime).Seconds())
		}
	}()

	s := grpc.NewServer()
	grpcServer := &server{
		store:          storeInstance,
		raft:           raftInstance,
		nodeID:         *nodeID,
		grpcAddr:       fmt.Sprintf("0.0.0.0:%s", *grpcPort),
		startTime:      startTime,
		lastLeaderAddr: "",
	}

	pb.RegisterKVServiceServer(s, grpcServer)
	reflection.Register(s)

	log.Printf("Server %s running at %s", *nodeID, *raftBindAddr)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
