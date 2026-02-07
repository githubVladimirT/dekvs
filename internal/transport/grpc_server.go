package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"

	"time"

	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	dekvsraft "github.com/githubVladimirT/dekvs/internal/raft"
	pb "github.com/githubVladimirT/dekvs/proto"
)

type Server struct {
	pb.UnimplementedDeKVSServer
	raftNode *raft.Raft
	fsm      dekvsraft.FSMManager
	health   *nodeHealth
}

type nodeHealth struct {
	mu            sync.RWMutex
	failures      map[string]int
	lastSeen      map[string]time.Time
	healthStatus  map[string]bool
	maxFailures   int
	checkInterval time.Duration
}

func NewServer(raftNode *raft.Raft, fsm dekvsraft.FSMManager) *Server {
	return &Server{
		raftNode: raftNode,
		fsm:      fsm,
		health:   newNodeHealth(3, 10*time.Second),
	}
}

func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	start := time.Now()
	defer func() {
		requestDuration.WithLabelValues("get").Observe(time.Since(start).Seconds())
	}()

	requestCount.WithLabelValues("get").Inc()

	value, version, found := s.fsm.Get(req.Key)

	return &pb.GetResponse{
		Value:   value,
		Version: version,
		Found:   found,
	}, nil
}

func (s *Server) Set(ctx context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	start := time.Now()
	defer func() {
		requestDuration.WithLabelValues("set").Observe(time.Since(start).Seconds())
	}()
	requestCount.WithLabelValues("set").Inc()

	if s.raftNode.State() != raft.Leader {
		return nil, status.Error(codes.FailedPrecondition, "not the leader")
	}

	cmd := dekvsraft.Command{
		Op:    "set",
		Key:   req.Key,
		Value: req.Value,
		TTL:   req.TtlSeconds,
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	future := s.raftNode.Apply(data, 5*time.Second)
	if err := future.Error(); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	response := future.Response()

	if responses, ok := response.([]interface{}); ok {
		if len(responses) > 0 {
			if version, ok := responses[0].(uint64); ok {
				return &pb.SetResponse{Version: version}, nil
			}
		}
	}

	return nil, status.Error(codes.Internal, "unexpected response from raft")
}

func (s *Server) Batch(ctx context.Context, req *pb.BatchRequest) (*pb.BatchResponse, error) {
	start := time.Now()
	defer func() {
		requestDuration.WithLabelValues("batch").Observe(time.Since(start).Seconds())
	}()
	requestCount.WithLabelValues("batch").Inc()

	if s.raftNode.State() != raft.Leader {
		return nil, status.Error(codes.FailedPrecondition, "not the leader")
	}

	var ops []dekvsraft.Command
	var versions []uint64
	var successes []bool

	for _, op := range req.Operations {
		if set := op.GetSet(); set != nil {
			ops = append(ops, dekvsraft.Command{
				Op:    "set",
				Key:   set.Key,
				Value: set.Value,
				TTL:   set.TtlSeconds,
			})
		} else if key := op.GetDelete(); key != "" {
			ops = append(ops, dekvsraft.Command{
				Op:  "delete",
				Key: key,
			})
		}
	}

	batchCmd := dekvsraft.BatchCommand{Ops: ops}
	data, err := json.Marshal(batchCmd)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	future := s.raftNode.Apply(data, 10*time.Second)
	if err := future.Error(); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	results := future.Response().([]interface{})
	for _, result := range results {
		switch v := result.(type) {
		case uint64:
			versions = append(versions, v)
		case bool:
			successes = append(successes, v)
		}
	}

	return &pb.BatchResponse{
		Versions:  versions,
		Successes: successes,
	}, nil
}

func (s *Server) Join(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {
	if s.raftNode.State() != raft.Leader {
		return nil, status.Error(codes.FailedPrecondition, "not the leader")
	}

	configFuture := s.raftNode.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	for _, server := range configFuture.Configuration().Servers {
		if server.ID == raft.ServerID(req.NodeId) ||
			server.Address == raft.ServerAddress(req.Address) {
			return &pb.JoinResponse{Success: true}, nil
		}
	}

	addFuture := s.raftNode.AddVoter(
		raft.ServerID(req.NodeId),
		raft.ServerAddress(req.Address),
		0,
		0,
	)

	if err := addFuture.Error(); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.JoinResponse{Success: true}, nil
}

func (s *Server) Stats(ctx context.Context, req *pb.StatsRequest) (*pb.StatsResponse, error) {
	stats := make(map[string]string)

	stats["state"] = s.raftNode.State().String()
	stats["leader"] = string(s.raftNode.Leader())

	configFuture := s.raftNode.GetConfiguration()
	if err := configFuture.Error(); err == nil {
		servers := configFuture.Configuration().Servers
		stats["nodes_count"] = fmt.Sprintf("%d", len(servers))
		for i, server := range servers {
			stats[fmt.Sprintf("node_%d", i)] = fmt.Sprintf("%s:%s", server.ID, server.Address)
		}
	}

	return &pb.StatsResponse{Stats: stats}, nil
}

var (
	requestCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kvstore_requests_total",
			Help: "Total number of requests",
		},
		[]string{"method"},
	)

	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kvstore_request_duration_seconds",
			Help:    "Request duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method"},
	)

	raftState = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kvstore_raft_state",
			Help: "Current Raft state",
		},
		[]string{"state"},
	)

	keyCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "kvstore_keys_total",
			Help: "Total number of keys in store",
		},
	)
)

func init() {
	prometheus.MustRegister(requestCount)
	prometheus.MustRegister(requestDuration)
	prometheus.MustRegister(raftState)
	prometheus.MustRegister(keyCount)
}

func (s *Server) recordMetrics() {
	go func() {
		for {
			time.Sleep(10 * time.Second)

			state := s.raftNode.State().String()
			raftState.Reset()
			raftState.WithLabelValues(state).Set(1)

			// TODO: update keyCount (add method to store)
		}
	}()
}

func newNodeHealth(maxFailures int, interval time.Duration) *nodeHealth {
	return &nodeHealth{
		failures:      make(map[string]int),
		lastSeen:      make(map[string]time.Time),
		healthStatus:  make(map[string]bool),
		maxFailures:   maxFailures,
		checkInterval: interval,
	}
}

func (s *Server) StartHealthMonitor() {
	go func() {
		ticker := time.NewTicker(s.health.checkInterval)
		defer ticker.Stop()

		for range ticker.C {
			s.checkNodesHealth()
		}
	}()
}

func (s *Server) checkNodesHealth() {
	if s.raftNode.State() != raft.Leader {
		return
	}

	configFuture := s.raftNode.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		log.Printf("Failed to get cluster config: %v", err)
		return
	}

	config := configFuture.Configuration()
	now := time.Now()

	for _, server := range config.Servers {
		nodeID := string(server.ID)

		if nodeID == string(s.raftNode.Leader()) {
			s.health.mu.Lock()
			s.health.failures[nodeID] = 0
			s.health.lastSeen[nodeID] = now
			s.health.healthStatus[nodeID] = true
			s.health.mu.Unlock()
			continue
		}

		healthy := s.pingNode(server.Address)

		s.health.mu.Lock()
		defer s.health.mu.Unlock()
		if healthy {
			s.health.failures[nodeID] = 0
			s.health.lastSeen[nodeID] = now
			s.health.healthStatus[nodeID] = true
			log.Printf("Node %s is healthy", nodeID)
		} else {
			s.health.failures[nodeID]++
			s.health.healthStatus[nodeID] = false

			failures := s.health.failures[nodeID]
			log.Printf("Node %s failed %d/%d times", nodeID, failures, s.health.maxFailures)

			if failures >= s.health.maxFailures {
				go s.safelyRemoveNode(nodeID, server)
			}
		}
	}
}

func (s *Server) pingNode(address raft.ServerAddress) bool {
	conn, err := net.DialTimeout("tcp", string(address), 2*time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func (s *Server) safelyRemoveNode(nodeID string, server raft.Server) {
	log.Printf("Attempting to remove dead node %s (%s)", nodeID, server.Address)

	if s.raftNode.State() != raft.Leader {
		log.Printf("Not a leader, cannot remove node %s", nodeID)
		return
	}

	configFuture := s.raftNode.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		log.Printf("Failed to get config before removal: %v", err)
		return
	}

	removeFuture := s.raftNode.RemoveServer(server.ID, 0, 10*time.Second)
	if err := removeFuture.Error(); err != nil {
		log.Printf("Failed to remove node %s: %v", nodeID, err)
	} else {
		log.Printf("Successfully removed dead node %s", nodeID)

		s.health.mu.Lock()
		delete(s.health.failures, nodeID)
		delete(s.health.lastSeen, nodeID)
		delete(s.health.healthStatus, nodeID)
		s.health.mu.Unlock()
	}
}

func (s *Server) RemoveNode(ctx context.Context, req *pb.RemoveNodeRequest) (*pb.RemoveNodeResponse, error) {
	if s.raftNode.State() != raft.Leader {
		return &pb.RemoveNodeResponse{
			Success: false,
			Error:   "Not the leader",
		}, nil
	}

	configFuture := s.raftNode.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return &pb.RemoveNodeResponse{
			Success: false,
			Error:   fmt.Sprintf("Failed to get config: %v", err),
		}, nil
	}

	config := configFuture.Configuration()
	var targetServer raft.Server
	found := false

	for _, server := range config.Servers {
		if string(server.ID) == req.NodeId {
			targetServer = server
			found = true
			break
		}
	}

	if !found {
		return &pb.RemoveNodeResponse{
			Success: false,
			Error:   fmt.Sprintf("Node %s not found in cluster", req.NodeId),
		}, nil
	}

	removeFuture := s.raftNode.RemoveServer(targetServer.ID, 0, 10*time.Second)
	if err := removeFuture.Error(); err != nil {
		return &pb.RemoveNodeResponse{
			Success: false,
			Error:   fmt.Sprintf("Removal failed: %v", err),
		}, nil
	}

	return &pb.RemoveNodeResponse{Success: true}, nil
}

func (s *Server) GetClusterState(ctx context.Context, req *pb.ClusterStateRequest) (*pb.ClusterStateResponse, error) {
	configFuture := s.raftNode.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	config := configFuture.Configuration()
	response := &pb.ClusterStateResponse{
		LeaderId: string(s.raftNode.Leader()),
		Nodes:    make([]*pb.NodeInfo, 0, len(config.Servers)),
	}

	for _, server := range config.Servers {
		nodeInfo := &pb.NodeInfo{
			Id:      string(server.ID),
			Address: string(server.Address),
			State:   "follower",
		}

		// if string(server.ID) == response.LeaderId {
		// 	nodeInfo.State = "leader"
		// 	nodeInfo.Healthy = true
		// } else if string(s.raftNode.Leader()) == string(server.ID) {
		// 	nodeInfo.State = "leader"
		// 	nodeInfo.Healthy = true
		// } else {
		// 	s.health.mu.RLock()
		// 	if healthy, exists := s.health.healthStatus[string(server.ID)]; exists {
		// 		nodeInfo.Healthy = healthy
		// 	} else {
		// 		nodeInfo.Healthy = s.pingNode(server.Address)
		// 	}
		// 	s.health.mu.RUnlock()
		// }

		s.health.mu.RLock()
		if healthy, exists := s.health.healthStatus[string(server.ID)]; exists {
			nodeInfo.Healthy = healthy
		} else {
			nodeInfo.Healthy = s.pingNode(server.Address)
		}
		s.health.mu.RUnlock()

		response.Nodes = append(response.Nodes, nodeInfo)
	}

	return response, nil
}

var (
	nodeHealthStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kvstore_node_health",
			Help: "Health status of cluster nodes (1=healthy, 0=unhealthy)",
		},
		[]string{"node_id"},
	)

	nodeRemovals = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "kvstore_node_removals_total",
			Help: "Total number of nodes removed due to failure",
		},
	)

	failoverDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "kvstore_failover_duration_seconds",
			Help:    "Time taken for cluster failover",
			Buckets: []float64{0.5, 1, 2, 5, 10, 30},
		},
	)
)

func init() {
	prometheus.MustRegister(nodeHealthStatus)
	prometheus.MustRegister(nodeRemovals)
	prometheus.MustRegister(failoverDuration)
}
