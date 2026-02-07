package transport

import (
	"context"
	"encoding/json"

	//"log"
	"time"

	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"

	//"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	dekvsraft "github.com/githubVladimirT/dekvs/internal/raft"
	pb "github.com/githubVladimirT/dekvs/proto"
)

type Server struct {
	pb.UnimplementedDeKVSServer
	raftNode *raft.Raft
	fsm      dekvsraft.FSMManager
}

func NewServer(raftNode *raft.Raft, fsm dekvsraft.FSMManager) *Server {
	return &Server{
		raftNode: raftNode,
		fsm:      fsm,
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

	version := future.Response().(uint64)
	return &pb.SetResponse{Version: version}, nil
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
