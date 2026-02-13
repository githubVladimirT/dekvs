package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net"

	dekvsraft "github.com/githubVladimirT/dekvs/internal/raft"
	"github.com/githubVladimirT/dekvs/internal/store"
	pb "github.com/githubVladimirT/dekvs/proto"
	"github.com/hashicorp/raft"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	nodeID = flag.String("id", "node1", "Node ID")
	addr   = flag.String("addr", "127.0.0.1:8081", "Node address")
	grpcPort   = flag.String("grpc", "9091", "gRPC port")
	join   = flag.Bool("join", false, "Join existing cluster")
)

type server struct {
	pb.UnimplementedKVServiceServer
	store *store.Store
	raft  *raft.Raft
}

func (s *server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	cmd := &store.Command{
		Op:    "put",
		Key:   req.Key,
		Value: req.Value,
	}

	b, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}

	f := s.raft.Apply(b, 10000) // 10 seconds timeout
	if e := f.Error(); e != nil {
		return nil, e
	}

	return &pb.PutResponse{Success: true}, nil
}

func (s *server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	value, found := s.store.Get(req.Key)
	return &pb.GetResponse{Value: value, Found: found}, nil
}

func main() {
	flag.Parse()

	lis, err := net.Listen("tcp", ":" + *grpcPort)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	storeInstance := store.NewStore()
	fsm := dekvsraft.NewFSM(storeInstance, nil)

	r, err := dekvsraft.NewRaft(*nodeID, *addr, fsm, *join)
	if err != nil {
		log.Fatalf("Failed to start raft: %v", err)
	}

	fsm.SetRaft(r)

	s := grpc.NewServer()
	grpcServer := &server{
		store: storeInstance,
		raft:  r,
	}

	pb.RegisterKVServiceServer(s, grpcServer)
	reflection.Register(s)

	log.Printf("Server %s running at %s", *nodeID, *addr)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func (s *server) AddPeer(ctx context.Context, req *pb.AddPeerRequest) (*pb.AddPeerResponse, error) {
	if s.raft.State() != raft.Leader {
		return &pb.AddPeerResponse{
			Success: false,
			Message: "not leader",
		}, nil
	}

	cmd := &store.Command{
		Op:       "addPeer",
		PeerID:   req.Id,
		PeerAddr: req.Addr,
	}

	b, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}

	f := s.raft.Apply(b, 10000)
	if e := f.Error(); e != nil {
		return &pb.AddPeerResponse{Success: false, Message: e.Error()}, nil
	}

	return &pb.AddPeerResponse{Success: true, Message: "added"}, nil
}

func (s *server) RemovePeer(ctx context.Context, req *pb.RemovePeerRequest) (*pb.RemovePeerResponse, error) {
	if s.raft.State() != raft.Leader {
		return &pb.RemovePeerResponse{
			Success: false,
			Message: "not leader",
		}, nil
	}

	cmd := &store.Command{
		Op:     "removePeer",
		PeerID: req.Id,
	}

	b, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}

	f := s.raft.Apply(b, 10000)
	if e := f.Error(); e != nil {
		return &pb.RemovePeerResponse{Success: false, Message: e.Error()}, nil
	}

	return &pb.RemovePeerResponse{Success: true, Message: "removed"}, nil
}

func (s *server) ClusterState(ctx context.Context, req *pb.ClusterStateRequest) (*pb.ClusterStateResponse, error) {
	state := s.raft.Stats()
	leader := s.raft.Leader()
	cfg := s.raft.GetConfiguration().Configuration()

	peers := make([]*pb.Peer, len(cfg.Servers))
	for i, srv := range cfg.Servers {
		peers[i] = &pb.Peer{
			Id:       string(srv.ID),
			Addr:     string(srv.Address),
			IsLeader: string(srv.Address) == string(leader),
		}
	}

	return &pb.ClusterStateResponse{
		Leader: state["leader"],
		Peers:  peers,
	}, nil
}
