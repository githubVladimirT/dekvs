package detector

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/VT0x00/dekvs/internal/store"
	pb "github.com/VT0x00/dekvs/proto"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
)

type PeerStatus struct {
	Failures int
	LastSeen time.Time
}

type FailureDetector struct {
	peersAddr map[string]string // ID -> gRPC addr
	raft      *raft.Raft
	nodeID    string
	interval  time.Duration
	timeout   time.Duration
	attempts  int

	recentlyAdded map[string]time.Time
	mu            sync.Mutex

	status map[string]*PeerStatus
}

func NewFailureDetector(nodeID string, r *raft.Raft, interval, timeout time.Duration, attempts int) *FailureDetector {
	return &FailureDetector{
		peersAddr:     make(map[string]string),
		raft:          r,
		nodeID:        nodeID,
		interval:      interval,
		timeout:       timeout,
		attempts:      attempts,
		recentlyAdded: make(map[string]time.Time),
		status:        make(map[string]*PeerStatus),
	}
}

func (fd *FailureDetector) Start() {
	time.Sleep(10 * time.Second)

	ticker := time.NewTicker(fd.interval)
	defer ticker.Stop()

	for range ticker.C {
		fd.checkPeers()
	}
}

func (fd *FailureDetector) checkPeers() {
	if fd.raft.State() != raft.Leader {
		return
	}

	cfg := fd.raft.GetConfiguration().Configuration()

	for _, srv := range cfg.Servers {
		id := string(srv.ID)
		addr := string(srv.Address)

		if id == fd.nodeID {
			continue
		}

		if fd.isRecentlyAdded(id) {
			continue
		}

		if !fd.isAlive(id, addr) {
			fd.updateFailure(id)
			if fd.shouldRemove(id) {
				log.Printf("Peer %s is dead. Removing from cluster.", id)
				fd.removePeer(id)
			}
		} else {
			fd.updateSuccess(id)
		}
	}
}

func (fd *FailureDetector) updateFailure(id string) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	if _, exists := fd.status[id]; !exists {
		fd.status[id] = &PeerStatus{}
	}

	fd.status[id].Failures++
	fd.status[id].LastSeen = time.Now()
}

func (fd *FailureDetector) updateSuccess(id string) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	if s, exists := fd.status[id]; exists {
		s.Failures = 0
		s.LastSeen = time.Now()
	}
}

func (fd *FailureDetector) shouldRemove(id string) bool {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	s, exists := fd.status[id]
	if !exists {
		return false
	}

	return s.Failures >= fd.attempts
}

func (fd *FailureDetector) isAlive(id, raftAddr string) bool {
	grpcAddr, exists := fd.peersAddr[id]
	if !exists {
		// fallback на raftAddr, если не знаем gRPC-адрес
		grpcAddr = raftAddr
	}

	conn, err := grpc.Dial(grpcAddr, grpc.WithInsecure(), grpc.WithTimeout(fd.timeout))
	if err != nil {
		return false
	}
	defer conn.Close()

	client := pb.NewKVServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), fd.timeout)
	defer cancel()

	_, err = client.Ping(ctx, &pb.PingRequest{})
	return err == nil
}

func (fd *FailureDetector) removePeer(id string) {
	cmd := &store.Command{
		Op:     "removePeer",
		PeerID: id,
	}

	b, err := json.Marshal(cmd)
	if err != nil {
		log.Printf("Failed to marshal removePeer command: %v", err)
		return
	}

	f := fd.raft.Apply(b, 10000)
	if e := f.Error(); e != nil {
		log.Printf("Failed to apply removePeer command: %v", e)
	}
}

func (fd *FailureDetector) AddRecentlyAddedWithAddr(id, grpcAddr string) {
	fd.mu.Lock()
	defer fd.mu.Unlock()
	fd.recentlyAdded[id] = time.Now()
	fd.peersAddr[id] = grpcAddr
}

func (fd *FailureDetector) isRecentlyAdded(id string) bool {
	fd.mu.Lock()
	defer fd.mu.Unlock()
	t, exists := fd.recentlyAdded[id]
	if !exists {
		return false
	}
	if time.Since(t) > 15*time.Second {
		delete(fd.recentlyAdded, id)
		return false
	}
	return true
}
