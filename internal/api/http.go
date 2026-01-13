package api

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/githubVladimirT/dekvs/internal/raft"
	"github.com/githubVladimirT/dekvs/pkg/types"
)

type HTTPServer struct {
	node *raft.Node
}

func NewHTTPServer(node *raft.Node) *HTTPServer {
	return &HTTPServer{node: node}
}

func (h *HTTPServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	switch {
	case r.Method == types.OpGet && strings.HasPrefix(r.URL.Path, "/key/"):
		h.GetHandler(w, r)
	case r.Method == types.OpPost && r.URL.Path == "/key":
		h.PutHandler(w, r)
	case r.Method == types.OpDelete && strings.HasPrefix(r.URL.Path, "/key/"):
		h.DeleteHandler(w, r)
	case r.Method == types.OpGet && r.URL.Path == "/cluster/status":
		h.ClusterStatusHandler(w, r)
	case r.Method == types.OpGet && r.URL.Path == "/cluster/nodes":
		h.ClusterNodesHandler(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (h *HTTPServer) GetHandler(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/key/")
	if key == "" {
		http.Error(w, `{"error": "Key is required"}`, http.StatusBadRequest)
		return
	}

	resp, err := h.node.Get(key)
	if err != nil {
		if err == types.ErrKeyNotFound {
			http.Error(w, `{"error": "Key not found"}`, http.StatusNotFound)
			return
		}
		http.Error(w, `{"error": "`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"key":     key,
		"value":   string(resp.Value),
		"version": resp.Version,
		"success": resp.Success,
	})
}

func (h *HTTPServer) PutHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Key   string `json:"key"`
		Value string `json:"value"`
		TTL   int    `json:"ttl,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error": "Invalid JSON"}`, http.StatusBadRequest)
		return
	}

	if req.Key == "" {
		http.Error(w, `{"error": "Key is required"}`, http.StatusBadRequest)
		return
	}

	cmd := types.Command{
		Op:    types.OpPut,
		Key:   req.Key,
		Value: []byte(req.Value),
	}

	if err := h.node.ApplyCommand(cmd); err != nil {
		http.Error(w, `{"error": "`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(map[string]bool{"success": true})
}

func (h *HTTPServer) DeleteHandler(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/key/")
	if key == "" {
		http.Error(w, `{"error": "Key is required"}`, http.StatusBadRequest)
		return
	}

	cmd := types.Command{
		Op:  types.OpDelete,
		Key: key,
	}

	if err := h.node.ApplyCommand(cmd); err != nil {
		http.Error(w, `{"error": "`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(map[string]bool{"success": true})
}

func (h *HTTPServer) ClusterStatusHandler(w http.ResponseWriter, r *http.Request) {
	config := h.node.Config()
	status := map[string]interface{}{
		"node_id":   config.NodeID,
		"state":     h.node.State(),
		"leader":    h.node.Leader(),
		"is_leader": h.node.IsLeader(),
		"ready":     h.node.IsReady(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func (h *HTTPServer) ClusterNodesHandler(w http.ResponseWriter, r *http.Request) {
	config := h.node.Config()
	stats := h.node.Stats()

	response := map[string]interface{}{
		"current_node": map[string]interface{}{
			"id":      config.NodeID,
			"address": config.RaftAddr,
			"state":   h.node.State(),
		},
		"raft_stats": stats,
		"peers":      config.Peers,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
