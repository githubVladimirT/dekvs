package api

import (
    "dekvs/internal/raft"
    // "dekvs/internal/store"
    "dekvs/pkg/types"
    "encoding/json"
    "net/http"
    "strings"
)

type HTTPServer struct {
    node *raft.Node
}

func NewHTTPServer(node *raft.Node) *HTTPServer {
    return &HTTPServer{node: node}
}

func (h *HTTPServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")
    w.Header().Set("Access-Control-Allow-Origin", "*")
    w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
    w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
    
    if r.Method == "OPTIONS" {
        w.WriteHeader(http.StatusOK)
        return
    }
    
    switch {
    case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/key/"):
        h.GetHandler(w, r)
    case r.Method == "POST" && r.URL.Path == "/key":
        h.PutHandler(w, r)
    case r.Method == "DELETE" && strings.HasPrefix(r.URL.Path, "/key/"):
        h.DeleteHandler(w, r)
    default:
        http.NotFound(w, r)
    }
}

// JSON structures
type PutRequest struct {
    Key   string `json:"key"`
    Value string `json:"value"` // Use string instead of []byte to avoid base64
}

type GetResponse struct {
    Key     string `json:"key"`
    Value   string `json:"value"`
    Version int64  `json:"version"`
    Success bool   `json:"success"`
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
    
    jsonResponse := GetResponse{
        Key:     key,
        Value:   string(resp.Value), // Convert []byte to string
        Version: resp.Version,
        Success: resp.Success,
    }
    
    json.NewEncoder(w).Encode(jsonResponse)
}

func (h *HTTPServer) PutHandler(w http.ResponseWriter, r *http.Request) {
    var req PutRequest
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
        Value: []byte(req.Value), // Convert string to []byte
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
