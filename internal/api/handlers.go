package api

import (
	"fmt"
	"net/http"
	"os"
	"strings"
)

func (s *HTTPServer) createSnapshotHandler(w http.ResponseWriter, r *http.Request) {
	if !s.node.IsLeader() {
		http.Error(w, "Only leader can create snapshots", http.StatusServiceUnavailable)
		return
	}

	if err := s.node.CreateSnapshot(); err != nil {
		http.Error(w, fmt.Sprintf("Failed to create snapshot: %v", err), http.StatusInternalServerError)
		return
	}

	respondJSON(w, map[string]interface{}{
		"success": true,
		"message": "Snapshot created successfully",
	})
}

func (s *HTTPServer) getSnapshotInfoHandler(w http.ResponseWriter, r *http.Request) {
	info, err := s.node.GetSnapshotInfo()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get snapshot info: %v", err), http.StatusInternalServerError)
		return
	}

	respondJSON(w, info)
}

func (s *HTTPServer) listSnapshotsHandler(w http.ResponseWriter, r *http.Request) {
	nodeID := s.node.Config().NodeID
	snapshotsDir := fmt.Sprintf("./raft-data-%s/snapshots", nodeID)

	if _, err := os.Stat(snapshotsDir); os.IsNotExist(err) {
		respondJSON(w, map[string]interface{}{
			"snapshots": []string{},
			"count":     0,
		})
		return
	}

	files, err := os.ReadDir(snapshotsDir)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to read snapshots directory: %v", err), http.StatusInternalServerError)
		return
	}

	var snapshots []map[string]interface{}
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".snap") {
			info, _ := file.Info()
			snapshots = append(snapshots, map[string]interface{}{
				"name": file.Name(),
				"size": info.Size(),
			})
		}
	}

	respondJSON(w, map[string]interface{}{
		"snapshots": snapshots,
		"count":     len(snapshots),
		"directory": snapshotsDir,
	})
}
