#!/bin/bash

echo "=== DeKVS Snapshot Integrity Test ==="

echo "Cleaning previous data..."
rm -rf ./raft-data-node1 ./raft-data-node2 ./raft-data-node3

echo "Starting node-1..."
export NODE_ID="node-1"
export RAFT_ADDR="127.0.0.1:9090"
export HTTP_ADDR=":8080"
export DATA_DIR="./raft-data-node1"
export RAFT_PEERS=""
go run ./cmd/kv-server &

NODE1_PID=$!
echo "Node-1 PID: $NODE1_PID"

echo "Waiting for node to start..."
sleep 5

echo "=== Phase 1: Creating Test Data ==="

curl -X POST http://localhost:8080/key \
  -H "Content-Type: application/json" \
  -d '{"key": "before-snapshot-1", "value": "data-before-snapshot-1"}'

curl -X POST http://localhost:8080/key \
  -H "Content-Type: application/json" \
  -d '{"key": "before-snapshot-2", "value": "data-before-snapshot-2"}'

echo "Data before snapshot:"
curl http://localhost:8080/key/before-snapshot-1
curl http://localhost:8080/key/before-snapshot-2

echo ""
echo "=== Phase 2: Creating Snapshot ==="

curl -X POST http://localhost:8080/snapshot/create

sleep 3

echo "Snapshot status:"
curl http://localhost:8080/snapshot/status

echo ""
echo "=== Phase 3: Creating More Data After Snapshot ==="

curl -X POST http://localhost:8080/key \
  -H "Content-Type: application/json" \
  -d '{"key": "after-snapshot-1", "value": "data-after-snapshot-1"}'

echo "All data after snapshot:"
curl http://localhost:8080/key/before-snapshot-1
curl http://localhost:8080/key/before-snapshot-2
curl http://localhost:8080/key/after-snapshot-1

echo ""
echo "=== Phase 4: Testing Snapshot Recovery ==="

echo "Stopping node..."
kill $NODE1_PID
pkill -f "kv-server"
wait $NODE1_PID 2>/dev/null

echo "Checking snapshots directory..."
ls -la ./raft-data-node1/snapshots/

echo "Restarting node..."
go run ./cmd/kv-server &

sleep 5

echo "=== Phase 5: Verifying Recovered Data ==="

echo "Data after recovery from snapshot:"
curl http://localhost:8080/key/before-snapshot-1
curl http://localhost:8080/key/before-snapshot-2

echo "Data created after snapshot (may be lost):"
curl http://localhost:8080/key/after-snapshot-1 || echo "Key not found (expected)"

echo ""
echo "=== Snapshot Integrity Test Complete ==="

pkill -f "kv-server"