#!/bin/bash

echo "=== Testing DeKVS Snapshot Features ==="

echo "Cleaning previous data..."
rm -rf ./raft-data-node1

echo "Starting single node..."
export NODE_ID="node-1"
export RAFT_ADDR="127.0.0.1:9090"
export HTTP_ADDR=":8080"
export DATA_DIR="./raft-data-node1"
export RAFT_PEERS=""
go run ./cmd/kv-server &

NODE_PID=$!
echo "Node PID: $NODE_PID"

echo "Waiting for node to start..."
sleep 5

echo "=== Phase 1: Creating Test Data ==="

curl -X POST http://localhost:8080/key \
  -H "Content-Type: application/json" \
  -d '{"key": "user:1", "value": "{\"name\":\"John\",\"age\":30}"}'

curl -X POST http://localhost:8080/key \
  -H "Content-Type: application/json" \
  -d '{"key": "user:2", "value": "{\"name\":\"Alice\",\"age\":25}"}'

curl -X POST http://localhost:8080/key \
  -H "Content-Type: application/json" \
  -d '{"key": "config:app", "value": "{\"version\":\"1.0.0\",\"features\":[\"auth\",\"api\"]}"}'

echo "Data before snapshot:"
curl http://localhost:8080/key/user:1
curl http://localhost:8080/key/user:2
curl http://localhost:8080/key/config:app

echo ""
echo "=== Phase 2: Checking Snapshot Status ==="

echo "Snapshot status before creation:"
curl http://localhost:8080/snapshot/status | jq .

echo ""
echo "=== Phase 3: Creating Manual Snapshot ==="

echo "Creating manual snapshot..."
curl -X POST http://localhost:8080/snapshot/create

sleep 3

echo "Snapshot status after creation:"
curl http://localhost:8080/snapshot/status | jq .

echo ""
echo "=== Phase 4: Creating More Data ==="

curl -X POST http://localhost:8080/key \
  -H "Content-Type: application/json" \
  -d '{"key": "temp:data", "value": "this-may-be-lost"}'

echo "All data after snapshot:"
curl http://localhost:8080/key/user:1
curl http://localhost:8080/key/user:2
curl http://localhost:8080/key/config:app
curl http://localhost:8080/key/temp:data

echo ""
echo "=== Phase 5: Testing Snapshot Recovery ==="

echo "Stopping node..."
kill $NODE_PID
wait $NODE_PID 2>/dev/null

echo "Snapshot files:"
find ./raft-data-node1/snapshots -type f -name "*.bin" | head -5

echo "Restarting node..."
go run ./cmd/kv-server &

sleep 5

echo "=== Phase 6: Verifying Recovered Data ==="

echo "Data after recovery:"
curl http://localhost:8080/key/user:1
curl http://localhost:8080/key/user:2
curl http://localhost:8080/key/config:app

echo "Temporary data (should be missing):"
curl http://localhost:8080/key/temp:data || echo "Key not found (expected)"

echo ""
echo "=== Phase 7: Testing Snapshot Metadata ==="

echo "Final snapshot status:"
curl http://localhost:8080/snapshot/status | jq .

echo ""
echo "=== Snapshot Features Test Complete ==="

pkill -f "kv-server"