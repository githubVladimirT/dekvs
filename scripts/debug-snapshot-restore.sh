#!/bin/bash

echo "=== Final Snapshot System Test ==="

rm -rf ./final-test-data
mkdir -p ./final-test-data

echo "1. Phase 1: Initial startup and data creation..."
export NODE_ID="final-test-node"
export RAFT_ADDR="127.0.0.1:39990"
export HTTP_ADDR=":39980"
export DATA_DIR="./final-test-data"
export RAFT_PEERS=""
go run ./cmd/kv-server &

SERVER_PID=$!
sleep 5

echo "2. Creating unique test data..."
UNIQUE_PREFIX="data_$(date +%s)_"
curl -X POST http://localhost:39980/key \
  -H "Content-Type: application/json" \
  -d "{\"key\": \"test-key-1\", \"value\": \"${UNIQUE_PREFIX}value1\"}"

curl -X POST http://localhost:39980/key \
  -H "Content-Type: application/json" \
  -d "{\"key\": \"test-key-2\", \"value\": \"${UNIQUE_PREFIX}value2\"}"

echo "3. Verifying initial data:"
curl http://localhost:39980/key/test-key-1
curl http://localhost:39980/key/test-key-2

echo "4. Creating snapshot..."
curl -X POST http://localhost:39980/snapshot/create
sleep 3

echo "5. Creating post-snapshot data..."
curl -X POST http://localhost:39980/key \
  -H "Content-Type: application/json" \
  -d '{"key": "post-snapshot-key", "value": "this_should_disappear_after_restore"}'

echo "6. Current data before restart:"
curl http://localhost:39980/key/test-key-1
curl http://localhost:39980/key/test-key-2
curl http://localhost:39980/key/post-snapshot-key

echo "7. Stopping server..."
kill $SERVER_PID
sleep 2

echo "8. Restarting server (should restore from snapshot)..."
go run ./cmd/kv-server &

SERVER_PID=$!
sleep 5

echo "9. Verifying restored data:"
echo "Data that should exist (from snapshot):"
curl http://localhost:39980/key/test-key-1
curl http://localhost:39980/key/test-key-2

echo "Data that should NOT exist (created after snapshot):"
curl http://localhost:39980/key/post-snapshot-key

echo "10. Final snapshot status:"
curl http://localhost:39980/snapshot/status

echo "11. Cleanup..."
kill $SERVER_PID 2>/dev/null
rm -rf ./final-test-data

echo "=== Final Test Complete ==="