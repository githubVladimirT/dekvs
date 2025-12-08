#!/bin/bash

echo "=== Enhanced Snapshot System Test ==="

rm -rf ./test-snapshot-data
mkdir -p ./test-snapshot-data

echo "1. Starting test node..."
export NODE_ID="snapshot-test-node"
export RAFT_ADDR="127.0.0.1:19990"
export HTTP_ADDR=":19980"
export DATA_DIR="./test-snapshot-data"
export RAFT_PEERS=""
go run ./cmd/kv-server &

SERVER_PID=$!
echo "Server PID: $SERVER_PID"

sleep 5

echo "2. Creating test data..."
curl -X POST http://localhost:19980/key \
  -H "Content-Type: application/json" \
  -d '{"key": "string-key", "value": "simple string value"}'

curl -X POST http://localhost:19980/key \
  -H "Content-Type: application/json" \
  -d '{"key": "json-key", "value": "{\"name\":\"test\",\"count\":42}"}'

curl -X POST http://localhost:19980/key \
  -H "Content-Type: application/json" \
  -d '{"key": "binary-key", "value": "binary\0data\0with\0nulls"}'

echo "3. Verifying initial data..."
curl http://localhost:19980/key/string-key
curl http://localhost:19980/key/json-key
curl http://localhost:19980/key/binary-key

echo "4. Creating snapshot..."
SNAPSHOT_RESULT=$(curl -s -X POST http://localhost:19980/snapshot/create)
echo "Snapshot result: $SNAPSHOT_RESULT"

sleep 3

echo "5. Creating more data after snapshot..."
curl -X POST http://localhost:19980/key \
  -H "Content-Type: application/json" \
  -d '{"key": "after-snapshot", "value": "this should be lost after recovery"}'

echo "6. Checking snapshot status..."
curl http://localhost:19980/snapshot/status

echo "7. Stopping server..."
kill $SERVER_PID
wait $SERVER_PID 2>/dev/null

echo "8. Checking snapshot files..."
find ./test-snapshot-data -name "*.snap" -exec ls -la {} \;
find ./test-snapshot-data/snapshots -type f -exec ls -la {} \; 2>/dev/null || echo "No snapshots directory"

echo "9. Restarting server (should recover from snapshot)..."
go run ./cmd/kv-server &

SERVER_PID=$!
sleep 5

echo "10. Verifying recovered data..."
echo "Data that should exist (from snapshot):"
curl http://localhost:19980/key/string-key
curl http://localhost:19980/key/json-key
curl http://localhost:19980/key/binary-key

echo "Data that should NOT exist (created after snapshot):"
curl http://localhost:19980/key/after-snapshot || echo "Key not found (expected)"

echo "11. Testing compression..."
SNAPSHOT_FILE=$(find ./test-snapshot-data -name "*.snap" | head -1)
if [ -n "$SNAPSHOT_FILE" ]; then
    echo "Snapshot file: $SNAPSHOT_FILE"
    file "$SNAPSHOT_FILE"
    ls -la "$SNAPSHOT_FILE"
fi

echo "12. Cleanup..."
kill $SERVER_PID 2>/dev/null
rm -rf ./test-snapshot-data

echo "=== Enhanced Snapshot Test Complete ==="