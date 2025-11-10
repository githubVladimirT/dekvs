#!/bin/bash

echo "Starting DeKVS 3-node cluster..."

# Clean up previous data
rm -rf ./raft-data*

# Start node 1
echo "Starting node-1..."
export NODE_ID="node-1"
export RAFT_ADDR="127.0.0.1:9090"
export HTTP_ADDR=":8080"
export DATA_DIR="./raft-data-node1"
#export RAFT_PEERS="node-2@127.0.0.1:9091,node-3@127.0.0.1:9092"
go run ./cmd/kv-server &

# Wait for node 1 to start
sleep 5

# Start node 2
echo "Starting node-2..."
export NODE_ID="node-2"
export RAFT_ADDR="127.0.0.1:9091"
export HTTP_ADDR=":8081"
export DATA_DIR="./raft-data-node2"
export RAFT_PEERS="node-1@127.0.0.1:9090"
go run ./cmd/kv-server &

# Wait for node 2 to start
sleep 5

# Start node 3
echo "Starting node-3..."
export NODE_ID="node-3"
export RAFT_ADDR="127.0.0.1:9092"
export HTTP_ADDR=":8082"
export DATA_DIR="./raft-data-node3"
export RAFT_PEERS="node-1@127.0.0.1:9090,node-2@127.0.0.1:9091"
go run ./cmd/kv-server &

echo "Cluster started! Nodes running on:"
echo "  node-1: http://localhost:8080"
echo "  node-2: http://localhost:8081" 
echo "  node-3: http://localhost:8082"
echo ""
echo "Waiting for cluster to stabilize..."
sleep 10

echo "Testing cluster status..."
curl -s http://localhost:8080/cluster/status | jq .
curl -s http://localhost:8081/cluster/status | jq .
curl -s http://localhost:8082/cluster/status | jq .

echo ""
echo "Press Ctrl+C to stop all nodes"

# Wait for interrupt
wait
