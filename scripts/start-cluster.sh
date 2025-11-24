#!/bin/bash

echo "=== Starting DeKVS 3-Node Cluster ==="

# echo "Cleaning previous data..."  
# rm -rf ./raft-data-node1 ./raft-data-node2 ./raft-data-node3

wait_for_port() {
    local port=$1
    local name=$2
    echo "Waiting for $name to listen on port $port..."
    
    for i in {1..30}; do
        if netstat -tulpn 2>/dev/null | grep ":$port " > /dev/null; then
            echo "$name is listening on port $port"
            return 0
        fi
        sleep 1
    done
    echo "Timeout waiting for $name on port $port"
    return 1
}

# Start node 1
echo "Starting node-1 as bootstrap node..."
export NODE_ID="node-1"
export RAFT_ADDR="127.0.0.1:9090"
export HTTP_ADDR=":8080"
export DATA_DIR="./raft-data-node1"
export RAFT_PEERS="node-2@127.0.0.1:9091,node-3@127.0.0.1:9092"
go run ./cmd/kv-server &

# Wait for node 1 to start
wait_for_port 8080 "node-1 HTTP"
wait_for_port 9090 "node-1 Raft"

# Give node 1 time to bootstrap and become leader
echo "Giving node-1 time to bootstrap cluster..."
sleep 10

# Start node 2
echo "Starting node-2..."
export NODE_ID="node-2"
export RAFT_ADDR="127.0.0.1:9091"
export HTTP_ADDR=":8081"
export DATA_DIR="./raft-data-node2"
export RAFT_PEERS="node-1@127.0.0.1:9090,node-3@127.0.0.1:9092"
go run ./cmd/kv-server &

# Wait for node 2 to start
wait_for_port 8081 "node-2 HTTP" 
wait_for_port 9091 "node-2 Raft"

# Start node 3
echo "Starting node-3..."
export NODE_ID="node-3"
export RAFT_ADDR="127.0.0.1:9092"
export HTTP_ADDR=":8082"
export DATA_DIR="./raft-data-node3"
export RAFT_PEERS="node-1@127.0.0.1:9090,node-2@127.0.0.1:9091"
go run ./cmd/kv-server &

# Wait for node 3 to start
wait_for_port 8082 "node-3 HTTP"
wait_for_port 9092 "node-3 Raft"

echo "=== All nodes started ==="
echo "Waiting for cluster formation..."
sleep 20

echo "=== Testing Cluster Status ==="
./scripts/test-cluster-formation.sh

echo ""
echo "=== Cluster Startup Complete ==="
echo "Nodes:"
echo "  node-1: http://localhost:8080"
echo "  node-2: http://localhost:8081"
echo "  node-3: http://localhost:8082"
echo ""
echo "Run './scripts/test-cluster-operations.sh' to test data replication"
echo "Press Ctrl+C to stop all nodes"

# Wait for interrupt
wait
