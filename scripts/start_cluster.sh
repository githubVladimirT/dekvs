#!/bin/bash

set -e

echo "Starting 3-node cluster..."

# Очищаем старые логи Raft
rm -rf /tmp/raft-log-*

# node-root
echo "Starting node1..."
ptyxis --title="Node1" -- bash -c "
    go run ../cmd/server/main.go --id=node1 --addr=127.0.0.1:8080 --grpc :9090 --join=false;
    exec bash
"

sleep 3

# node2
echo "Starting node2..."
ptyxis --title="Node2" -- bash -c "
    go run ../cmd/server/main.go --id=node2 --addr=127.0.0.1:8082 --grpc :9092 --join=true;
    exec bash
"

sleep 3

# node3
echo "Starting node3..."
ptyxis --title="Node3" -- bash -c "
    go run ../cmd/server/main.go --id=node3 --addr=127.0.0.1:8083 --grpc :9093 --join=true;
    exec bash
"

echo "Cluster started. Press Ctrl+C to stop."
wait
