#!/bin/bash

echo "Starting cluster with 3 nodes..."

pkill -f "./server" || true
sleep 2

rm -rf ./data/node1 ./data/node2 ./data/node3

echo "Starting node1..."
./server -id node1 -raft 127.0.0.1:12001 -grpc 127.0.0.1:13001 -metrics 127.0.0.1:9091 -data ./data/node1 > logs/node1.log 2>&1 &
echo $! > pids/node1.pid

sleep 3

echo "Starting node2..."
./server -id node2 -raft 127.0.0.1:12002 -grpc 127.0.0.1:13002 -metrics 127.0.0.1:9092 -data ./data/node2 -join 127.0.0.1:13001 > logs/node2.log 2>&1 &
echo $! > pids/node2.pid

sleep 2

echo "Starting node3..."
./server -id node3 -raft 127.0.0.1:12003 -grpc 127.0.0.1:13003 -metrics 127.0.0.1:9093 -data ./data/node3 -join 127.0.0.1:13001 > logs/node3.log 2>&1 &
echo $! > pids/node3.pid

sleep 2

echo "Cluster started!"
echo "Monitoring logs..."

tail -f logs/node1.log logs/node2.log logs/node3.log
