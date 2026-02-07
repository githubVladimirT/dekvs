#!/bin/bash

echo "=== Raft Cluster Failover Test ==="

echo "1. Current leader:"
curl -s http://127.0.0.1:9091/metrics | grep raft_state
curl -s http://127.0.0.1:9092/metrics | grep raft_state
curl -s http://127.0.0.1:9093/metrics | grep raft_state

echo -e "\n2. Killing leader..."
LEADER_PID=$(cat pids/node1.pid)
kill -9 $LEADER_PID 2>/dev/null || true
echo "Node1 (leader) killed"

echo -e "\n3. Waiting for election..."
sleep 5

echo -e "\n4. New leader status:"
curl -s http://127.0.0.1:9092/metrics | grep raft_state
curl -s http://127.0.0.1:9093/metrics | grep raft_state

echo -e "\n5. Restarting node1..."
./server -id node1 -raft 127.0.0.1:12001 -grpc 127.0.0.1:13001 -metrics 127.0.0.1:9091 -data ./data/node1 -join 127.0.0.1:13002 > logs/node1-restarted.log 2>&1 &
echo $! > pids/node1.pid

sleep 3

echo -e "\n6. Final cluster status:"
curl -s http://127.0.0.1:9091/metrics | grep raft_state
curl -s http://127.0.0.1:9092/metrics | grep raft_state
curl -s http://127.0.0.1:9093/metrics | grep raft_state

echo -e "\n=== Test complete ==="
