#!/bin/bash

echo "=== Testing Cluster Formation ==="

for port in 8080 8081 8082; do
    echo "Node on port $port:"
    curl -s http://localhost:$port/cluster/status | jq .
    echo "---"
done

echo ""

LEADER_COUNT=0
for port in 8080 8081 8082; do
    if curl -s http://localhost:$port/cluster/status | jq -r '.is_leader' | grep -q true; then
        LEADER_COUNT=$((LEADER_COUNT + 1))
        LEADER_PORT=$port
    fi
done

if [ $LEADER_COUNT -eq 1 ]; then
    echo "SUCCESS: Found exactly one leader on port $LEADER_PORT"
    
    echo "Testing data replication through leader..."
    curl -X POST http://localhost:$LEADER_PORT/key \
      -H "Content-Type: application/json" \
      -d '{"key": "cluster-formation-test", "value": "data-from-leader"}'
    
    echo ""
    echo "Checking data on all nodes:"
    for port in 8080 8081 8082; do
        echo "Node $port:"
        curl -s http://localhost:$port/key/cluster-formation-test | jq .
    done
    
else
    echo "FAILED: Found $LEADER_COUNT leaders (expected 1)"
    echo "Cluster formation failed!"
    exit 1
fi

echo "=== Cluster Formation Test Complete ==="
