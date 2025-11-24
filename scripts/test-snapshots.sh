#!/bin/bash

echo "Testing DeKVS real snapshots..."

echo "Creating test data..."
curl -X POST http://localhost:8080/key \
  -H "Content-Type: application/json" \
  -d '{"key": "snapshot-test-1", "value": "important-data-1"}'
  
curl -X POST http://localhost:8080/key \
  -H "Content-Type: application/json" \
  -d '{"key": "snapshot-test-2", "value": "important-data-2"}'

echo -e "\nSnapshot status BEFORE:"
curl http://localhost:8080/snapshot/status | jq .

echo -e "\nCreating snapshot..."
curl -X POST http://localhost:8080/snapshot/create

sleep 3

echo -e "\nSnapshot status AFTER:"
curl http://localhost:8080/snapshot/status | jq .

echo -e "\nTesting data access:"
curl http://localhost:8080/key/snapshot-test-1 | jq .
curl http://localhost:8080/key/snapshot-test-2 | jq .

echo -e "\nCreating more data after snapshot..."
curl -X POST http://localhost:8080/key \
  -H "Content-Type: application/json" \
  -d '{"key": "snapshot-test-3", "value": "data-after-snapshot"}'

echo -e "\nReal snapshot test completed!"
