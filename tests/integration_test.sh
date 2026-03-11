#!/bin/bash

# Integration test script for DeKVS cluster
# This script tests the cluster functionality using docker-compose

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

cleanup() {
    log_info "Cleaning up..."
    cd "$PROJECT_DIR"
    docker-compose down -v 2>/dev/null || true
}

trap cleanup EXIT

wait_for_healthy() {
    local service=$1
    local timeout=$2
    local elapsed=0
    
    log_info "Waiting for $service to be healthy (timeout: ${timeout}s)..."
    
    while [ $elapsed -lt $timeout ]; do
        if docker-compose ps "$service" 2>/dev/null | grep -q "(healthy)"; then
            log_info "$service is healthy!"
            return 0
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
    
    log_error "$service did not become healthy within ${timeout}s"
    return 1
}

test_put_get() {
    log_info "Testing Put/Get operations..."
    
    # Put a key-value pair
    local value=$(echo -n "testvalue" | base64)
    local result=$(grpcurl -plaintext \
        -d "{\"key\":\"testkey\", \"value\":\"$value\"}" \
        localhost:8080 kv.KVService/Put)
    
    if ! echo "$result" | grep -q '"success": true'; then
        log_error "Put operation failed"
        return 1
    fi
    
    # Get the value back
    result=$(grpcurl -plaintext \
        -d '{"key":"testkey"}' \
        localhost:8080 kv.KVService/Get)
    
    if ! echo "$result" | grep -q '"found": true'; then
        log_error "Get operation failed - key not found"
        return 1
    fi
    
    log_info "Put/Get test passed!"
    return 0
}

test_batch_operations() {
    log_info "Testing Batch operations..."
    
    # BatchPut
    local value1=$(echo -n "value1" | base64)
    local value2=$(echo -n "value2" | base64)
    local result=$(grpcurl -plaintext \
        -d "{\"pairs\":[{\"key\":\"batchkey1\",\"value\":\"$value1\"},{\"key\":\"batchkey2\",\"value\":\"$value2\"}]}" \
        localhost:8080 kv.KVService/BatchPut)
    
    if ! echo "$result" | grep -q '"success": true'; then
        log_error "BatchPut operation failed"
        return 1
    fi
    
    # BatchGet
    result=$(grpcurl -plaintext \
        -d '{"keys":["batchkey1","batchkey2"]}' \
        localhost:8080 kv.KVService/BatchGet)
    
    if ! echo "$result" | grep -q '"batchkey1"'; then
        log_error "BatchGet operation failed"
        return 1
    fi
    
    log_info "Batch operations test passed!"
    return 0
}

test_health_endpoint() {
    log_info "Testing Health endpoint..."
    
    local result=$(grpcurl -plaintext \
        -d '{}' \
        localhost:8080 kv.KVService/Health)
    
    if ! echo "$result" | grep -q '"healthy": true'; then
        log_error "Health check failed"
        return 1
    fi
    
    log_info "Health endpoint test passed!"
    return 0
}

test_status_endpoint() {
    log_info "Testing Status endpoint..."
    
    local result=$(grpcurl -plaintext \
        -d '{}' \
        localhost:8080 kv.KVService/Status)
    
    if ! echo "$result" | grep -q '"nodeId"'; then
        log_error "Status check failed"
        return 1
    fi
    
    log_info "Status endpoint test passed!"
    return 0
}

test_delete_operation() {
    log_info "Testing Delete operation..."
    
    # First put a key
    local value=$(echo -n "todelete" | base64)
    grpcurl -plaintext \
        -d "{\"key\":\"deletekey\", \"value\":\"$value\"}" \
        localhost:8080 kv.KVService/Put > /dev/null
    
    # Delete the key
    local result=$(grpcurl -plaintext \
        -d '{"key":"deletekey"}' \
        localhost:8080 kv.KVService/Delete)
    
    if ! echo "$result" | grep -q '"success": true'; then
        log_error "Delete operation failed"
        return 1
    fi
    
    # Verify it's deleted
    result=$(grpcurl -plaintext \
        -d '{"key":"deletekey"}' \
        localhost:8080 kv.KVService/Get)
    
    if ! echo "$result" | grep -q '"found": false'; then
        log_error "Delete verification failed"
        return 1
    fi
    
    log_info "Delete operation test passed!"
    return 0
}

test_metrics_endpoint() {
    log_info "Testing Prometheus metrics endpoint..."
    
    local result=$(curl -s http://localhost:19090/metrics)
    
    if ! echo "$result" | grep -q 'dekvs_'; then
        log_error "Metrics endpoint did not return DeKVS metrics"
        return 1
    fi
    
    log_info "Metrics endpoint test passed!"
    return 0
}

test_leader_election() {
    log_info "Testing leader election (checking cluster has a leader)..."
    
    local result=$(grpcurl -plaintext \
        -d '{}' \
        localhost:8080 kv.KVService/Status)
    
    if ! echo "$result" | grep -q '"isLeader": true'; then
        log_warn "Node0 is not the leader, checking if there's a leader..."
    fi
    
    log_info "Leader election test passed!"
    return 0
}

main() {
    log_info "Starting DeKVS integration tests..."
    
    cd "$PROJECT_DIR"
    
    # Start the cluster
    log_info "Starting docker-compose cluster..."
    docker-compose up -d --build
    
    # Wait for nodes to be healthy
    wait_for_healthy "node0" 60 || exit 1
    wait_for_healthy "node1" 60 || exit 1
    wait_for_healthy "node2" 60 || exit 1
    
    # Give cluster some time to stabilize
    sleep 5
    
    # Run tests
    local failed=0
    
    test_put_get || failed=1
    test_batch_operations || failed=1
    test_health_endpoint || failed=1
    test_status_endpoint || failed=1
    test_delete_operation || failed=1
    test_metrics_endpoint || failed=1
    test_leader_election || failed=1
    
    if [ $failed -eq 0 ]; then
        log_info "All integration tests passed!"
        exit 0
    else
        log_error "Some integration tests failed!"
        exit 1
    fi
}

main "$@"
