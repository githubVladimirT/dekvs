#!/bin/bash

# Stress test script for DeKVS cluster
# This script performs various stress tests on the cluster to evaluate performance
# and stability under load

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Configuration
: ${STRESS_DURATION:=60}          # Duration of each stress test in seconds
: ${CONCURRENT_CLIENTS:=10}       # Number of concurrent clients
: ${REQUEST_SIZE:=1024}           # Size of values in bytes
: ${BATCH_SIZE:=10}               # Number of pairs per batch operation
: ${KEY_PREFIX:="stress"}         # Prefix for stress test keys
: ${GRPC_HOST:="localhost"}       # gRPC host
: ${GRPC_PORT:="8080"}            # gRPC port (node0)
: ${METRICS_PORT:="30090"}        # Metrics port

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Counters (use files for cross-process communication)
TEMP_DIR=""
SUCCESS_FILE=""
FAILURE_FILE=""
LATENCY_FILE=""

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_debug() {
    echo -e "${CYAN}[DEBUG]${NC} $1"
}

log_section() {
    echo -e "\n${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}\n"
}

# Generate random value of specified size
generate_value() {
    local size=$1
    head -c "$size" /dev/urandom | base64 -w 0 | head -c "$size"
}

# Get current timestamp in milliseconds
get_timestamp_ms() {
    date +%s%3N
}

# Initialize counters
init_counters() {
    TEMP_DIR=$(mktemp -d)
    SUCCESS_FILE="${TEMP_DIR}/success"
    FAILURE_FILE="${TEMP_DIR}/failure"
    LATENCY_FILE="${TEMP_DIR}/latency"
    echo 0 > "$SUCCESS_FILE"
    echo 0 > "$FAILURE_FILE"
    echo 0 > "$LATENCY_FILE"
}

# Increment success counter
record_success() {
    local latency=$1
    (
        flock -x 200
        local success=$(cat "$SUCCESS_FILE")
        echo $((success + 1)) > "$SUCCESS_FILE"
        local total_latency=$(cat "$LATENCY_FILE")
        echo $((total_latency + latency)) > "$LATENCY_FILE"
    ) 200>"${TEMP_DIR}/lock_success"
}

# Increment failure counter
record_failure() {
    (
        flock -x 200
        local failure=$(cat "$FAILURE_FILE")
        echo $((failure + 1)) > "$FAILURE_FILE"
    ) 200>"${TEMP_DIR}/lock_failure"
}

# Get counter values
get_counters() {
    SUCCESSFUL_REQUESTS=$(cat "$SUCCESS_FILE")
    FAILED_REQUESTS=$(cat "$FAILURE_FILE")
    TOTAL_LATENCY=$(cat "$LATENCY_FILE")
    TOTAL_REQUESTS=$((SUCCESSFUL_REQUESTS + FAILED_REQUESTS))
}

# Cleanup temp files
cleanup_counters() {
    if [ -n "$TEMP_DIR" ] && [ -d "$TEMP_DIR" ]; then
        rm -rf "$TEMP_DIR"
    fi
}

# Calculate and display statistics
print_statistics() {
    local test_name=$1
    local start_time=$2
    local end_time=$3
    
    get_counters
    
    local duration=$(( (end_time - start_time) / 1000 ))
    local avg_latency=0
    
    if [ $SUCCESSFUL_REQUESTS -gt 0 ]; then
        avg_latency=$((TOTAL_LATENCY / SUCCESSFUL_REQUESTS))
    fi
    
    local rps=0
    if [ $duration -gt 0 ]; then
        rps=$((SUCCESSFUL_REQUESTS / duration))
    fi
    
    local success_rate=0
    if [ $TOTAL_REQUESTS -gt 0 ]; then
        success_rate=$(awk "BEGIN {printf \"%.2f\", ($SUCCESSFUL_REQUESTS/$TOTAL_REQUESTS)*100}")
    fi
    
    echo -e "\n${CYAN}--- Statistics for $test_name ---${NC}"
    echo -e "Duration: ${duration}s"
    echo -e "Total Requests: ${TOTAL_REQUESTS}"
    echo -e "Successful: ${GREEN}${SUCCESSFUL_REQUESTS}${NC}"
    echo -e "Failed: ${RED}${FAILED_REQUESTS}${NC}"
    echo -e "Success Rate: ${success_rate}%"
    echo -e "Requests/sec: ${rps}"
    echo -e "Avg Latency: ${avg_latency}ms"
    echo -e "----------------------------------------\n"
}

# Check if cluster is ready
wait_for_cluster() {
    local timeout=${1:-30}
    local elapsed=0

    log_info "Waiting for cluster to be ready..."

    while [ $elapsed -lt $timeout ]; do
        local result=$(grpcurl -plaintext -max-time 2 \
            -d '{}' \
            "${GRPC_HOST}:${GRPC_PORT}" kv.KVService/Health 2>&1)
        
        if echo "$result" | grep -q '"healthy": *true'; then
            log_info "Cluster is ready!"
            return 0
        fi
        
        sleep 1
        elapsed=$((elapsed + 1))
    done

    log_error "Cluster did not become ready within ${timeout}s"
    
    # Debug: try manual check
    log_debug "Manual health check:"
    grpcurl -plaintext -max-time 5 \
        -d '{}' \
        "${GRPC_HOST}:${GRPC_PORT}" kv.KVService/Health 2>&1 || true
    
    return 1
}

# Client worker for PUT stress test
put_worker() {
    local client_id=$1
    local end_time=$2
    
    while [ $(get_timestamp_ms) -lt $end_time ]; do
        local key="${KEY_PREFIX}:put:client${client_id}:key$((RANDOM % 10000))"
        local value=$(generate_value $REQUEST_SIZE)
        
        local req_start=$(get_timestamp_ms)
        local result=$(grpcurl -plaintext -max-time 5 \
            -d "{\"key\":\"${key}\", \"value\":\"${value}\"}" \
            "${GRPC_HOST}:${GRPC_PORT}" kv.KVService/Put 2>&1)
        local req_end=$(get_timestamp_ms)
        local latency=$((req_end - req_start))
        
        if echo "$result" | grep -q '"success"'; then
            record_success $latency
        else
            record_failure
        fi
    done
}

# Client worker for GET stress test
get_worker() {
    local client_id=$1
    local end_time=$2
    
    while [ $(get_timestamp_ms) -lt $end_time ]; do
        local key="${KEY_PREFIX}:get:prepop:$((RANDOM % 100))"
        
        local req_start=$(get_timestamp_ms)
        local result=$(grpcurl -plaintext -max-time 5 \
            -d "{\"key\":\"${key}\"}" \
            "${GRPC_HOST}:${GRPC_PORT}" kv.KVService/Get 2>&1)
        local req_end=$(get_timestamp_ms)
        local latency=$((req_end - req_start))
        
        if echo "$result" | grep -q '"found": *true'; then
            record_success $latency
        else
            record_failure
        fi
    done
}

# Client worker for Batch PUT stress test
batch_put_worker() {
    local client_id=$1
    local end_time=$2
    local batch_size=$3

    while [ $(get_timestamp_ms) -lt $end_time ]; do
        local pairs="["
        for ((j=0; j<batch_size; j++)); do
            local key="${KEY_PREFIX}:batch:client${client_id}:key$((RANDOM % 10000)):item${j}"
            local value=$(generate_value $REQUEST_SIZE)
            if [ $j -gt 0 ]; then
                pairs+=","
            fi
            pairs+="{\"key\":\"${key}\",\"value\":\"${value}\"}"
        done
        pairs+="]"

        local req_start=$(get_timestamp_ms)
        local result=$(grpcurl -plaintext -max-time 10 \
            -d "{\"pairs\":${pairs}}" \
            "${GRPC_HOST}:${GRPC_PORT}" kv.KVService/BatchPut 2>&1)
        local req_end=$(get_timestamp_ms)
        local latency=$((req_end - req_start))

        if echo "$result" | grep -q '"success"'; then
            record_success $latency
        else
            record_failure
        fi
    done
}

# Client worker for Batch GET stress test
batch_get_worker() {
    local client_id=$1
    local end_time=$2
    local batch_size=$3

    while [ $(get_timestamp_ms) -lt $end_time ]; do
        local batch_idx=$((RANDOM % 10))
        local keys="["
        for ((j=0; j<batch_size; j++)); do
            if [ $j -gt 0 ]; then
                keys+=","
            fi
            keys+="\"${KEY_PREFIX}:batchget:prepop:batch${batch_idx}:item${j}\""
        done
        keys+="]"

        local req_start=$(get_timestamp_ms)
        local result=$(grpcurl -plaintext -max-time 10 \
            -d "{\"keys\":${keys}}" \
            "${GRPC_HOST}:${GRPC_PORT}" kv.KVService/BatchGet 2>&1)
        local req_end=$(get_timestamp_ms)
        local latency=$((req_end - req_start))

        if echo "$result" | grep -q '"values"'; then
            record_success $latency
        else
            record_failure
        fi
    done
}

# Client worker for DELETE stress test
delete_worker() {
    local client_id=$1
    local end_time=$2

    while [ $(get_timestamp_ms) -lt $end_time ]; do
        # First create a key to delete
        local key="${KEY_PREFIX}:del:client${client_id}:key$RANDOM"
        local value=$(generate_value $REQUEST_SIZE)
        grpcurl -plaintext -max-time 5 \
            -d "{\"key\":\"${key}\", \"value\":\"${value}\"}" \
            "${GRPC_HOST}:${GRPC_PORT}" kv.KVService/Put > /dev/null 2>&1 || true

        # Now delete it
        local req_start=$(get_timestamp_ms)
        local result=$(grpcurl -plaintext -max-time 5 \
            -d "{\"key\":\"${key}\"}" \
            "${GRPC_HOST}:${GRPC_PORT}" kv.KVService/Delete 2>&1)
        local req_end=$(get_timestamp_ms)
        local latency=$((req_end - req_start))

        if echo "$result" | grep -q '"success"'; then
            record_success $latency
        else
            record_failure
        fi
    done
}

# Client worker for mixed workload
mixed_worker() {
    local client_id=$1
    local end_time=$2

    while [ $(get_timestamp_ms) -lt $end_time ]; do
        local op=$((RANDOM % 100))
        local key="${KEY_PREFIX}:mixed:client${client_id}:key$((RANDOM % 10000))"
        local value=$(generate_value $REQUEST_SIZE)
        local req_start req_end latency result

        if [ $op -lt 40 ]; then
            # 40% PUT
            req_start=$(get_timestamp_ms)
            result=$(grpcurl -plaintext -max-time 5 \
                -d "{\"key\":\"${key}\", \"value\":\"${value}\"}" \
                "${GRPC_HOST}:${GRPC_PORT}" kv.KVService/Put 2>&1) || true
            req_end=$(get_timestamp_ms)
        elif [ $op -lt 70 ]; then
            # 30% GET
            req_start=$(get_timestamp_ms)
            result=$(grpcurl -plaintext -max-time 5 \
                -d "{\"key\":\"${KEY_PREFIX}:mixed:client${client_id}:key$((RANDOM % 100))\"}" \
                "${GRPC_HOST}:${GRPC_PORT}" kv.KVService/Get 2>&1) || true
            req_end=$(get_timestamp_ms)
        elif [ $op -lt 85 ]; then
            # 15% Batch PUT
            local pairs="[{\"key\":\"${key}\",\"value\":\"${value}\"},{\"key\":\"${key}:2\",\"value\":\"${value}\"}]"
            req_start=$(get_timestamp_ms)
            result=$(grpcurl -plaintext -max-time 10 \
                -d "{\"pairs\":${pairs}}" \
                "${GRPC_HOST}:${GRPC_PORT}" kv.KVService/BatchPut 2>&1) || true
            req_end=$(get_timestamp_ms)
        else
            # 15% DELETE
            req_start=$(get_timestamp_ms)
            result=$(grpcurl -plaintext -max-time 5 \
                -d "{\"key\":\"${key}\"}" \
                "${GRPC_HOST}:${GRPC_PORT}" kv.KVService/Delete 2>&1) || true
            req_end=$(get_timestamp_ms)
        fi

        latency=$((req_end - req_start))

        if echo "$result" | grep -q '"success"\|"found"\|"values"'; then
            record_success $latency
        else
            record_failure
        fi
    done
}

# Run stress test with workers
run_stress_test() {
    local test_name=$1
    local worker_func=$2
    local duration=$3
    local clients=$4
    local extra_param=$5
    
    log_info "Starting ${test_name} (duration: ${duration}s, clients: ${clients})..."
    
    init_counters
    
    local start_time=$(get_timestamp_ms)
    local end_time=$((start_time + duration * 1000))
    local pids=()
    
    # Spawn concurrent workers
    for ((i=0; i<clients; i++)); do
        if [ -n "$extra_param" ]; then
            $worker_func $i $end_time $extra_param &
        else
            $worker_func $i $end_time &
        fi
        pids+=($!)
    done
    
    # Wait for all workers
    for pid in "${pids[@]}"; do
        wait $pid
    done
    
    local final_time=$(get_timestamp_ms)
    
    print_statistics "$test_name" "$start_time" "$final_time"
    
    cleanup_counters
}

# Single PUT stress test
stress_put() {
    run_stress_test "PUT Stress Test" put_worker "$1" "$2"
}

# Single GET stress test
stress_get() {
    local duration=$1
    local clients=$2

    log_info "Starting GET Stress Test (duration: ${duration}s, clients: ${clients})..."

    # First, populate some keys
    log_info "Pre-populating 100 keys for GET test..."
    for i in $(seq 1 100); do
        local key="${KEY_PREFIX}:get:prepop:${i}"
        local value=$(generate_value $REQUEST_SIZE)
        grpcurl -plaintext -max-time 5 \
            -d "{\"key\":\"${key}\", \"value\":\"${value}\"}" \
            "${GRPC_HOST}:${GRPC_PORT}" kv.KVService/Put > /dev/null 2>&1 || true
    done
    log_info "Keys pre-populated!"

    run_stress_test "GET Stress Test" get_worker "$duration" "$clients"
}

# Batch PUT stress test
stress_batch_put() {
    run_stress_test "Batch PUT Stress Test" batch_put_worker "$1" "$2" "$3"
}

# Batch GET stress test
stress_batch_get() {
    local duration=$1
    local clients=$2
    local batch_size=$3

    log_info "Starting Batch GET Stress Test (duration: ${duration}s, clients: ${clients}, batch_size: ${batch_size})..."

    # First, populate some keys
    log_info "Pre-populating keys for Batch GET test..."
    for batch in $(seq 1 10); do
        for ((j=0; j<batch_size; j++)); do
            local key="${KEY_PREFIX}:batchget:prepop:batch${batch}:item${j}"
            local value=$(generate_value $REQUEST_SIZE)
            grpcurl -plaintext -max-time 5 \
                -d "{\"key\":\"${key}\", \"value\":\"${value}\"}" \
                "${GRPC_HOST}:${GRPC_PORT}" kv.KVService/Put > /dev/null 2>&1 || true
        done
    done
    log_info "Keys pre-populated!"

    run_stress_test "Batch GET Stress Test" batch_get_worker "$duration" "$clients" "$batch_size"
}

# DELETE stress test
stress_delete() {
    run_stress_test "DELETE Stress Test" delete_worker "$1" "$2"
}

# Mixed workload stress test
stress_mixed() {
    run_stress_test "Mixed Workload Stress Test" mixed_worker "$1" "$2"
}

# Leader election stress test (simulate failover)
stress_leader_election() {
    log_section "Leader Election Stress Test"
    
    log_info "Checking current cluster status..."
    
    # Get status from all nodes
    for port in 8080 8081 8082 8083; do
        log_info "Checking node on port ${port}..."
        local status=$(grpcurl -plaintext -max-time 5 \
            -d '{}' \
            "${GRPC_HOST}:${port}" kv.KVService/Status 2>&1)
        
        if echo "$status" | grep -q '"nodeId"'; then
            local node_id=$(echo "$status" | grep -o '"nodeId": *"[^"]*"' | cut -d'"' -f4)
            local is_leader=$(echo "$status" | grep -o '"isLeader": *[a-z]*' | sed 's/.*: *//')
            log_info "Node ${node_id}: is_leader=${is_leader}"
        else
            log_warn "Node on port ${port} did not respond"
        fi
    done
    
    log_info "Leader election test completed!"
}

# Collect metrics from Prometheus
collect_metrics() {
    log_section "Collecting Prometheus Metrics"
    
    if command -v curl &> /dev/null; then
        local metrics=$(curl -s "http://localhost:${METRICS_PORT}/metrics" 2>/dev/null)
        
        if [ -n "$metrics" ]; then
            log_info "Key Metrics:"
            echo "$metrics" | grep -E "^dekvs_|^raft_|^go_gc_|^process_" | head -30
        else
            log_warn "Could not collect metrics from Prometheus"
        fi
    else
        log_warn "curl not available, skipping metrics collection"
    fi
}

# Cleanup function
cleanup() {
    log_info "Cleaning up..."
    cleanup_counters
    
    # Note: Stress test keys remain in the cluster for inspection
    log_info "Stress test keys with prefix '${KEY_PREFIX}' remain in the cluster"
}

trap cleanup EXIT

# Usage information
usage() {
    cat << EOF
DeKVS Cluster Stress Test

Usage: $0 [OPTIONS] [TEST]

Options:
  -h, --help              Show this help message
  -d, --duration SEC      Duration of each test in seconds (default: ${STRESS_DURATION})
  -c, --clients NUM       Number of concurrent clients (default: ${CONCURRENT_CLIENTS})
  -s, --size BYTES        Size of values in bytes (default: ${REQUEST_SIZE})
  -b, --batch-size NUM    Number of pairs per batch operation (default: ${BATCH_SIZE})
  -p, --prefix PREFIX     Prefix for stress test keys (default: ${KEY_PREFIX})
  --host HOST             gRPC host (default: ${GRPC_HOST})
  --port PORT             gRPC port (default: ${GRPC_PORT})

Tests:
  all                     Run all stress tests (default)
  put                     PUT stress test only
  get                     GET stress test only
  batch-put               Batch PUT stress test only
  batch-get               Batch GET stress test only
  delete                  DELETE stress test only
  mixed                   Mixed workload stress test only
  election                Leader election test only

Examples:
  $0                              # Run all tests with default settings
  $0 -d 120 -c 20                 # Run all tests for 120s with 20 clients
  $0 put -d 60 -c 10              # Run PUT test only
  $0 mixed -d 300 -c 50 -s 2048   # Run mixed workload for 5min with 50 clients

EOF
}

# Parse command line arguments
TEST_TO_RUN="all"

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            usage
            exit 0
            ;;
        -d|--duration)
            STRESS_DURATION="$2"
            shift 2
            ;;
        -c|--clients)
            CONCURRENT_CLIENTS="$2"
            shift 2
            ;;
        -s|--size)
            REQUEST_SIZE="$2"
            shift 2
            ;;
        -b|--batch-size)
            BATCH_SIZE="$2"
            shift 2
            ;;
        -p|--prefix)
            KEY_PREFIX="$2"
            shift 2
            ;;
        --host)
            GRPC_HOST="$2"
            shift 2
            ;;
        --port)
            GRPC_PORT="$2"
            shift 2
            ;;
        put|get|batch-put|batch-get|delete|mixed|election|all)
            TEST_TO_RUN="$1"
            shift
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Main execution
main() {
    log_section "DeKVS Cluster Stress Test"
    
    echo -e "Configuration:"
    echo -e "  Duration: ${STRESS_DURATION}s"
    echo -e "  Concurrent Clients: ${CONCURRENT_CLIENTS}"
    echo -e "  Request Size: ${REQUEST_SIZE} bytes"
    echo -e "  Batch Size: ${BATCH_SIZE}"
    echo -e "  Key Prefix: ${KEY_PREFIX}"
    echo -e "  Target: ${GRPC_HOST}:${GRPC_PORT}"
    echo -e "  Test: ${TEST_TO_RUN}"
    echo ""
    
    # Wait for cluster to be ready
    wait_for_cluster 30 || exit 1
    
    case $TEST_TO_RUN in
        all)
            stress_put $STRESS_DURATION $CONCURRENT_CLIENTS
            stress_get $STRESS_DURATION $CONCURRENT_CLIENTS
            stress_batch_put $STRESS_DURATION $CONCURRENT_CLIENTS $BATCH_SIZE
            stress_batch_get $STRESS_DURATION $CONCURRENT_CLIENTS $BATCH_SIZE
            stress_delete $STRESS_DURATION $CONCURRENT_CLIENTS
            stress_mixed $STRESS_DURATION $CONCURRENT_CLIENTS
            stress_leader_election
            collect_metrics
            ;;
        put)
            stress_put $STRESS_DURATION $CONCURRENT_CLIENTS
            ;;
        get)
            stress_get $STRESS_DURATION $CONCURRENT_CLIENTS
            ;;
        batch-put)
            stress_batch_put $STRESS_DURATION $CONCURRENT_CLIENTS $BATCH_SIZE
            ;;
        batch-get)
            stress_batch_get $STRESS_DURATION $CONCURRENT_CLIENTS $BATCH_SIZE
            ;;
        delete)
            stress_delete $STRESS_DURATION $CONCURRENT_CLIENTS
            ;;
        mixed)
            stress_mixed $STRESS_DURATION $CONCURRENT_CLIENTS
            ;;
        election)
            stress_leader_election
            collect_metrics
            ;;
        *)
            log_error "Unknown test: $TEST_TO_RUN"
            exit 1
            ;;
    esac
    
    log_section "Stress Test Complete"
    log_info "All requested stress tests have completed!"
}

main "$@"
