# Golang Decentralised Key-Value Storage

A fault-tolerant, distributed key-value storage system built with Go, leveraging HashiCorp Raft for consensus and gRPC for communication.

## Overview

This project implements a highly available key-value store that uses the Raft consensus algorithm to ensure consistency across multiple nodes. It supports dynamic cluster membership with automatic node connection and provides a comprehensive gRPC-based API for interacting with the store.

The system is designed to be resilient against node failures and network partitions, making it suitable for environments requiring high availability and strong consistency.

## Features

- **Distributed Consensus**: Built on top of HashiCorp Raft for reliable leader election and log replication.
- **Automatic Leader Election**: Raft automatically handles leader election when the current leader fails.
- **gRPC API**: Fast and efficient communication between clients and nodes.
- **Automatic Cluster Connection**: Nodes automatically connect to the cluster on startup when configured with leader addresses.
- **Key-Value Operations**: `Put`, `Get`, `Delete` operations.
- **Batch Operations**: `BatchPut`, `BatchGet` for efficient bulk operations.
- **Health & Status Endpoints**: Monitor node health and cluster status.
- **Prometheus Metrics**: Built-in metrics endpoint for monitoring.
- **Docker Support**: Full Docker and docker-compose support for easy deployment and testing.
- **Reflection Support**: gRPC reflection enabled for easy debugging and introspection.
- **Cross-Platform**: Runs on Linux, macOS, and Windows.

---

## Quick Start

### Prerequisites

- Go 1.19+
- `protoc` compiler and `protoc-gen-go` / `protoc-gen-go-grpc` plugins.
- Docker and docker-compose (for containerized deployment)

### Building

```bash
go mod tidy
go build ./cmd/server
```

### Running with Docker Compose (Recommended)

```bash
# Start the full cluster with Prometheus
docker-compose up -d --build

# View logs
docker-compose logs -f

# Stop the cluster
docker-compose down
```

This starts:
- 3 DeKVS nodes (node0, node1, node2)
- Prometheus for metrics collection

### Running Locally

1. **Start the Leader Node**:

```bash
./server --id=node0 \
  --raft-bind-addr=127.0.0.1:9090 \
  --grpc-port=8080 \
  --join=false \
  --enable-metrics=true \
  --metrics-port=19090
```

2. **Start Follower Nodes** (in separate terminals):

```bash
./server --id=node1 \
  --raft-bind-addr=127.0.0.1:9091 \
  --grpc-port=8081 \
  --join=true \
  --leader-addrs=127.0.0.1:8080 \
  --enable-metrics=true \
  --metrics-port=19091

./server --id=node2 \
  --raft-bind-addr=127.0.0.1:9092 \
  --grpc-port=8082 \
  --join=true \
  --leader-addrs=127.0.0.1:8080 \
  --enable-metrics=true \
  --metrics-port=19092
```

**Note**: Wait for the leader node to fully start (about 2-3 seconds) before starting follower nodes.

3. **Perform Operations**:

```bash
# Put a key-value pair
grpcurl -plaintext -d '{"key":"mykey", "value":"bXl2YWx1ZQ=="}' localhost:8080 kv.KVService/Put

# Get a value
grpcurl -plaintext -d '{"key":"mykey"}' localhost:8080 kv.KVService/Get

# Delete a key
grpcurl -plaintext -d '{"key":"mykey"}' localhost:8080 kv.KVService/Delete

# Batch Put
grpcurl -plaintext -d '{"pairs":[{"key":"k1", "value":"djE="},{"key":"k2", "value":"djI="}]}' localhost:8080 kv.KVService/BatchPut

# Batch Get
grpcurl -plaintext -d '{"keys":["k1","k2"]}' localhost:8080 kv.KVService/BatchGet

# Health check
grpcurl -plaintext -d '{}' localhost:8080 kv.KVService/Health

# Node status
grpcurl -plaintext -d '{}' localhost:8080 kv.KVService/Status
```

---

## Architecture

- **Raft Consensus**: Ensures all nodes agree on the order of operations and handles leader election automatically.
- **gRPC**: Handles client-server and inter-node communication.
- **FSM (Finite State Machine)**: Applies commands to the local key-value store.
- **Prometheus Integration**: Exposes metrics for monitoring cluster health and performance.

### Leader Election

When the current leader node fails:
1. Remaining nodes detect the failure through Raft's heartbeat mechanism
2. A follower transitions to candidate state and starts an election
3. Candidates request votes from other nodes
4. Once a candidate receives majority votes, it becomes the new leader
5. The cluster continues operating with the new leader

This process is fully automatic and typically completes within seconds.

---

## Configuration

### Command-line Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--id` | Unique node identifier | `node1` |
| `--raft-bind-addr` | Raft bind address (use 0.0.0.0:port for Docker) | `127.0.0.1:9091` |
| `--raft-adv-addr` | Raft advertise address (for cluster communication) | `` |
| `--grpc-port` | gRPC service port | `8081` |
| `--join` | Join existing cluster | `false` |
| `--leader-addrs` | Comma-separated list of leader gRPC addresses to join | `` |
| `--metrics-port` | Prometheus metrics port | `9090` |
| `--enable-metrics` | Enable Prometheus metrics endpoint | `false` |

### Ports

| Port | Purpose |
|------|---------|
| 8080-8082 | gRPC API (per node) |
| 9090-9092 | Raft consensus protocol (per node) |
| 19090 | Prometheus metrics (internal, per node) |
| 30090 | Prometheus UI (when using docker-compose) |

**Note**: In Docker deployments, metrics ports (19090) are only exposed internally for Prometheus scraping. Access Prometheus UI at `http://localhost:30090`.

### Docker Configuration

For Docker deployments, use:
- `--raft-bind-addr=0.0.0.0:9090` - Bind to all interfaces
- `--raft-adv-addr=<hostname>:9090` - Advertise the container hostname

Example:
```bash
./server --id=node0 \
  --raft-bind-addr=0.0.0.0:9090 \
  --raft-adv-addr=node0:9090 \
  --grpc-port=8080 \
  --join=false
```

---

## gRPC API Reference

### Services

#### KVService

| Method | Request | Response | Description |
|--------|---------|----------|-------------|
| `Put` | `PutRequest` | `PutResponse` | Store a key-value pair |
| `Get` | `GetRequest` | `GetResponse` | Retrieve a value by key |
| `Delete` | `DeleteRequest` | `DeleteResponse` | Delete a key |
| `Join` | `JoinRequest` | `JoinResponse` | Request to join the cluster (leader only) |
| `Health` | `HealthRequest` | `HealthResponse` | Check node health |
| `Status` | `StatusRequest` | `StatusResponse` | Get detailed node status |
| `BatchPut` | `BatchPutRequest` | `BatchPutResponse` | Store multiple key-value pairs |
| `BatchGet` | `BatchGetRequest` | `BatchGetResponse` | Retrieve multiple values |

### Messages

```protobuf
message PutRequest {
    string key = 1;
    bytes value = 2;
}

message PutResponse {
    bool success = 1;
}

message GetRequest {
    string key = 1;
}

message GetResponse {
    bytes value = 1;
    bool found = 2;
}

message DeleteRequest {
    string key = 1;
}

message DeleteResponse {
    bool success = 1;
    bool existed = 2;
}

message JoinRequest {
    string node_id = 1;
    string raft_addr = 2;
}

message JoinResponse {
    bool success = 1;
    string message = 2;
}

message HealthRequest {}

message HealthResponse {
    bool healthy = 1;
    string node_id = 2;
    bool is_leader = 3;
    string leader_id = 4;
}

message StatusRequest {}

message StatusResponse {
    string node_id = 1;
    string raft_addr = 2;
    string grpc_addr = 3;
    bool is_leader = 4;
    string leader_id = 5;
    uint64 last_log_index = 6;
    uint64 last_log_term = 7;
    string state = 8;
    int32 peer_count = 9;
}

message BatchPutRequest {
    repeated KeyValue pairs = 1;
}

message KeyValue {
    string key = 1;
    bytes value = 2;
}

message BatchPutResponse {
    bool success = 1;
    int32 inserted_count = 2;
    repeated string failed_keys = 3;
}

message BatchGetRequest {
    repeated string keys = 1;
}

message BatchGetResponse {
    map<string, bytes> values = 1;
    repeated string not_found_keys = 2;
}
```

---

## Prometheus Metrics

The following metrics are exposed when `--enable-metrics=true`:

| Metric | Type | Description |
|--------|------|-------------|
| `dekvs_put_requests_total` | Counter | Total Put requests (labeled by status) |
| `dekvs_get_requests_total` | Counter | Total Get requests (labeled by status) |
| `dekvs_delete_requests_total` | Counter | Total Delete requests (labeled by status) |
| `dekvs_batch_requests_total` | Counter | Total Batch requests (labeled by type and status) |
| `dekvs_raft_state` | Gauge | Current Raft state per node |
| `dekvs_keys_total` | Gauge | Total number of keys in the store |

Access metrics at `http://localhost:19090/metrics` (or the configured metrics port).

---

## Testing

### Unit Tests

```bash
go test ./...
```

### Integration Tests

```bash
# Run full integration test suite with Docker
./tests/integration_test.sh

# Or manually with docker-compose
docker-compose up -d --build
# Wait for all nodes to be healthy, then run tests
docker-compose down
```

---

## Planned Features

- [x] Prometheus metrics
- [x] Batch operations
- [x] Docker/docker-compose support
- [x] Health and status endpoints
- [x] Delete operation
- [ ] TTL for keys
- [ ] Key versioning
- [ ] Snapshots and restore
- [ ] Caching layer
- [ ] Performance optimization under high load
- [ ] Range queries
- [ ] Watch API for key changes

---

## Troubleshooting

### Node fails to join cluster

1. Ensure the leader node is fully started and healthy
2. Check that `--leader-addrs` points to the leader's gRPC port
3. Verify network connectivity between nodes
4. Check logs: `docker-compose logs <node-name>`

### Raft bind address error

If you see "local bind address is not advertisable":
- For local development: use `--raft-bind-addr=127.0.0.1:9090`
- For Docker: use `--raft-bind-addr=0.0.0.0:9090 --raft-adv-addr=<hostname>:9090`

### Leader election takes too long

- Default election timeout is 1 second; adjust in `internal/raft/raft.go`
- Ensure network latency between nodes is low
- Check for resource constraints (CPU, memory)

### Metrics not showing

1. Ensure `--enable-metrics=true` flag is set
2. Verify the metrics port is accessible
3. Check Prometheus configuration in `prometheus.yml`

---

## License

DeKVS is Apache 2.0 licensed.
