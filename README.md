# Golang Decentralised Key-Value Storage

A fault-tolerant, distributed key-value storage system built with Go, leveraging HashiCorp Raft for consensus and gRPC for communication.

## Overview

This project implements a highly available key-value store that uses the Raft consensus algorithm to ensure consistency across multiple nodes. It supports dynamic cluster membership, automatic failure detection, and provides a simple gRPC-based API for interacting with the store.

The system is designed to be resilient against node failures and network partitions, making it suitable for environments requiring high availability and strong consistency.

## Features

- **Distributed Consensus**: Built on top of HashiCorp Raft for reliable leader election and log replication.
- **gRPC API**: Fast and efficient communication between clients and nodes.
- **Dynamic Membership**: Add or remove nodes from the cluster without downtime.
- **Automatic Failure Detection**: Nodes that become unreachable are automatically detected and removed from the cluster.
- **Key-Value Operations**: Basic `Put` and `Get` operations.
- **Leader Redirection**: Clients can connect to any node, which will redirect writes to the current leader. (TODO)
- **Reflection Support**: gRPC reflection enabled for easy debugging and introspection.
- **Cross-Platform**: Runs on Linux, macOS, and Windows.

---

## Quick Start

### Prerequisites

- Go 1.19+
- `protoc` compiler and `protoc-gen-go` / `protoc-gen-go-grpc` plugins.

### Building
```bash
go mod tidy
go build ./cmd/server
```

### Running a Cluster

1. **Start the Leader Node**:

```bash
./server --id=node0 --raft-addr=127.0.0.1:9090 --grpc-port=8080 --join=false
```

2. **Start Follower Nodes** (in separate terminals):

```bash
./server --id=node1 --raft-addr=127.0.0.1:9091 --grpc-port=8081 --join=true
./server --id=node2 --raft-addr=127.0.0.1:9092 --grpc-port=8082 --join=true
```

3. **Add Followers to the Cluster** (from leader's gRPC endpoint):

```bash
grpcurl -plaintext -d '{"id":"node1","raft_addr":"127.0.0.1:9091","grpc_addr":"127.0.0.1:8081"}' localhost:8080 kv.KVService/AddPeer
grpcurl -plaintext -d '{"id":"node2","raft_addr":"127.0.0.1:9092","grpc_addr":"127.0.0.1:8082"}' localhost:8080 kv.KVService/AddPeer
```

4. **Perform Operations**:

```bash
# Put a key-value pair
grpcurl -plaintext -d '{"key":"mykey", "value":"bXl2YWx1ZQ=="}' localhost:8080 kv.KVService/Put

# Get a value
grpcurl -plaintext -d '{"key":"mykey"}' localhost:8080 kv.KVService/Get
```

---

## Architecture

- **Raft Consensus**: Ensures all nodes agree on the order of operations.
- **gRPC**: Handles client-server and inter-node communication.
- **FSM (Finite State Machine)**: Applies commands to the local key-value store.
- **Failure Detector**: Monitors node health and removes dead nodes.

---

## Planned Features

- [ ] TTL for keys
- [ ] Key versioning
- [ ] Snapshots and restore
- [ ] Batch operations
- [ ] Prometheus metrics
- [ ] Caching layer
- [ ] Performance optimization under high load

---

## License

DeKVS is Apache 2.0 licensed.
