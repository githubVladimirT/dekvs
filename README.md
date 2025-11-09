# Golang DEcentralised Key-Value Storage

## Architecture (approximate):

#### Client → HTTP/gRPC API → Raft Consensus → Store → Replication

### Write data

#### Client → HTTP Put → Raft Leader → Log Replication → Majority Ack → Apply to Store

### Read data

#### Client → HTTP Get → Check Consistency Level → Read from Store → Response

