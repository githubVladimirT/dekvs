# DeKVS Prometheus Query Examples

## Basic Queries

### Current number of keys across all nodes
```promql
dekvs_keys_total
```

### Total Put requests per node
```promql
rate(dekvs_put_requests_total[5m])
```

### Total Get requests per node (success vs not_found)
```promql
rate(dekvs_get_requests_total[5m])
```

### Total Delete requests per node
```promql
rate(dekvs_delete_requests_total[5m])
```

### Batch operations rate
```promql
rate(dekvs_batch_requests_total[5m])
```

## Advanced Queries

### Keys distribution across cluster (should be same on all nodes due to Raft)
```promql
dekvs_keys_total by (instance)
```

### Request success rate (Put operations)
```promql
sum(rate(dekvs_put_requests_total{status="success"}[5m])) 
/ 
sum(rate(dekvs_put_requests_total[5m])) * 100
```

### Get hit rate (found vs not_found)
```promql
sum(rate(dekvs_get_requests_total{status="found"}[5m])) 
/ 
sum(rate(dekvs_get_requests_total[5m])) * 100
```

### Total requests per second across cluster
```promql
sum(rate(dekvs_put_requests_total[5m])) + 
sum(rate(dekvs_get_requests_total[5m])) + 
sum(rate(dekvs_delete_requests_total[5m])) + 
sum(rate(dekvs_batch_requests_total[5m]))
```

### Raft state by node (0=Follower, 1=Candidate, 2=Leader, 3=Shutdown)
```promql
dekvs_raft_state by (node_id, instance)
```

### Identify current leader
```promql
dekvs_raft_state == 2
```

## Alerting Rules Examples

### Alert if a node is not healthy
```yaml
- alert: DeKVSNodeDown
  expr: up{job="dekvs"} == 0
  for: 1m
  labels:
    severity: critical
  annotations:
    summary: "DeKVS node {{ $labels.instance }} is down"
    description: "Node {{ $labels.instance }} has been unreachable for more than 1 minute."
```

### Alert if no leader is present
```yaml
- alert: DeKVSNoLeader
  expr: sum(dekvs_raft_state == 2) == 0
  for: 30s
  labels:
    severity: critical
  annotations:
    summary: "DeKVS cluster has no leader"
    description: "No leader node detected in the cluster for more than 30 seconds."
```

### Alert if keys count differs between nodes (data inconsistency)
```yaml
- alert: DeKVSDataInconsistency
  expr: max(dekvs_keys_total) - min(dekvs_keys_total) > 0
  for: 2m
  labels:
    severity: warning
  annotations:
    summary: "DeKVS data inconsistency detected"
    description: "Key count differs between nodes. Max: {{ $value }}"
```

### Alert on high error rate
```yaml
- alert: DeKVSHighErrorRate
  expr: |
    sum(rate(dekvs_put_requests_total{status="error"}[5m])) 
    / 
    sum(rate(dekvs_put_requests_total[5m])) > 0.1
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "High error rate on DeKVS Put operations"
    description: "Error rate is {{ $value | humanizePercentage }}"
```

## Grafana Dashboard Panels

### Panel 1: Keys Count Over Time
```promql
dekvs_keys_total
```
- Type: Time series
- Legend: {{instance}}

### Panel 2: Requests Per Second
```promql
sum(rate(dekvs_put_requests_total[1m]))
sum(rate(dekvs_get_requests_total[1m]))
sum(rate(dekvs_delete_requests_total[1m]))
```
- Type: Time series
- Legend: Put, Get, Delete

### Panel 3: Cluster Health
```promql
dekvs_raft_state
```
- Type: Stat
- Value mappings: 0=Follower, 1=Candidate, 2=Leader, 3=Shutdown

### Panel 4: Error Rate
```promql
sum(rate(dekvs_put_requests_total{status="error"}[5m])) 
/ 
sum(rate(dekvs_put_requests_total[5m])) * 100
```
- Type: Gauge
- Unit: Percent
