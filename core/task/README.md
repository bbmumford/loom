# Orbtr Task Mesh (mesh/task)

Task scheduling and execution primitives that run over the Orbtr VL1 overlay
and rely on LAD for discovery. This package contains the data types, queueing
logic, bidding policy, and gateway orchestration necessary to build a
role-based peer-to-peer work mesh.

## Responsibilities

- **Task modelling** — strongly-typed `Task`, `Bid`, `Assignment`, `Result`,
  and `Progress` structures for gateway↔executor communication.
- **Queueing** — EDF + priority aware `TaskQueue` implementation with
  tenant fairness hooks.
- **Gateway** — orchestrates offers, collects bids, issues leases, tracks
  in-flight assignments, and handles retries or expirations.
- **Bidding policy** — scoring helpers to evaluate executor capacity snapshots
  and ETA predictions.
- **Resource scoring** — utilities in `resources.go` to turn live CPU/RAM/disk
  metrics into normalized capacity scores.

## Quick Start

```go
gateway, _ := task.NewGateway(task.GatewayConfig{
    QueuePolicy: task.DefaultQueuePolicy(),
    BidPolicy:   task.DefaultBidPolicy(),
})

ctx, cancel := context.WithCancel(context.Background())
defer cancel()
gateway.Start(ctx)

_ = gateway.Enqueue(&task.Task{
    ID:        "t-123",
    TenantID:  "tenant-1",
    Role:      "identity",
    Type:      "verify",
    SLO:       task.TaskSLO{Latency: 500 * time.Millisecond, Priority: 5},
    CreatedAt: time.Now(),
})

go func() {
    for offer := range gateway.Offers() {
        // Publish offer over VL1 datagrams or a domain-specific channel.
        _ = offer
    }
}()

go func() {
    for assignment := range gateway.Assignments() {
        // Send assignment to the winning executor via VL1 stream.
        _ = assignment
    }
}()
```

Executors feed bids into `gateway.BidSink()` and report results through
`gateway.ResultSink()`.

## Integration Points

- **VL1 Transport (`mesh/transport`)** — publish task offers via VL1 datagrams and
  deliver assignments/results over Noise streams. Use `vl1.Manager.DialRole`
  to locate executors that have advertised the required role in LAD.
- **LAD Directory (`mesh/directory`)** — executors update their `ReachRecord`
  with current capacity hints. Gateways query LAD to seed offer topics and
  determine which nodes should receive invites.
- **Storage (`mesh/storage`)** — reference large payloads via manifests.
  Tasks can include `Payload` metadata that points to content-addressed blobs;
  executors hydrate data through the storage leech.
- **DB Mesh (`mesh/control`)** — schedule schema migrations or consistency
  checks by enqueuing tasks targeting MeshDB nodes.

## Executors & Bids

Executors compute a resource-based score before bidding:

```go
snapshot := task.ResourceSnapshot{
    CPUFree:       cpuFree,
    RAMPressure:   ramPressure,
    DiskFree:      diskFree,
    NetCongestion: netUsage,
    QueueDepth:    float64(activeTasks),
    QueueCapacity: float64(maxParallel),
    AvgLatency:    recentLatency,
    LatencySLO:    taskLatencyTarget,
}

raw := task.ComputeCapacityScore(snapshot, task.DefaultResourceWeights())
capacity := task.NormalizeCapacityScore(raw, 10)

bid := task.Bid{
    TaskID:        offer.Task.ID,
    NodeID:        selfNodeID,
    ETA:           predictedDuration,
    CapacityScore: capacity,
    ReceivedAt:    time.Now(),
}
```

The gateway clamps the capacity score and mixes it with ETA, freshness, and
diversity weights when selecting a winner.

## Reliability Features

- Lease renewal and expiry detection to reclaim stalled tasks.
- Automatic retry with exponential backoff and attempt counters.
- Tenant fairness limiter hook to avoid noisy-neighbour amplification.
- Progress channel for streaming execution telemetry.

## Observability

Integrate with your metrics/tracing stack by wrapping the queue and gateway:

- Count offers, bids, assignments, retries, and completions per role.
- Export wait time percentiles from the queue score calculations.
- Attach tracing metadata (`traceparent`) to `Task` or `Bid` metadata fields.

## Next Steps

- Bind the gateway to a `vl1.Manager` to send/receive wire formats.
- Register domain handlers using `task.Registry` (see `domain.go`) so logic can
  be invoked directly when assignments arrive.
- Feed LAD reach updates with executor health to improve bid scoring.
