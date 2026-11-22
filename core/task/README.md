# Loom Task Mesh (core/task)

Task scheduling and execution primitives that run over the loom VL1 overlay
and rely on LAD for discovery. This package contains the data types, queueing
logic, bidding policy, and gateway orchestration necessary to build a
role-based peer-to-peer work mesh.

## Responsibilities

- **Task modelling** — strongly-typed `Task`, `Bid`, `Assignment`, `Result`,
  and `Progress` structures for gateway↔executor communication.
- **Queueing** — EDF + priority aware `TaskQueue` implementation; the
  gateway applies partition fairness through its `PartitionLimiter` hook.
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
    Partition:  "partition-1",
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

- **Transport (`core/task/transport`)** — `BindGateway` wires a gateway to a
  `GatewayTransport`, which publishes task offers and assignments and streams
  bids, results and progress back to the gateway.
- **LAD Directory (`directory`, gossip in `core/directory/gossip`)** — executors
  update their `ReachRecord` with current capacity hints. Gateways query LAD to
  seed offer topics and determine which nodes should receive invites.
- **Storage (`node/storage`)** — reference large payloads via manifests.
  Tasks can include `Payload` metadata that points to content-addressed blobs;
  executors hydrate data through the storage leech.
- **DB Mesh** — schedule schema migrations or consistency checks by enqueuing
  tasks targeting MeshDB nodes.

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

## Task model

A `Task` carries its `ID`, `Partition`, `Role` (which executors may take it) and `Type`, `Tags` (the first names the handler; with no tags the handler is `<role>.<type>`), an opaque JSON `Payload`, `Artifacts`, an `SLO` (`Latency`, `Priority`, `Reliability`), an `Idempotency` key, its `Attempt` and `Status`, and its timestamps.

- **Queue.** Waiting tasks are ordered by `Priority×w_priority + w_deadline/time-to-deadline + age×w_age`, where the deadline is `CreatedAt + SLO.Latency` (the policy's default latency when the task sets none) and the priority is capped at `PriorityCeiling`. A task already past its deadline has no deadline term; its age keeps raising it. `queue.DefaultQueuePolicy` weights priority 2.0, deadline 1.2 and age 0.8, with a 2 s default latency and a ceiling of 15; equal scores go to the earlier enqueue.
- **Offers and bids.** The gateway publishes a `TaskOffer` (the task, its expiry and an offer token). An executor bids only on a task of a role it serves, while it is under its concurrency limit and holds the task's handler; its `Bid` carries a capacity score and an ETA.
- **Award.** When an offer's bid window (`BidWindow`) closes, the offer's `AwardRule` picks the winner; `bidding.ScoreRule` is the rule unless a job kind supplies its own. `BidPolicy.Evaluate` adds the weighted capacity score (clamped to 0-10), ETA and bid age, less a diversity weight for a recent winner, and the highest score at or above `MinScore` wins. A tie goes to the earlier bid, then the lower node id, and is marked ambiguous; the `AwardDecision` records the winner, the rule and every candidate.
- **Assignment and lease.** The winner receives an `Assignment`: a primary, optional standbys, a fence token and a lease, which renewals extend. A lease that lapses unrenewed is offered again for a takeover until its `TakeoverWindow` ends, and the task then ends reaped; with no takeover window an expired lease is released at once and the task retries as a new attempt. A task waiting longer than `MaxWait` for an award is abandoned. A `Result` carries the assignment's fence.

The gateway's store (`WithStore`; a `MemoryStore` when none is given) holds every admission, award, lease version and outcome, and `Start` rehydrates from it: a waiting task is queued again, a task under an unexpired lease is held to that lease's expiry and not awarded again, and a lapsed lease is offered for takeover or, past its window, reaped.

## Execution flow

1. A producer calls `Gateway.EnqueueAuthorized(ctx, task)`, which admits the task only under a server-established principal with a partition. `Enqueue` is the body-only path: it binds no partition, so an executor started without an independently established principal refuses the task.
2. The gateway offers the task (`Offers()`); executors bid into `BidSink()`.
3. The award goes out on `Assignments()`. The winning executor runs the handler and reports into `ProgressSink()`, `RenewalSink()` and `ResultSink()`.
4. `execution.Executor` is the bridge to the node's synchronous handlers: it bids for the roles it serves and runs each assignment through the handler registry's task path (`ExecuteTask`; a handler with no task path of its own runs its RPC path).

## Reliability Features

- Lease renewal and expiry detection to reclaim stalled tasks.
- Automatic retry with exponential backoff and attempt counters.
- Partition fairness limiter hook (`PartitionLimiter`, set with `WithLimiter`)
  to avoid noisy-neighbour amplification.
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
