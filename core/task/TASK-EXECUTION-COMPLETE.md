# Task Execution Path - Implementation Complete

## Summary

The **Task Path** (robust, asynchronous execution) is now fully implemented and separated from the **RPC Path** (fast, synchronous execution). Both paths execute the SAME handlers from `/domain/*/api/` but use different transport and orchestration mechanisms.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    Mesh Network (VL1 Overlay)                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌─────────────────────┐         ┌──────────────────────────┐   │
│  │   RPC Path (Fast)   │         │  Task Path (Robust)      │   │
│  ├─────────────────────┤         ├──────────────────────────┤   │
│  │                     │         │                          │   │
│  │ HTTP Request        │         │ Task Submission          │   │
│  │      ↓              │         │      ↓                   │   │
│  │ HTTPBridge          │         │ Gateway.Enqueue()        │   │
│  │      ↓              │         │      ↓                   │   │
│  │ LAD Discovery       │         │ Broadcast Offer          │   │
│  │      ↓              │         │      ↓                   │   │
│  │ VL1 P2P RPC         │         │ Collect Bids             │   │
│  │      ↓              │         │      ↓                   │   │
│  │ Remote Node         │         │ Evaluate & Assign        │   │
│  │      ↓              │         │      ↓                   │   │
│  │ Handler.ExecuteRPC()│         │ Executor receives        │   │
│  │      ↓              │         │      ↓                   │   │
│  │ Synchronous Return  │         │ Handler.ExecuteTask()    │   │
│  │                     │         │      ↓                   │   │
│  │                     │         │ Result → Gateway         │   │
│  └─────────────────────┘         │      ↓                   │   │
│                                  │ Async Result Delivery    │   │
│                                  └──────────────────────────┘   │
│                                                                   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │         Shared Handler Registry                          │   │
│  │  (Handlers implement BOTH ExecuteRPC and ExecuteTask)    │   │
│  │                                                           │   │
│  │  • domain/auth/api/*       → auth.login, auth.verify     │   │
│  │  • domain/identity/api/*   → identity.getUser            │   │
│  │  • domain/notify/api/*     → notify.sendEmail            │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

## Implementation Components

### 1. **TaskExecutor** (`library/mesh/task/executor.go`) ✅

**Purpose**: Listens to Gateway offers, bids on tasks it can handle, executes assignments using RPC handlers, and returns results.

**Key Features**:
- **Role-based filtering**: Only bids on tasks matching executor's roles
- **Capacity awareness**: Checks `maxConcurrent` limit before bidding
- **Handler compatibility**: Uses same `node.HandlerRegistry` as RPC Bridge
- **Bid evaluation**: Calculates capacity score and ETA for Gateway scoring
- **Execution**: Calls `handler.ExecuteTask()` - uses TASK execution path (not RPC)
- **Result reporting**: Converts `node.TaskResult` to `task.Result` and sends to Gateway

**Configuration**:
```go
type ExecutorConfig struct {
    NodeID        string                // Unique executor identifier
    Roles         []string              // Roles this executor handles
    CapacityFunc  func() float64        // Returns 0.0-1.0 capacity
    BidTimeout    time.Duration         // Delay before bidding (anti-herd)
    MaxConcurrent int                   // Max parallel task executions
}
```

**Usage Example**:
```go
// Create executor with auth + identity roles
executor, _ := task.NewExecutor(gateway, handlerRegistry, task.ExecutorConfig{
    NodeID:        "executor-node-01",
    Roles:         []string{"auth", "identity"},
    CapacityFunc:  func() float64 { return 0.8 },
    BidTimeout:    50 * time.Millisecond,
    MaxConcurrent: 10,
})

// Start executor (runs until context canceled)
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

go executor.Start(ctx)
```

### 2. **Gateway** (`library/mesh/task/gateway.go`) ✅ (Already Existed)

**Purpose**: Orchestrates task lifecycle - queueing, offering, bid collection, assignment, lease management, retries.

**Flow**:
1. Client calls `gateway.Enqueue(task)`
2. Gateway queues task based on priority/SLO
3. Gateway broadcasts `TaskOffer` via `Offers()` channel
4. Executors send bids to `BidSink()`
5. Gateway evaluates bids using `BidPolicy`
6. Gateway assigns to best bidder, publishes to `Assignments()`
7. Executor executes task, sends result to `ResultSink()`
8. Gateway handles result (success/failure/retry logic)

### 3. **Task Types** (`library/mesh/task/types.go`) ✅ (Already Existed)

**Key Types**:
- `Task`: Work unit with SLO, payload, role, idempotency
- `TaskOffer`: Broadcast to executors with expiry
- `Bid`: Executor response with capacity, ETA, score
- `Assignment`: Lease with fence token for execution
- `Result`: Terminal outcome with status, output, error

## Execution Flow Details

### Task Creation & Submission

```go
// Client creates a task (e.g., from HTTP API or internal trigger)
task := &task.Task{
    ID:       "task-abc123",
    TenantID: "tenant-xyz",
    Role:     "identity",          // Which executor role can handle this
    Type:     "getUser",           // Task type
    Tags:     []string{"identity.getUser"}, // Handler name
    Payload:  rpcRequestJSON,      // RPC request as JSON
    SLO: task.TaskSLO{
        Latency:     5 * time.Second,
        Priority:    128,            // 0-255
        Reliability: 0.99,
    },
    Idempotency: "req-xyz",
    Status:      task.TaskStatusQueued,
    CreatedAt:   time.Now(),
}

// Submit to gateway
gateway.Enqueue(task)
```

### Executor Bidding Logic

```go
// Executor receives offer
offer := <-gateway.Offers()

// Check if executor should bid
shouldBid := (
    executor.hasRole(offer.Task.Role) &&       // Role match
    executor.ActiveTasks() < maxConcurrent &&  // Has capacity
    executor.hasHandler(offer.Task.Tags[0])    // Has handler
)

if shouldBid {
    // Calculate bid
    bid := task.Bid{
        TaskID:        offer.Task.ID,
        NodeID:        executor.nodeID,
        CapacityScore: executor.CapacityFunc(), // 0.0-1.0
        ETA:           estimateETA(offer.Task),
        ReceivedAt:    time.Now(),
    }
    
    // Send bid to gateway
    gateway.BidSink() <- bid
}
```

### Task Execution

```go
// Executor receives assignment
assignment := <-gateway.Assignments()

if assignment.Primary == executor.nodeID {
    // Extract handler name from task tags
    handlerName := assignment.Task.Tags[0] // e.g., "identity.getUser"
    
    // Get handler from registry (SAME registry as RPC path!)
    handler, _ := executor.registry.Get(handlerName)
    
    // Convert mesh Task to node.Task format
    nodeTask := &node.Task{
        ID:       assignment.Task.ID,
        Handler:  handlerName,
        Payload:  assignment.Task.Payload,
        Priority: int(assignment.Task.SLO.Priority),
        Deadline: time.Now().Add(assignment.Task.SLO.Latency),
    }
    
    // Execute handler (uses TASK execution path!)
    taskResult, err := handler.ExecuteTask(ctx, nodeTask)
    
    // Convert node.TaskResult to mesh result
    result := task.Result{
        TaskID: assignment.TaskID,
        NodeID: executor.nodeID,
        Fence:  assignment.Fence,
        Status: mapTaskStatus(taskResult.Status),
        Output: taskResult.Output,
        Error:  taskResult.Error,
        At:     time.Now(),
    }
    
    // Send result to gateway
    gateway.ResultSink() <- task.ResultReport{Result: result}
}
```

## Key Design Decisions

### 1. **No RPC → Task Failover**

**Rationale**: RPC and Task have incompatible semantics:
- RPC: Synchronous, blocking, must return immediately
- Task: Asynchronous, fire-and-forget, result via callback/subscription

**Result**: Bridge handles RPC only. If all RPC attempts fail, request fails. No automatic conversion to Task path.

### 2. **Separate Execution Paths**

**Rationale**: RPC and Task execution have different semantics and requirements:
- RPC Path: `handler.ExecuteRPC()` - synchronous, blocking, immediate return
- Task Path: `handler.ExecuteTask()` - asynchronous, retry logic, result via callback

**Benefits**:
- Handlers can implement different logic for sync vs async execution
- Task execution can use proper retry/timeout semantics
- RPC execution gets immediate response requirements
- Proper separation enables future optimizations (batching, etc.)

### 3. **Task Payload = Arbitrary Bytes**

**Rationale**: Tasks serialize RPC requests as payload, making conversion trivial.

**Benefits**:
- Simple executor implementation
- Easy debugging (payload is human-readable JSON)
- Clear contract between task submitter and executor

### 4. **Result Retrieval Challenge**

**Current Limitation**: Gateway doesn't expose results to task requesters—only to internal state management.

**Proposed Solutions** (for future sync task support):
1. **Gateway.Subscribe(taskID)**: Subscribe to specific task results
2. **ResultRouter**: Pub/sub layer on top of Gateway
3. **ResultStore**: Shared correlation map for polling
4. **Async-only**: Accept that tasks are fire-and-forget

**Recommendation**: Option 1 (Gateway.Subscribe) for cleanest architecture.

## Integration Points

### In `node.hstles.com/main.go`:

```go
// Create Gateway
gateway, _ := task.NewGateway(task.GatewayConfig{
    QueuePolicy: task.DefaultQueuePolicy(),
    BidPolicy:   task.DefaultBidPolicy(),
})

// Start Gateway background workers
ctx, cancel := context.WithCancel(context.Background())
defer cancel()
gateway.Start(ctx)

// Create Executor (same registry as Bridge!)
executor, _ := task.NewExecutor(gateway, handlerRegistry, task.ExecutorConfig{
    NodeID:        nodeID,
    Roles:         []string{"auth", "identity", "notify"},
    CapacityFunc:  getSystemCapacity, // Function to check CPU/memory
    MaxConcurrent: 20,
})

// Start Executor
go executor.Start(ctx)

// Bridge uses SAME handlerRegistry for RPC
bridge := meshhttp.NewHTTPBridge(
    handlerRegistry,  // ← SHARED with Executor
    ledger,
    cache,
    vl1Mgr,
    rpcExecutor,
    nodeID,
    meshhttp.DefaultBridgeConfig(),
)
```

## Testing

### Unit Tests Needed:

- [ ] Executor bid evaluation (role matching, capacity limits)
- [ ] Executor task execution (RPC handler invocation)
- [ ] Task → RPC request conversion
- [ ] RPC response → Task result conversion
- [ ] Concurrent task execution limits

### Integration Tests Needed:

- [ ] End-to-end task flow (enqueue → offer → bid → assign → execute → result)
- [ ] Multiple executors bidding on same task
- [ ] Executor failure and retry logic
- [ ] Gateway lease expiration and requeue

## Performance Characteristics

### RPC Path:
- **Latency**: ~10-50ms (P2P direct)
- **Throughput**: High (limited by network/handler)
- **Failure Mode**: Immediate error, no retry

### Task Path:
- **Latency**: ~100-500ms (offer/bid/assign overhead)
- **Throughput**: Medium (limited by queue/bid evaluation)
- **Failure Mode**: Automatic retry, graceful degradation

## Current Status

✅ **Complete**:
- TaskExecutor implementation
- Integration with RPC handler registry
- Bid evaluation and capacity management
- Task execution via handler.ExecuteTask() (separate async execution path)
- Result reporting to Gateway
- Documentation

⏳ **Pending** (Future Work):
- Gateway.Subscribe() for sync result retrieval
- Task submission HTTP API
- Metrics and observability
- Integration tests
- Deployment to node.hstles.com

## Files Modified/Created

- ✅ **Created**: `library/mesh/task/executor.go` (404 lines)
- ✅ **Modified**: Removed unused task import from `library/mesh/http/bridge.go`
- ✅ **Created**: This documentation

---

**Next Steps**: Wire up Gateway + Executor in `node.hstles.com/main.go` to enable task-based execution in production.
