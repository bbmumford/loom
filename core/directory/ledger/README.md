# LAD Ledger

**Status**: 🔴 **NOT IMPLEMENTED**

---

## Purpose

Append-only log that backs the DirectoryCache, enabling distributed synchronization via gossip protocol.

---

## Planned Components

### 1. Ledger Interface (`ledger.go`)
```go
type Ledger interface {
    Append(ctx, records) (seq uint64, error)
    Read(ctx, fromSeq uint64) ([]Record, error)
    Subscribe(ctx, fromSeq uint64) (<-chan Record, error)
    Head(ctx) (uint64, error)
}
```

### 2. Storage Backend (`storage.go`)
- SQLite-based implementation
- Schema: `lad_records` table with sequence numbers
- Indexes on tenant_id, record_type, node_id

### 3. Append Operations (`append.go`)
- Atomic append with sequence number assignment
- Record validation before append
- Duplicate detection

### 4. Subscription API (`subscribe.go`)
- Real-time subscription to new records
- Poll-based implementation (efficient for low-frequency updates)

---

## Schema

```sql
CREATE TABLE lad_records (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id TEXT NOT NULL,
    record_type TEXT NOT NULL,  -- member|role|reach|handler
    node_id TEXT NOT NULL,
    data BLOB NOT NULL,
    timestamp INTEGER NOT NULL,
    INDEX idx_tenant_type (tenant_id, record_type),
    INDEX idx_node (node_id)
);
```

---

## Implementation Plan

See **Track C Phase 2** in [MESH-RESTRUCTURE-AND-IMPLEMENTATION-PLAN.md](../../docs/MESH-RESTRUCTURE-AND-IMPLEMENTATION-PLAN.md)

**Estimated Effort**: 2-3 weeks

---

## Dependencies

- Existing `directory/types.go` for Record types
- SQLite database (or compatible)
- DirectoryCache integration for materialized view refresh

---

## Testing Requirements

- Unit tests for append/read operations
- Concurrency tests (multi-writer scenarios)
- Performance tests (target: >1000 writes/sec)
- Durability tests (crash recovery)
