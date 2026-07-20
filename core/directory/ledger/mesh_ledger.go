/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/bbmumford/envreq"
	"github.com/bbmumford/tursoraft/manager"

	lad "github.com/bbmumford/ledger"
	"github.com/bbmumford/loom/core/internal/bloom"
)

// MeshLedger implements the Ledger interface using Tursoraft for distributed
// Raft-replicated storage. This is used for LAD peer discovery records that need
// to be synchronized across all mesh nodes via Raft consensus.
//
// Unlike LocalDBLedger (single-node persistence) or MemoryLedger (ephemeral),
// MeshLedger provides:
//   - Strong consistency via Raft consensus
//   - Automatic failover when leader changes
//   - Cross-node replication of peer discovery data
//   - Crash recovery from Raft log and snapshots
//
// Use cases:
//   - Multi-node mesh deployments requiring consistent peer directory
//   - Production deployments where LAD state must survive node failures
//   - Scenarios requiring audit trail of peer discovery changes
type MeshLedger struct {
	mgr         *manager.Manager
	db          *sql.DB // database handle from tursoraft
	groupName   string
	dbName      string
	mu          sync.RWMutex
	subscribers map[int]*subscriber
	nextSubID   int

	// appendMu serialises the auto-seq assignment + insert (MESH-F06). Without
	// it, two concurrent same-topic Appends both read the same Head and compute
	// the same Seq, colliding on PRIMARY KEY (topic, seq) so one INSERT fails.
	appendMu sync.Mutex
	stopChan    chan struct{}
	stopOnce    sync.Once

	// Compaction configuration
	retentionDays int
	compactTicker *time.Ticker

	// Bloom filter manager for efficient queries
	bloomMgr    *bloom.BloomFilterManager
	bloomTicker *time.Ticker
}

// MeshLedgerConfig holds configuration for MeshLedger
type MeshLedgerConfig struct {
	// Tursoraft manager configuration
	ManagerConfig manager.ManagerConfig

	// Group and database names for LAD storage
	// LAD records are stored in: <GroupName>.<DBName>
	GroupName string
	DBName    string

	// Peers is the list of Raft peer addresses for mesh replication.
	// If empty, falls back to MESHDB_LAD_PEERS environment variable via envreq.
	// Format: comma-separated addresses like "node1:7000,node2:7000"
	Peers []string

	// Retention and compaction settings (same as LocalDBLedger)
	RetentionDays      int               // Keep records for this many days (0 = keep forever)
	CompactionInterval time.Duration     // How often to run compaction (0 = disabled)
	EnableBloomFilters bool              // Enable Bloom filters for query optimization
	BloomConfig        bloom.BloomConfig // Bloom filter configuration
}

// DefaultMeshLedgerConfig returns sensible defaults for mesh LAD storage.
// You must still set the ManagerConfig with appropriate Raft settings.
func DefaultMeshLedgerConfig() MeshLedgerConfig {
	return MeshLedgerConfig{
		GroupName:          "mesh",
		DBName:             "lad",
		RetentionDays:      30,             // Keep 30 days of history
		CompactionInterval: 24 * time.Hour, // Compact daily
		EnableBloomFilters: true,           // Enable Bloom filters
		BloomConfig:        bloom.DefaultBloomConfig(),
	}
}

// NewMeshLedger creates a distributed ledger backed by Tursoraft.
// The tursoraft manager handles Raft consensus and database replication.
//
// The schema is automatically initialized (same as LocalDBLedger):
//
//	CREATE TABLE IF NOT EXISTS lad_records (
//	    topic TEXT NOT NULL,
//	    seq INTEGER NOT NULL,
//	    tenant_id TEXT NOT NULL,
//	    node_id TEXT NOT NULL,
//	    body TEXT NOT NULL,
//	    signature BLOB,
//	    timestamp TEXT NOT NULL,
//	    PRIMARY KEY (topic, seq)
//	);
func NewMeshLedger(ctx context.Context, cfg MeshLedgerConfig) (*MeshLedger, error) {
	dbgMesh.Printf("Creating mesh ledger (group=%s, db=%s)", cfg.GroupName, cfg.DBName)

	// Validate configuration
	if cfg.GroupName == "" {
		return nil, fmt.Errorf("lad: mesh ledger requires group name")
	}
	if cfg.DBName == "" {
		return nil, fmt.Errorf("lad: mesh ledger requires database name")
	}

	// Resolve peers using envreq pattern:
	// 1. Use cfg.Peers if provided directly
	// 2. Fall back to MESHDB_LAD_PEERS env var via envreq
	peers := cfg.Peers
	if len(peers) == 0 {
		peersResult := envreq.Check(envreq.Requirement{
			Name:        "MESHDB_LAD_PEERS",
			Source:      "ledger.mesh",
			Description: "Comma-separated list of Raft peer addresses for mesh LAD replication",
			Optional:    true,
			Default:     "",
		})
		if peersResult.Value != "" {
			peers = strings.Split(peersResult.Value, ",")
			for i := range peers {
				peers[i] = strings.TrimSpace(peers[i])
			}
		}
	}

	// Set env var and PeerEnvVar for tursoraft (tursoraft reads peers from this env var)
	const peerEnvVar = "MESHDB_LAD_PEERS"
	if len(peers) > 0 {
		os.Setenv(peerEnvVar, strings.Join(peers, ","))
		dbgMesh.Printf("Resolved %d peers for mesh LAD: %v", len(peers), peers)
	}
	cfg.ManagerConfig.PeerEnvVar = peerEnvVar

	// Create tursoraft manager
	mgr, err := manager.NewManager(ctx, cfg.ManagerConfig)
	if err != nil {
		return nil, fmt.Errorf("lad: create tursoraft manager: %w", err)
	}

	// Get database handle for LAD storage
	db, err := mgr.Database(cfg.GroupName, cfg.DBName)
	if err != nil {
		mgr.Shutdown()
		return nil, fmt.Errorf("lad: get database handle: %w", err)
	}

	// Initialize schema
	schema := `
		CREATE TABLE IF NOT EXISTS lad_records (
			topic TEXT NOT NULL,
			seq INTEGER NOT NULL,
			tenant_id TEXT NOT NULL,
			node_id TEXT NOT NULL,
			body TEXT NOT NULL,
			signature BLOB,
			timestamp TEXT NOT NULL,
			PRIMARY KEY (topic, seq)
		);
		CREATE INDEX IF NOT EXISTS idx_lad_tenant ON lad_records(tenant_id, topic, seq);
		CREATE INDEX IF NOT EXISTS idx_lad_node ON lad_records(node_id, topic, seq);
		CREATE INDEX IF NOT EXISTS idx_lad_timestamp ON lad_records(timestamp DESC);
		
		-- Bloom filter table for efficient existence checks
		CREATE TABLE IF NOT EXISTS lad_bloom_filters (
			scope TEXT NOT NULL,
			filter_data BLOB NOT NULL,
			updated_at TEXT NOT NULL,
			PRIMARY KEY (scope)
		);
	`

	if _, err := db.ExecContext(ctx, schema); err != nil {
		mgr.Shutdown()
		return nil, fmt.Errorf("lad: failed to initialize schema: %w", err)
	}

	dbgMesh.Printf("Mesh ledger schema initialized")

	ledger := &MeshLedger{
		mgr:           mgr,
		db:            db,
		groupName:     cfg.GroupName,
		dbName:        cfg.DBName,
		subscribers:   make(map[int]*subscriber),
		stopChan:      make(chan struct{}),
		retentionDays: cfg.RetentionDays,
	}

	// Initialize Bloom filter manager if enabled
	if cfg.EnableBloomFilters {
		ledger.bloomMgr = bloom.NewBloomFilterManager(cfg.BloomConfig)

		// Load existing Bloom filters from database
		if err := ledger.loadBloomFilters(); err != nil {
			// Log error but don't fail initialization
			// Bloom filters will be rebuilt on first rebuild cycle
			dbgMesh.Printf("Failed to load bloom filters (will rebuild): %v", err)
		}

		// Start periodic rebuild if configured
		if cfg.BloomConfig.RebuildInterval > 0 {
			ledger.bloomTicker = time.NewTicker(cfg.BloomConfig.RebuildInterval)
			go ledger.runBloomRebuild()
		}
	}

	// Start background compaction if configured
	if cfg.CompactionInterval > 0 && cfg.RetentionDays > 0 {
		ledger.compactTicker = time.NewTicker(cfg.CompactionInterval)
		go ledger.runCompaction()
	}

	log.Printf("[LAD] Mesh ledger initialized (group=%s, db=%s, retention=%dd)",
		cfg.GroupName, cfg.DBName, cfg.RetentionDays)

	return ledger, nil
}

// NewMeshLedgerWithManager creates a distributed ledger using an existing tursoraft manager.
// Use this when you want to share a manager across multiple components.
func NewMeshLedgerWithManager(ctx context.Context, mgr *manager.Manager, cfg MeshLedgerConfig) (*MeshLedger, error) {
	dbgMesh.Printf("Creating mesh ledger with existing manager (group=%s, db=%s)", cfg.GroupName, cfg.DBName)

	if mgr == nil {
		return nil, fmt.Errorf("lad: tursoraft manager required")
	}
	if cfg.GroupName == "" {
		return nil, fmt.Errorf("lad: mesh ledger requires group name")
	}
	if cfg.DBName == "" {
		return nil, fmt.Errorf("lad: mesh ledger requires database name")
	}

	// Get database handle for LAD storage
	db, err := mgr.Database(cfg.GroupName, cfg.DBName)
	if err != nil {
		return nil, fmt.Errorf("lad: get database handle: %w", err)
	}

	// Initialize schema
	schema := `
		CREATE TABLE IF NOT EXISTS lad_records (
			topic TEXT NOT NULL,
			seq INTEGER NOT NULL,
			tenant_id TEXT NOT NULL,
			node_id TEXT NOT NULL,
			body TEXT NOT NULL,
			signature BLOB,
			timestamp TEXT NOT NULL,
			PRIMARY KEY (topic, seq)
		);
		CREATE INDEX IF NOT EXISTS idx_lad_tenant ON lad_records(tenant_id, topic, seq);
		CREATE INDEX IF NOT EXISTS idx_lad_node ON lad_records(node_id, topic, seq);
		CREATE INDEX IF NOT EXISTS idx_lad_timestamp ON lad_records(timestamp DESC);
		
		-- Bloom filter table for efficient existence checks
		CREATE TABLE IF NOT EXISTS lad_bloom_filters (
			scope TEXT NOT NULL,
			filter_data BLOB NOT NULL,
			updated_at TEXT NOT NULL,
			PRIMARY KEY (scope)
		);
	`

	if _, err := db.ExecContext(ctx, schema); err != nil {
		return nil, fmt.Errorf("lad: failed to initialize schema: %w", err)
	}

	dbgMesh.Printf("Mesh ledger schema initialized (existing manager)")

	ledger := &MeshLedger{
		mgr:           nil, // Don't own the manager, don't shut it down
		db:            db,
		groupName:     cfg.GroupName,
		dbName:        cfg.DBName,
		subscribers:   make(map[int]*subscriber),
		stopChan:      make(chan struct{}),
		retentionDays: cfg.RetentionDays,
	}

	// Initialize Bloom filter manager if enabled
	if cfg.EnableBloomFilters {
		ledger.bloomMgr = bloom.NewBloomFilterManager(cfg.BloomConfig)

		// Load existing Bloom filters from database
		if err := ledger.loadBloomFilters(); err != nil {
			dbgMesh.Printf("Failed to load bloom filters (will rebuild): %v", err)
		}

		// Start periodic rebuild if configured
		if cfg.BloomConfig.RebuildInterval > 0 {
			ledger.bloomTicker = time.NewTicker(cfg.BloomConfig.RebuildInterval)
			go ledger.runBloomRebuild()
		}
	}

	// Start background compaction if configured
	if cfg.CompactionInterval > 0 && cfg.RetentionDays > 0 {
		ledger.compactTicker = time.NewTicker(cfg.CompactionInterval)
		go ledger.runCompaction()
	}

	log.Printf("[LAD] Mesh ledger initialized with existing manager (group=%s, db=%s)",
		cfg.GroupName, cfg.DBName)

	return ledger, nil
}

// Close stops background goroutines and shuts down the tursoraft manager if owned.
func (l *MeshLedger) Close() error {
	dbgMesh.Printf("Closing mesh ledger")

	l.stopOnce.Do(func() {
		close(l.stopChan)

		if l.compactTicker != nil {
			l.compactTicker.Stop()
		}

		if l.bloomTicker != nil {
			l.bloomTicker.Stop()
		}

		// Only shutdown manager if we own it (created via NewMeshLedger)
		if l.mgr != nil {
			l.mgr.Shutdown()
		}
	})
	return nil
}

// ledgerOpTimeout caps the time any single SQL-backed ledger operation
// can block. When Turso is slow or temporarily unreachable, this prevents
// publish-path goroutines (publishPeerLatencies, gossip Apply, hot
// Append loops) from hanging indefinitely and starving downstream
// callers — the HTTP listener, the connection manager, the topology
// monitor. Callers can pass a tighter ctx to override; the effective
// deadline is min(caller-deadline, ledgerOpTimeout).
const ledgerOpTimeout = 5 * time.Second

// Head returns the latest sequence per topic.
func (l *MeshLedger) Head(ctx context.Context) (lad.CausalWatermark, error) {
	dbgMesh.Printf("Getting head watermark")

	ctx, cancel := context.WithTimeout(ctx, ledgerOpTimeout)
	defer cancel()

	query := `
		SELECT topic, MAX(seq) as max_seq
		FROM lad_records
		GROUP BY topic
	`

	rows, err := l.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("lad: query head: %w", err)
	}
	defer rows.Close()

	wm := make(lad.CausalWatermark)
	for rows.Next() {
		var topic string
		var seq uint64
		if err := rows.Scan(&topic, &seq); err != nil {
			return nil, fmt.Errorf("scan head: %w", err)
		}
		wm[lad.Topic(topic)] = seq
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("lad: rows error: %w", err)
	}

	dbgMesh.Printf("Head watermark: %v", wm)
	return wm, nil
}

// Append validates and stores a record with Raft-replicated persistence.
func (l *MeshLedger) Append(ctx context.Context, rec lad.Record) error {
	if rec.Topic == "" {
		return fmt.Errorf("lad: missing topic")
	}

	dbgMesh.Printf("Appending record (topic=%s, node=%s)", rec.Topic, rec.NodeID)

	// Auto-assign sequence if not set. Head bounds itself; this call
	// gets its own ledgerOpTimeout window. MESH-F06: hold appendMu across the
	// Head→assign→insert so two concurrent same-topic auto-seq Appends can't read
	// the same Head and collide on PRIMARY KEY (topic, seq). Caller-provided seqs
	// own their value and skip the lock.
	if rec.Seq == 0 {
		l.appendMu.Lock()
		defer l.appendMu.Unlock()
		head, err := l.Head(ctx)
		if err != nil {
			return fmt.Errorf("lad: get head for seq: %w", err)
		}
		rec.Seq = head[rec.Topic] + 1
	}

	// Auto-set timestamp if not provided
	if rec.Timestamp.IsZero() {
		rec.Timestamp = time.Now().UTC()
	}

	// Insert record into database (Raft-replicated via tursoraft)
	query := `
		INSERT INTO lad_records (topic, seq, tenant_id, node_id, body, signature, timestamp)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`

	bodyJSON, err := json.Marshal(rec.Body)
	if err != nil {
		return fmt.Errorf("lad: marshal body: %w", err)
	}

	execCtx, cancel := context.WithTimeout(ctx, ledgerOpTimeout)
	defer cancel()

	_, err = l.db.ExecContext(execCtx, query,
		string(rec.Topic),
		rec.Seq,
		rec.TenantID,
		rec.NodeID,
		string(bodyJSON),
		rec.Signature,
		rec.Timestamp.Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("lad: insert record: %w", err)
	}

	dbgMesh.Printf("Record appended (topic=%s, seq=%d)", rec.Topic, rec.Seq)

	// Update Bloom filter if enabled
	if l.bloomMgr != nil {
		l.updateBloomFilter(rec)
	}

	// Notify subscribers
	l.broadcast(rec)

	return nil
}

// BatchAppend appends multiple records in a single transaction for high-throughput scenarios.
// Records are assigned sequences atomically and subscribers are notified after commit.
// Maximum batch size is 1000 records to avoid large transactions.
func (l *MeshLedger) BatchAppend(ctx context.Context, records []lad.Record) error {
	if len(records) == 0 {
		return nil
	}

	const maxBatchSize = 1000
	if len(records) > maxBatchSize {
		return fmt.Errorf("lad: batch size %d exceeds maximum %d", len(records), maxBatchSize)
	}

	dbgMesh.Printf("Batch appending %d records", len(records))

	// Bound the entire batch to ledgerOpTimeout. All SQL ops inside
	// (Head, BeginTx, PrepareContext, statement Exec) inherit this
	// deadline so a slow Turso can't stall the publish path.
	txCtx, cancel := context.WithTimeout(ctx, ledgerOpTimeout)
	defer cancel()

	tx, err := l.db.BeginTx(txCtx, nil)
	if err != nil {
		return fmt.Errorf("lad: begin transaction: %w", err)
	}
	defer tx.Rollback() // Rollback on error

	// Get current head for each topic to assign sequences. Use txCtx
	// so the Head call shares the bounded deadline (was previously
	// using the unbounded caller ctx, which let Head hang past the
	// txCtx timeout and the rest of BatchAppend would still try to
	// proceed against an effectively-cancelled tx).
	head, err := l.Head(txCtx)
	if err != nil {
		return fmt.Errorf("lad: get head for batch: %w", err)
	}

	// Track next sequence per topic within this batch
	nextSeq := make(map[lad.Topic]uint64)
	for topic, seq := range head {
		nextSeq[topic] = seq + 1
	}

	// Prepare insert statement
	stmt, err := tx.PrepareContext(txCtx, `
		INSERT INTO lad_records (topic, seq, tenant_id, node_id, body, signature, timestamp)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("lad: prepare batch insert: %w", err)
	}
	defer stmt.Close()

	// Process each record
	processedRecords := make([]lad.Record, 0, len(records))
	for i := range records {
		rec := &records[i]

		if rec.Topic == "" {
			return fmt.Errorf("lad: missing topic in record %d", i)
		}

		// Auto-assign sequence if not set
		if rec.Seq == 0 {
			rec.Seq = nextSeq[rec.Topic]
			nextSeq[rec.Topic]++
		} else {
			// If sequence is manually set, update our tracking
			if rec.Seq >= nextSeq[rec.Topic] {
				nextSeq[rec.Topic] = rec.Seq + 1
			}
		}

		// Auto-set timestamp if not provided
		if rec.Timestamp.IsZero() {
			rec.Timestamp = time.Now().UTC()
		}

		// Marshal body
		bodyJSON, err := json.Marshal(rec.Body)
		if err != nil {
			return fmt.Errorf("lad: marshal body for record %d: %w", i, err)
		}

		// Execute insert
		_, err = stmt.ExecContext(txCtx,
			string(rec.Topic),
			rec.Seq,
			rec.TenantID,
			rec.NodeID,
			string(bodyJSON),
			rec.Signature,
			rec.Timestamp.Format(time.RFC3339Nano),
		)
		if err != nil {
			return fmt.Errorf("lad: insert record %d: %w", i, err)
		}

		processedRecords = append(processedRecords, *rec)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("lad: commit batch: %w", err)
	}

	dbgMesh.Printf("Batch committed (%d records)", len(processedRecords))

	// Update Bloom filters if enabled
	if l.bloomMgr != nil {
		for _, rec := range processedRecords {
			l.updateBloomFilter(rec)
		}
	}

	// Notify subscribers after successful commit
	for _, rec := range processedRecords {
		l.broadcast(rec)
	}

	return nil
}

// Stream forwards historical and live records to the caller.
func (l *MeshLedger) Stream(ctx context.Context, from lad.CausalWatermark, topics []lad.Topic) (<-chan lad.Record, error) {
	dbgMesh.Printf("Starting stream (topics=%v)", topics)

	topicSet := make(map[lad.Topic]struct{}, len(topics))
	for _, topic := range topics {
		if topic != "" {
			topicSet[topic] = struct{}{}
		}
	}

	out := make(chan lad.Record, 64)

	go func() {
		defer close(out)

		// Build WHERE clause for topic filter
		var topicFilter string
		var args []interface{}
		if len(topicSet) > 0 {
			topicFilter = " AND topic IN ("
			first := true
			for topic := range topicSet {
				if !first {
					topicFilter += ", "
				}
				topicFilter += "?"
				args = append(args, string(topic))
				first = false
			}
			topicFilter += ")"
		}

		// MESH-F03: subscribe to live records BEFORE running the historical
		// query, so any record appended in the window between the query snapshot
		// and the subscribe lands in the subscription buffer instead of being
		// lost by both phases. `delivered` tracks the highest seq already streamed
		// per topic so the live phase can de-dup records the historical scan
		// already covered.
		subID, subCh := l.subscribe(topicSet)
		defer l.unsubscribe(subID)
		delivered := make(map[lad.Topic]uint64)

		// Query historical records
		query := fmt.Sprintf(`
			SELECT topic, seq, tenant_id, node_id, body, signature, timestamp
			FROM lad_records
			WHERE 1=1 %s
			ORDER BY timestamp ASC, topic ASC, seq ASC
		`, topicFilter)

		rows, err := l.db.QueryContext(ctx, query, args...)
		if err != nil {
			dbgMesh.Printf("Stream query error: %v", err)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var rec lad.Record
			var topicStr, bodyStr, timestampStr string

			if err := rows.Scan(&topicStr, &rec.Seq, &rec.TenantID, &rec.NodeID, &bodyStr, &rec.Signature, &timestampStr); err != nil {
				dbgMesh.Printf("Stream scan error: %v", err)
				return
			}

			rec.Topic = lad.Topic(topicStr)
			rec.Body = json.RawMessage(bodyStr)
			rec.Timestamp, _ = time.Parse(time.RFC3339Nano, timestampStr)

			// Skip if before watermark
			if from != nil && rec.Seq <= from[rec.Topic] {
				continue
			}

			select {
			case out <- rec:
				if rec.Seq > delivered[rec.Topic] {
					delivered[rec.Topic] = rec.Seq
				}
			case <-ctx.Done():
				return
			}
		}

		// Drain live records (subscription opened before the historical query).
		for {
			select {
			case rec, ok := <-subCh:
				if !ok {
					return
				}
				// MESH-F03: skip anything the historical scan already streamed or
				// the caller's watermark excludes, so the subscribe-first ordering
				// can't double-deliver.
				if rec.Seq <= delivered[rec.Topic] {
					continue
				}
				if from != nil && rec.Seq <= from[rec.Topic] {
					continue
				}
				select {
				case out <- rec:
					if rec.Seq > delivered[rec.Topic] {
						delivered[rec.Topic] = rec.Seq
					}
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			case <-l.stopChan:
				return
			}
		}
	}()

	return out, nil
}

// Snapshot returns a JSON dump of all records for backup or debugging.
func (l *MeshLedger) Snapshot(ctx context.Context) (io.ReadCloser, error) {
	dbgMesh.Printf("Creating snapshot")

	query := `
		SELECT topic, seq, tenant_id, node_id, body, signature, timestamp
		FROM lad_records
		ORDER BY topic ASC, seq ASC
	`

	rows, err := l.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("lad: query snapshot: %w", err)
	}
	defer rows.Close()

	var records []lad.Record
	for rows.Next() {
		var rec lad.Record
		var topicStr, bodyStr, timestampStr string

		if err := rows.Scan(&topicStr, &rec.Seq, &rec.TenantID, &rec.NodeID, &bodyStr, &rec.Signature, &timestampStr); err != nil {
			return nil, fmt.Errorf("scan snapshot: %w", err)
		}

		rec.Topic = lad.Topic(topicStr)
		rec.Body = json.RawMessage(bodyStr)
		rec.Timestamp, _ = time.Parse(time.RFC3339Nano, timestampStr)

		records = append(records, rec)
	}

	data, err := json.Marshal(records)
	if err != nil {
		return nil, fmt.Errorf("lad: marshal snapshot: %w", err)
	}

	dbgMesh.Printf("Snapshot created (%d records, %d bytes)", len(records), len(data))
	return io.NopCloser(jsonReader{data: data}), nil
}

// jsonReader implements io.Reader for a byte slice
type jsonReader struct {
	data []byte
	pos  int
}

func (r jsonReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

// Manager returns the underlying tursoraft manager (may be nil if using shared manager).
func (l *MeshLedger) Manager() *manager.Manager {
	return l.mgr
}

// subscribe creates a subscription for live records
func (l *MeshLedger) subscribe(topics map[lad.Topic]struct{}) (int, <-chan lad.Record) {
	l.mu.Lock()
	defer l.mu.Unlock()

	id := l.nextSubID
	l.nextSubID++

	ch := make(chan lad.Record, 64)
	l.subscribers[id] = &subscriber{
		id:     id,
		topics: topics,
		ch:     ch,
	}

	return id, ch
}

// unsubscribe removes a subscription
func (l *MeshLedger) unsubscribe(id int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if sub, ok := l.subscribers[id]; ok {
		close(sub.ch)
		delete(l.subscribers, id)
	}
}

// broadcast sends a record to all matching subscribers (uses shared implementation)
func (l *MeshLedger) broadcast(rec lad.Record) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	for _, sub := range l.subscribers {
		// Check if subscriber is interested in this topic
		if len(sub.topics) > 0 {
			if _, ok := sub.topics[rec.Topic]; !ok {
				continue
			}
		}

		select {
		case sub.ch <- rec:
		default:
			// Drop record if subscriber is slow
			dbgMesh.Printf("Dropping record for slow subscriber %d", sub.id)
		}
	}
}

// runCompaction periodically removes old records based on retention policy
func (l *MeshLedger) runCompaction() {
	for {
		select {
		case <-l.stopChan:
			return
		case <-l.compactTicker.C:
			l.compact()
		}
	}
}

// compact removes records older than retention period
func (l *MeshLedger) compact() {
	if l.retentionDays <= 0 {
		return
	}

	dbgMesh.Printf("Running compaction (retention=%dd)", l.retentionDays)

	cutoff := time.Now().UTC().AddDate(0, 0, -l.retentionDays)

	result, err := l.db.Exec(`
		DELETE FROM lad_records
		WHERE timestamp < ?
	`, cutoff.Format(time.RFC3339Nano))

	if err != nil {
		dbgMesh.Printf("Compaction error: %v", err)
		return
	}

	if rows, _ := result.RowsAffected(); rows > 0 {
		dbgMesh.Printf("Compacted %d records older than %s", rows, cutoff.Format(time.RFC3339))
	}
}

// updateBloomFilter adds the record to the appropriate Bloom filters.
//
// MESH-F05: this whole Bloom subsystem is currently WRITE-ONLY — no query path
// calls bf.Contains, so the per-Append updates, the periodic rebuild, and the
// Turso persistence are pure CPU/memory/DB-IO overhead with no read benefit. It
// is retained (and gated OFF unless cfg.BloomConfig is set) pending an
// existence-check query fast-path that actually consults the filters; until
// then, enabling it buys nothing. The crash surface it used to carry is closed
// by MESH-F02 / MESH-F12.
func (l *MeshLedger) updateBloomFilter(rec lad.Record) {
	if l.bloomMgr == nil {
		return
	}

	// Scope by topic
	topicFilter := l.bloomMgr.GetOrCreate(fmt.Sprintf("topic:%s", rec.Topic))
	topicFilter.Add([]byte(fmt.Sprintf("%s:%d", rec.Topic, rec.Seq)))

	// Scope by tenant+topic for efficient tenant queries
	if rec.TenantID != "" {
		tenantFilter := l.bloomMgr.GetOrCreate(fmt.Sprintf("tenant:%s:%s", rec.TenantID, rec.Topic))
		tenantFilter.Add([]byte(fmt.Sprintf("%s:%s:%d", rec.TenantID, rec.Topic, rec.Seq)))
	}

	// Scope by node for node-specific queries
	if rec.NodeID != "" {
		nodeFilter := l.bloomMgr.GetOrCreate(fmt.Sprintf("node:%s", rec.NodeID))
		nodeFilter.Add([]byte(fmt.Sprintf("%s:%d", rec.NodeID, rec.Seq)))
	}
}

// loadBloomFilters loads persisted Bloom filters from database
func (l *MeshLedger) loadBloomFilters() error {
	if l.bloomMgr == nil {
		return nil
	}

	rows, err := l.db.Query(`
		SELECT scope, filter_data FROM lad_bloom_filters
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	loaded := 0
	for rows.Next() {
		var scope string
		var data []byte
		if err := rows.Scan(&scope, &data); err != nil {
			continue // Skip invalid entries
		}

		filter, err := bloom.DeserializeBloomFilter(data)
		if err != nil {
			continue // Skip corrupt filters
		}

		l.bloomMgr.Set(scope, filter)
		loaded++
	}

	if loaded > 0 {
		dbgMesh.Printf("Loaded %d Bloom filters from storage", loaded)
	}

	return rows.Err()
}

// runBloomRebuild periodically rebuilds Bloom filters
func (l *MeshLedger) runBloomRebuild() {
	for {
		select {
		case <-l.stopChan:
			return
		case <-l.bloomTicker.C:
			l.rebuildBloomFilters()
		}
	}
}

// rebuildBloomFilters rebuilds all Bloom filters from current data
func (l *MeshLedger) rebuildBloomFilters() {
	if l.bloomMgr == nil {
		return
	}

	dbgMesh.Printf("Rebuilding bloom filters")

	// Clear existing filters
	l.bloomMgr.Clear()

	// Rebuild from all records
	rows, err := l.db.Query(`
		SELECT topic, seq, tenant_id, node_id FROM lad_records
	`)
	if err != nil {
		dbgMesh.Printf("Bloom rebuild query error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var topicStr, tenantID, nodeID string
		var seq uint64
		if err := rows.Scan(&topicStr, &seq, &tenantID, &nodeID); err != nil {
			continue
		}

		rec := lad.Record{
			Topic:    lad.Topic(topicStr),
			Seq:      seq,
			TenantID: tenantID,
			NodeID:   nodeID,
		}
		l.updateBloomFilter(rec)
		count++
	}

	// Persist rebuilt filters
	l.persistBloomFilters()

	dbgMesh.Printf("Bloom filters rebuilt from %d records", count)
}

// persistBloomFilters saves Bloom filters to database
func (l *MeshLedger) persistBloomFilters() {
	if l.bloomMgr == nil {
		return
	}

	filters := l.bloomMgr.Filters()
	for scope, filter := range filters {
		data := filter.Serialize()
		_, err := l.db.Exec(`
			INSERT OR REPLACE INTO lad_bloom_filters (scope, filter_data, updated_at)
			VALUES (?, ?, ?)
		`, scope, data, time.Now().UTC().Format(time.RFC3339Nano))
		if err != nil {
			dbgMesh.Printf("Failed to persist bloom filter %s: %v", scope, err)
		}
	}
}

// Stats returns ledger statistics for monitoring
type MeshLedgerStats struct {
	GroupName     string
	DBName        string
	RecordCount   int64
	TopicCount    int
	OldestRecord  time.Time
	NewestRecord  time.Time
	SubscriberCnt int
}

// Stats returns current ledger statistics
func (l *MeshLedger) Stats(ctx context.Context) (MeshLedgerStats, error) {
	stats := MeshLedgerStats{
		GroupName: l.groupName,
		DBName:    l.dbName,
	}

	ctx, cancel := context.WithTimeout(ctx, ledgerOpTimeout)
	defer cancel()

	// Get record count
	var count int64
	err := l.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM lad_records`).Scan(&count)
	if err != nil {
		return stats, err
	}
	stats.RecordCount = count

	// Get topic count
	var topicCount int
	err = l.db.QueryRowContext(ctx, `SELECT COUNT(DISTINCT topic) FROM lad_records`).Scan(&topicCount)
	if err != nil {
		return stats, err
	}
	stats.TopicCount = topicCount

	// Get oldest/newest timestamps
	var oldest, newest sql.NullString
	err = l.db.QueryRowContext(ctx, `SELECT MIN(timestamp), MAX(timestamp) FROM lad_records`).Scan(&oldest, &newest)
	if err != nil {
		return stats, err
	}

	if oldest.Valid {
		stats.OldestRecord, _ = time.Parse(time.RFC3339Nano, oldest.String)
	}
	if newest.Valid {
		stats.NewestRecord, _ = time.Parse(time.RFC3339Nano, newest.String)
	}

	// Get subscriber count
	l.mu.RLock()
	stats.SubscriberCnt = len(l.subscribers)
	l.mu.RUnlock()

	return stats, nil
}
