/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package gossip

import (
	"encoding/binary"
	"fmt"

	lad "github.com/bbmumford/ledger"
	"github.com/bbmumford/ledger/cache"
	"github.com/bbmumford/whisper"
	"github.com/bbmumford/whisper/iblt"
	"google.golang.org/protobuf/proto"
)

// ladReconcileCodec implements whisper.ReconcileCodec on top of the
// LAD record envelope. Tables, replies, and requests serialise with
// a compact length-prefixed format — no protobuf round-trip cost on
// the hot reconciliation path.
//
// Wire formats (per body, after the engine's [magic][length][flags]
// envelope):
//
//	Table:   [k uint32][seed uint64][cell_count uint32][cells...]
//	Reply:   [count uint32][len uint32][record_bytes]...
//	Request: [count uint32][16-byte content_hash]...
//
// Each cell encodes as [16-byte key XOR][8-byte key-hash XOR][4-byte count].
type ladReconcileCodec struct{}

// NewLADReconcileCodec returns a codec usable with
// whisper.NewReconcileDriver. Stateless; one instance per process.
func NewLADReconcileCodec() whisper.ReconcileCodec { return &ladReconcileCodec{} }

const reconcileCellSize = 16 + 8 + 4 // 28 bytes per cell

// EncodeTable serialises an IBLT to wire bytes.
func (c *ladReconcileCodec) EncodeTable(t *iblt.IBLT, mode whisper.ReconcileMode) ([]byte, error) {
	cells := t.Cells()
	header := 4 + 8 + 4 + 1 // k uint32 + seed uint64 + cell_count uint32 + mode byte
	out := make([]byte, header+len(cells)*reconcileCellSize)
	binary.BigEndian.PutUint32(out[0:4], uint32(t.K()))
	binary.BigEndian.PutUint64(out[4:12], t.Seed())
	binary.BigEndian.PutUint32(out[12:16], uint32(len(cells)))
	out[16] = byte(mode)
	off := header
	for _, cell := range cells {
		copy(out[off:off+16], cell.KeySum[:])
		off += 16
		binary.BigEndian.PutUint64(out[off:off+8], cell.KeyHashSum)
		off += 8
		binary.BigEndian.PutUint32(out[off:off+4], uint32(cell.Count))
		off += 4
	}
	return out, nil
}

// DecodeTable deserialises an IBLT from wire bytes. The mode byte
// follows the cell-count so a future wire-format extension can
// negotiate without breaking the table layout.
func (c *ladReconcileCodec) DecodeTable(body []byte) (*iblt.IBLT, whisper.ReconcileMode, error) {
	if len(body) < 17 {
		return nil, 0, fmt.Errorf("reconcile: short table header")
	}
	k := binary.BigEndian.Uint32(body[0:4])
	seed := binary.BigEndian.Uint64(body[4:12])
	count := binary.BigEndian.Uint32(body[12:16])
	mode := whisper.ReconcileMode(body[16])
	if uint64(len(body)) < uint64(17)+uint64(count)*reconcileCellSize {
		return nil, 0, fmt.Errorf("reconcile: truncated cell stream")
	}
	// MESH-F10: k (the IBLT hash-function count) comes off the wire and is
	// passed straight into iblt.New, which may allocate proportional to it. A
	// real table uses a small k (typically 3-4); reject implausible values.
	if k == 0 || k > 16 {
		return nil, 0, fmt.Errorf("reconcile: implausible IBLT k=%d", k)
	}
	tbl := iblt.New(int(count), int(k), seed)
	cells := tbl.Cells()
	// MESH-F10: the constructor may round/clamp the cell count; assert the copy
	// loop stays within the allocated cells rather than indexing out of range.
	if len(cells) < int(count) {
		return nil, 0, fmt.Errorf("reconcile: iblt returned %d cells, need %d", len(cells), count)
	}
	off := 17
	for i := uint32(0); i < count; i++ {
		copy(cells[i].KeySum[:], body[off:off+16])
		off += 16
		cells[i].KeyHashSum = binary.BigEndian.Uint64(body[off : off+8])
		off += 8
		cells[i].Count = int32(binary.BigEndian.Uint32(body[off : off+4]))
		off += 4
	}
	return tbl, mode, nil
}

// EncodeReply packs records (opaque protobuf-marshalled
// LADRecord bytes) into a length-prefixed stream.
func (c *ladReconcileCodec) EncodeReply(records [][]byte) ([]byte, error) {
	total := 4
	for _, r := range records {
		total += 4 + len(r)
	}
	out := make([]byte, total)
	binary.BigEndian.PutUint32(out[0:4], uint32(len(records)))
	off := 4
	for _, r := range records {
		binary.BigEndian.PutUint32(out[off:off+4], uint32(len(r)))
		off += 4
		copy(out[off:], r)
		off += len(r)
	}
	return out, nil
}

// DecodeReply unpacks the reply body.
func (c *ladReconcileCodec) DecodeReply(body []byte) ([][]byte, error) {
	if len(body) < 4 {
		return nil, fmt.Errorf("reconcile: short reply")
	}
	count := binary.BigEndian.Uint32(body[0:4])
	// MESH-F01: bound count against the body length BEFORE allocating, exactly
	// as DecodeRequest does. Each record is at least 4 bytes (its length
	// prefix), so a body of len(body) can hold at most (len(body)-4)/4 records.
	// Without this a peer sending count=0xFFFFFFFF drove a ~103 GB slice-header
	// allocation (4.29e9 × 24 B) before the per-record guard ever ran, OOM-
	// crashing the node.
	if uint64(count) > uint64(len(body)-4)/4 {
		return nil, fmt.Errorf("reconcile: reply count %d exceeds body capacity (%d bytes)", count, len(body))
	}
	out := make([][]byte, 0, count)
	off := 4
	for i := uint32(0); i < count; i++ {
		if off+4 > len(body) {
			return nil, fmt.Errorf("reconcile: truncated reply at record %d", i)
		}
		ln := binary.BigEndian.Uint32(body[off : off+4])
		off += 4
		if off+int(ln) > len(body) {
			return nil, fmt.Errorf("reconcile: truncated record body")
		}
		rec := make([]byte, ln)
		copy(rec, body[off:off+int(ln)])
		out = append(out, rec)
		off += int(ln)
	}
	return out, nil
}

// EncodeRequest packs IBLT keys (16-byte content hashes) into a
// fixed-stride stream.
func (c *ladReconcileCodec) EncodeRequest(ids []iblt.Key) ([]byte, error) {
	out := make([]byte, 4+len(ids)*16)
	binary.BigEndian.PutUint32(out[0:4], uint32(len(ids)))
	for i, id := range ids {
		copy(out[4+i*16:], id[:])
	}
	return out, nil
}

// DecodeRequest unpacks a request body.
func (c *ladReconcileCodec) DecodeRequest(body []byte) ([]iblt.Key, error) {
	if len(body) < 4 {
		return nil, fmt.Errorf("reconcile: short request")
	}
	count := binary.BigEndian.Uint32(body[0:4])
	if uint64(len(body)) < uint64(4)+uint64(count)*16 {
		return nil, fmt.Errorf("reconcile: truncated request stream")
	}
	out := make([]iblt.Key, count)
	for i := uint32(0); i < count; i++ {
		copy(out[i][:], body[4+i*16:4+(i+1)*16])
	}
	return out, nil
}

// ladReconcileStore implements whisper.ReconcileStore on top of the
// DirectoryCache. Records are keyed by ledger.ContentHash truncated
// to 16 bytes (matches iblt.Key); FetchByID looks up records via
// the cache's internal map; Apply hands records to the cache's
// existing record-apply path.
type ladReconcileStore struct {
	cache     *cache.DirectoryCache
	encodeRec func(lad.Record) ([]byte, error)
	decodeRec func([]byte) (lad.Record, error)
}

// NewLADReconcileStore returns a ReconcileStore backed by the
// directory cache. Encode/decode functions are caller-supplied so
// the same protobuf round-trip used elsewhere (recordToProto /
// protoToRecord) feeds the reconciliation path without
// re-implementing.
func NewLADReconcileStore(c *cache.DirectoryCache,
	encode func(lad.Record) ([]byte, error),
	decode func([]byte) (lad.Record, error)) whisper.ReconcileStore {
	return &ladReconcileStore{cache: c, encodeRec: encode, decodeRec: decode}
}

// NewDefaultLADReconcileStore wires the package's existing protobuf
// codec (recordToProto / protoToRecord) into a ReconcileStore. Most
// consumers should use this — the explicit-codec form is for tests
// and migrations.
func NewDefaultLADReconcileStore(c *cache.DirectoryCache) whisper.ReconcileStore {
	return &ladReconcileStore{
		cache:     c,
		encodeRec: encodeRecord,
		decodeRec: decodeRecord,
	}
}

// encodeRecord marshals one lad.Record to its wire form. Mirrors
// encodeRecords (g1_adapter.go) but returns a single record's bytes.
func encodeRecord(r lad.Record) ([]byte, error) {
	return proto.Marshal(recordToProto(r))
}

// Snapshot returns every (NodeID, Topic) projection's content-hash
// key paired with its serialised record bytes. Built off the cache's
// IdentityView projection so the IBLT cells match what reconciliation
// is meant to converge on — per-(node, topic) identity, not raw
// record bytes (which can differ between two peers that merged the
// same logical update via different wire forms, e.g. one via a full
// snapshot and the other via a delta).
//
// Tombstones aren't represented in the projection (Apply removes the
// view entry on tombstone) — the IBLT-level absence of a key is
// itself the deletion signal.
//
// Implementation: walk Dump() once and index records by (NodeID,
// Topic), then iterate the projection looking up matched records.
// O(N + V) where N is record count and V is view count; both are
// bounded by fleet size.
func (s *ladReconcileStore) Snapshot() ([]iblt.Key, [][]byte) {
	if s.cache == nil {
		return nil, nil
	}
	views := s.cache.IdentityViews()
	if len(views) == 0 {
		return nil, nil
	}
	type ntKey struct {
		nodeID string
		topic  string
	}
	byNT := make(map[ntKey]lad.Record, len(views))
	for _, r := range s.cache.Dump() {
		k := ntKey{nodeID: r.NodeID, topic: string(r.Topic)}
		// Keep the highest-HLC record per (node, topic).
		if existing, ok := byNT[k]; ok && existing.HLCTimestamp >= r.HLCTimestamp {
			continue
		}
		byNT[k] = r
	}
	keys := make([]iblt.Key, 0, len(views))
	bytes := make([][]byte, 0, len(views))
	for _, v := range views {
		rec, ok := byNT[ntKey{nodeID: v.NodeID, topic: v.Topic}]
		if !ok {
			continue
		}
		var k iblt.Key
		copy(k[:], v.ContentHash[:])
		data, err := s.encodeRec(rec)
		if err != nil {
			continue
		}
		keys = append(keys, k)
		bytes = append(bytes, data)
	}
	return keys, bytes
}

// FetchByID looks up a record by its content hash. Walks the
// IdentityView projection first so we land on the canonical (node,
// topic) → hash mapping; then resolves the matching record via the
// cache's typed lookup path. O(V + N) in the worst case, the same
// asymptotic bound as Snapshot.
func (s *ladReconcileStore) FetchByID(id iblt.Key) ([]byte, bool) {
	if s.cache == nil {
		return nil, false
	}
	views := s.cache.IdentityViews()
	for _, v := range views {
		var k iblt.Key
		copy(k[:], v.ContentHash[:])
		if k != id {
			continue
		}
		// Found the projection — walk Dump() once to find the
		// matching record. (The projection doesn't carry the body
		// itself; its job is to be a hash + HLC.)
		for _, r := range s.cache.Dump() {
			if r.NodeID == v.NodeID && string(r.Topic) == v.Topic {
				data, err := s.encodeRec(r)
				if err != nil {
					return nil, false
				}
				return data, true
			}
		}
		return nil, false
	}
	return nil, false
}

// Apply ingests a single inbound record from a reconciliation
// reply. Decoding errors drop the record rather than fail the
// whole exchange.
func (s *ladReconcileStore) Apply(record []byte) error {
	if s.cache == nil {
		return nil
	}
	rec, err := s.decodeRec(record)
	if err != nil {
		return err
	}
	return s.cache.Apply(rec)
}
