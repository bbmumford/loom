/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package gossip

import (
	"encoding/binary"
	"fmt"

	"github.com/bbmumford/whisper"
	lad "github.com/bbmumford/ledger"
	"github.com/bbmumford/ledger/cache"
)

// ladSnapshotCodec implements whisper.SnapshotCodec on top of the
// LAD wire envelope. Manifest, ShardRequest and ShardChunk all use a
// compact length-prefixed format; erasure coding is delegated to the
// shared whisper.ReedSolomonErasure helper. Records ride the wire as
// the same protobuf-marshalled LADRecord form used elsewhere — keeps
// the cache.Apply path identical between G1 sync, G3 rumor, G4
// reconcile, and G5 snapshot transfers.
type ladSnapshotCodec struct {
	whisper.ReedSolomonErasure
}

// NewLADSnapshotCodec returns a codec usable with
// whisper.NewSnapshotDriver.
func NewLADSnapshotCodec() whisper.SnapshotCodec { return &ladSnapshotCodec{} }

// EncodeManifest serialises a SnapshotManifest into wire bytes.
//
//	[hlc uint64][fingerprint uint64][record_count uint32][total_bytes uint64]
//	[shard_count uint8][is_anchor uint8]
func (c *ladSnapshotCodec) EncodeManifest(m whisper.SnapshotManifest) ([]byte, error) {
	out := make([]byte, 8+8+4+8+1+1)
	binary.BigEndian.PutUint64(out[0:8], m.HLC)
	binary.BigEndian.PutUint64(out[8:16], m.Fingerprint)
	binary.BigEndian.PutUint32(out[16:20], m.RecordCount)
	binary.BigEndian.PutUint64(out[20:28], m.TotalBytes)
	out[28] = m.ShardCount
	if m.IsAnchor {
		out[29] = 1
	}
	return out, nil
}

// DecodeManifest deserialises a SnapshotManifest from wire bytes.
func (c *ladSnapshotCodec) DecodeManifest(body []byte) (whisper.SnapshotManifest, error) {
	if len(body) < 30 {
		return whisper.SnapshotManifest{}, fmt.Errorf("snapshot: short manifest")
	}
	return whisper.SnapshotManifest{
		HLC:         binary.BigEndian.Uint64(body[0:8]),
		Fingerprint: binary.BigEndian.Uint64(body[8:16]),
		RecordCount: binary.BigEndian.Uint32(body[16:20]),
		TotalBytes:  binary.BigEndian.Uint64(body[20:28]),
		ShardCount:  body[28],
		IsAnchor:    body[29] != 0,
	}, nil
}

// EncodeShardRequest packs a ShardSpec into wire bytes.
func (c *ladSnapshotCodec) EncodeShardRequest(spec whisper.SnapshotShardSpec) ([]byte, error) {
	return []byte{spec.ShardIndex, spec.ShardCount}, nil
}

// DecodeShardRequest unpacks a ShardSpec.
func (c *ladSnapshotCodec) DecodeShardRequest(body []byte) (whisper.SnapshotShardSpec, error) {
	if len(body) < 2 {
		return whisper.SnapshotShardSpec{}, fmt.Errorf("snapshot: short shard request")
	}
	return whisper.SnapshotShardSpec{ShardIndex: body[0], ShardCount: body[1]}, nil
}

// EncodeShardChunk packs a chunk into wire bytes.
//
//	[shard_index uint8][chunk_index uint16][total_chunks uint16][body_len uint32][body]
func (c *ladSnapshotCodec) EncodeShardChunk(ch whisper.SnapshotChunk) ([]byte, error) {
	out := make([]byte, 1+2+2+4+len(ch.Body))
	out[0] = ch.ShardIndex
	binary.BigEndian.PutUint16(out[1:3], ch.ChunkIndex)
	binary.BigEndian.PutUint16(out[3:5], ch.TotalChunks)
	binary.BigEndian.PutUint32(out[5:9], uint32(len(ch.Body)))
	copy(out[9:], ch.Body)
	return out, nil
}

// DecodeShardChunk unpacks a chunk.
func (c *ladSnapshotCodec) DecodeShardChunk(body []byte) (whisper.SnapshotChunk, error) {
	if len(body) < 9 {
		return whisper.SnapshotChunk{}, fmt.Errorf("snapshot: short chunk")
	}
	bodyLen := binary.BigEndian.Uint32(body[5:9])
	// MESH-F11: compare in uint64. `9 + int(bodyLen)` overflows to a negative int
	// on a 32-bit build for a large bodyLen, passing the guard, and the slice
	// below then panics out of range. 64-bit builds were already safe; this makes
	// the bound correct on both.
	if uint64(9)+uint64(bodyLen) > uint64(len(body)) {
		return whisper.SnapshotChunk{}, fmt.Errorf("snapshot: truncated chunk body")
	}
	return whisper.SnapshotChunk{
		ShardIndex:  body[0],
		ChunkIndex:  binary.BigEndian.Uint16(body[1:3]),
		TotalChunks: binary.BigEndian.Uint16(body[3:5]),
		Body:        body[9 : 9+int(bodyLen)],
	}, nil
}

// PartitionForShard distributes records across shards by bucketing
// on each record's canonical ledger.ContentHash. Any peer over the
// same record set produces identical partitions regardless of wire
// encoding quirks — erasure coding then runs across the partitions.
//
// Records arrive as protobuf-marshalled LADRecord wire bytes; the
// codec decodes each one, computes ContentHash(rec), and folds the
// first 8 bytes of that hash to pick the shard. Hashing the wire
// bytes directly would let signature/encoding-order differences
// scatter the same logical record across shards on different peers,
// breaking the determinism Reed-Solomon needs.
//
// Decode failures fall through silently (the record is dropped),
// matching the encodeRecords tolerance applied elsewhere.
func (c *ladSnapshotCodec) PartitionForShard(records [][]byte, spec whisper.SnapshotShardSpec) [][]byte {
	if spec.ShardCount == 0 {
		return records
	}
	out := make([][]byte, 0, len(records)/int(spec.ShardCount)+1)
	for _, r := range records {
		rec, err := decodeRecord(r)
		if err != nil {
			continue
		}
		ch := rec.ContentHash()
		// First 8 bytes of the 16-byte content hash, big-endian,
		// modulo ShardCount. Identical on every peer for the same
		// logical record.
		bucket := binary.BigEndian.Uint64(ch[:8])
		if uint8(bucket%uint64(spec.ShardCount)) == spec.ShardIndex {
			out = append(out, r)
		}
	}
	return out
}

// ladSnapshotStore implements whisper.SnapshotStore on top of the
// directory cache. Records are wire-form protobuf; the snapshot's
// HLC is the cache's max HLC across all records; fingerprint is the
// cache's existing XOR-based content fingerprint.
type ladSnapshotStore struct {
	cache *cache.DirectoryCache
}

// NewLADSnapshotStore returns a SnapshotStore backed by the cache.
func NewLADSnapshotStore(c *cache.DirectoryCache) whisper.SnapshotStore {
	return &ladSnapshotStore{cache: c}
}

// Snapshot returns the cache's full record set wire-encoded, plus
// HLC and fingerprint. HLC is the max across all records — any
// peer comparing manifests can pick the freshest by HLC alone.
func (s *ladSnapshotStore) Snapshot() (records [][]byte, hlc uint64, fingerprint uint64) {
	if s.cache == nil {
		return nil, 0, 0
	}
	dump := s.cache.Dump()
	out := make([][]byte, 0, len(dump))
	var maxHLC uint64
	for _, r := range dump {
		if r.HLCTimestamp > maxHLC {
			maxHLC = r.HLCTimestamp
		}
		body, err := encodeRecord(r)
		if err != nil {
			continue
		}
		out = append(out, body)
	}
	return out, maxHLC, s.cache.Fingerprint()
}

// Apply ingests a single inbound record from a snapshot transfer.
func (s *ladSnapshotStore) Apply(record []byte) error {
	if s.cache == nil {
		return nil
	}
	rec, err := decodeRecord(record)
	if err != nil {
		return err
	}
	return s.cache.Apply(rec)
}

// Suppress unused-import lint when the package compiles standalone.
var _ lad.Record
