/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package gossip

import (
	"time"

	lad "github.com/bbmumford/ledger"
	"github.com/bbmumford/ledger/cache"
	"github.com/bbmumford/whisper"
	"google.golang.org/protobuf/proto"
	gossippb "github.com/bbmumford/loom/core/directory/gossip/pb"
)

// ladStateStore adapts a DirectoryCache to whisper.StateStore so the
// native G1 handler can drive LAD exchanges without touching the
// cache directly. Records travel as protobuf-marshalled LADRecord
// bytes so the whisper handler stays payload-agnostic.
type ladStateStore struct {
	cache *cache.DirectoryCache
}

// newLADStateStore wraps a cache for use as a whisper StateStore.
func newLADStateStore(c *cache.DirectoryCache) *ladStateStore {
	return &ladStateStore{cache: c}
}

// Fingerprint returns the cache's order-independent XOR hash so
// whisper's G2 digest handler can short-circuit the exchange when
// both sides already agree.
func (s *ladStateStore) Fingerprint() uint64 {
	if s.cache == nil {
		return 0
	}
	return s.cache.Fingerprint()
}

// Snapshot returns every live cache record as individually-
// marshalled protobuf byte slices. Used on forced full-sync cycles
// and for peers with no prior watermark.
func (s *ladStateStore) Snapshot() [][]byte {
	if s.cache == nil {
		return nil
	}
	return encodeRecords(s.cache.Dump())
}

// Delta returns records mutated since the given time. Implementation
// tolerates a zero time by falling back to Snapshot so whisper's
// handler never has to special-case the bootstrap path.
func (s *ladStateStore) Delta(since time.Time) [][]byte {
	if s.cache == nil {
		return nil
	}
	if since.IsZero() {
		return encodeRecords(s.cache.Dump())
	}
	return encodeRecords(s.cache.DumpSince(since))
}

// Apply ingests a single protobuf-marshalled LADRecord from a peer's
// exchange or rumor push. Decoding errors drop the record rather than
// fail the whole exchange — one malformed record mustn't starve the
// others still waiting in the payload.
func (s *ladStateStore) Apply(data []byte) error {
	if s.cache == nil {
		return nil
	}
	rec, err := decodeRecord(data)
	if err != nil {
		return err
	}
	return s.cache.Apply(rec)
}

// BucketDigest returns the cache's per-bucket XOR digest so whisper's G2B
// handler can localise a scalar-fingerprint mismatch to specific buckets.
// Satisfies whisper.BucketFingerprintProvider — its presence auto-wires the
// bucketed-digest handler. XOR(BucketDigest(n)) == Fingerprint() by construction.
func (s *ladStateStore) BucketDigest(buckets uint16) []uint64 {
	if s.cache == nil {
		return nil
	}
	return s.cache.BucketDigest(buckets)
}

// RecordsInBuckets returns the marshalled records whose key falls in one of the
// wanted buckets — the bucket-scoped counterpart of Snapshot for the G1
// follow-up to a G2B mismatch. Satisfies whisper.BucketRecordsProvider. Records
// are serialized exactly as Snapshot/Delta so the peer decodes them identically.
func (s *ladStateStore) RecordsInBuckets(buckets uint16, want []bool) [][]byte {
	if s.cache == nil {
		return nil
	}
	return encodeRecords(s.cache.DumpBuckets(buckets, want))
}

// encodeRecords marshals each lad.Record to its protobuf form. An
// individual record that fails to marshal is skipped — the same
// tolerance the legacy respondToG1Exchange applied.
func encodeRecords(records []lad.Record) [][]byte {
	out := make([][]byte, 0, len(records))
	for _, rec := range records {
		b, err := proto.Marshal(recordToProto(rec))
		if err != nil {
			continue
		}
		out = append(out, b)
	}
	return out
}

// decodeRecord unmarshals a single protobuf LADRecord back into a
// lad.Record.
func decodeRecord(data []byte) (lad.Record, error) {
	var pb gossippb.LADRecord
	if err := proto.Unmarshal(data, &pb); err != nil {
		return lad.Record{}, err
	}
	return protoToRecord(&pb), nil
}

// ladG1Codec implements whisper.G1Codec on top of the LAD protobuf
// envelope format. Records arrive at the codec already protobuf-
// marshalled (from ladStateStore.Snapshot/Delta), so the codec only
// wraps them in the envelope and attaches the ExchangeMeta proto.
type ladG1Codec struct{}

// newLADG1Codec returns a codec suitable for WithG1Codec.
func newLADG1Codec() *ladG1Codec { return &ladG1Codec{} }

// EncodeExchange builds a GossipEnvelope containing the pre-marshalled
// records and the whisper-native ExchangeMeta. The records are already
// protobuf-marshalled LADRecord bytes so we slot them straight into
// the envelope's Records field without re-marshalling.
func (c *ladG1Codec) EncodeExchange(records [][]byte, meta *whisper.ExchangeMeta) ([]byte, error) {
	env := &gossippb.GossipEnvelope{Records: records}
	if meta != nil {
		env.Meta = metaToProto(meta)
	}
	return proto.Marshal(env)
}

// DecodeExchange unwraps the GossipEnvelope, returning the raw
// per-record protobuf bytes for the StateStore to apply. SeenNodeIDs
// is populated by peeking each record's NodeID field — cheaper than
// decoding the full record twice because the handler will re-decode
// anyway inside Apply.
func (c *ladG1Codec) DecodeExchange(body []byte) (whisper.DecodedExchange, error) {
	var env gossippb.GossipEnvelope
	if err := proto.Unmarshal(body, &env); err != nil {
		return whisper.DecodedExchange{}, err
	}
	out := whisper.DecodedExchange{Records: env.Records}
	if env.Meta != nil {
		out.Meta = protoToMeta(env.Meta)
	}
	if len(env.Records) > 0 {
		seen := make(map[string]struct{}, len(env.Records))
		for _, recBytes := range env.Records {
			var pb gossippb.LADRecord
			if err := proto.Unmarshal(recBytes, &pb); err != nil {
				continue
			}
			if pb.NodeId != "" {
				seen[pb.NodeId] = struct{}{}
			}
		}
		if len(seen) > 0 {
			out.SeenNodeIDs = make([]string, 0, len(seen))
			for id := range seen {
				out.SeenNodeIDs = append(out.SeenNodeIDs, id)
			}
		}
	}
	return out, nil
}
