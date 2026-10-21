/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package gossip

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	lad "github.com/bbmumford/ledger"
	gossippb "github.com/bbmumford/loom/core/directory/gossip/pb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ErrAllRecordsCorrupt is returned by unmarshalEnvelope when the
// envelope contained at least one record but every record failed to
// decode. Callers (gossip exchange handler) MUST surface this as an
// envelope-level failure so the peer's exchange counter doesn't tick
// as "successful empty exchange" — otherwise a corrupt-record-storm
// peer looks healthy from the directory's POV while delivering no
// usable state.
var ErrAllRecordsCorrupt = errors.New("gossip: every record in envelope failed to decode")

// corruptRecordLogThrottleNs throttles the per-record decode-failure
// log to at most one line per logged-bucket interval, so a peer
// emitting a corrupted-record storm cannot DoS our log infrastructure.
// The first failure of each unmarshalEnvelope call still logs (so
// operators see something on the first bad envelope); subsequent
// failures within the throttle window are dropped silently and
// counted in the envelope summary line.
const corruptRecordLogThrottleNs int64 = int64(5 * time.Second)

// lastCorruptLogUnixNano holds the most-recent unix-nanos timestamp at
// which a per-record corruption was logged. Read/written via
// atomic.CompareAndSwap so the throttle is safe across concurrent
// unmarshal calls without a dedicated mutex.
var lastCorruptLogUnixNano atomic.Int64

// ErrEnvelopeVersion is the refusal of an envelope whose codec_version and
// schema_version are not the pair the session's negotiated gossip.codec writes.
// errors.Is matches it on an *EnvelopeVersionError.
var ErrEnvelopeVersion = errors.New("gossip: envelope version not supported")

// EnvelopeVersionError names the pair an envelope carried and the pair the
// negotiated codec writes.
type EnvelopeVersionError struct {
	Codec, Schema         uint32
	WantCodec, WantSchema uint32
}

func (e *EnvelopeVersionError) Error() string {
	return fmt.Sprintf("%v: codec %d schema %d, want codec %d schema %d",
		ErrEnvelopeVersion, e.Codec, e.Schema, e.WantCodec, e.WantSchema)
}

// Is lets errors.Is(err, ErrEnvelopeVersion) match.
func (e *EnvelopeVersionError) Is(target error) bool { return target == ErrEnvelopeVersion }

// envelopeCodecField and envelopeSchemaField are the field numbers of
// GossipEnvelope's codec_version and schema_version, read from the generated
// descriptor so checkEnvelopeVersions and the .proto cannot disagree.
var (
	envelopeCodecField  = envelopeField("codec_version")
	envelopeSchemaField = envelopeField("schema_version")
)

func envelopeField(name string) protowire.Number {
	f := (&gossippb.GossipEnvelope{}).ProtoReflect().Descriptor().Fields().ByName(protoreflect.Name(name))
	if f == nil {
		panic("gossip: GossipEnvelope has no field " + name)
	}
	return f.Number()
}

// checkEnvelopeVersions reads the codec_version and schema_version of an
// encoded GossipEnvelope by scanning its top-level fields, without decoding
// meta or any record, and refuses with an *EnvelopeVersionError a pair p does
// not write. A later occurrence of a field wins, as it does for
// proto.Unmarshal. unmarshalEnvelope and ladG1Codec.DecodeExchange call it
// first, so an envelope at an unsupported version changes nothing.
func checkEnvelopeVersions(p Profile, data []byte) error {
	var codec, schema uint32
	for b := data; len(b) > 0; {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return fmt.Errorf("gossip: envelope: %w", protowire.ParseError(n))
		}
		b = b[n:]
		if (num == envelopeCodecField || num == envelopeSchemaField) && typ == protowire.VarintType {
			v, m := protowire.ConsumeVarint(b)
			if m < 0 {
				return fmt.Errorf("gossip: envelope: %w", protowire.ParseError(m))
			}
			if num == envelopeCodecField {
				codec = uint32(v)
			} else {
				schema = uint32(v)
			}
			b = b[m:]
			continue
		}
		m := protowire.ConsumeFieldValue(num, typ, b)
		if m < 0 {
			return fmt.Errorf("gossip: envelope: %w", protowire.ParseError(m))
		}
		b = b[m:]
	}
	wantCodec, wantSchema := p.envelope()
	if codec != wantCodec || schema != wantSchema || wantCodec == 0 {
		return &EnvelopeVersionError{Codec: codec, Schema: schema, WantCodec: wantCodec, WantSchema: wantSchema}
	}
	return nil
}

// marshalEnvelope serialises a gossip envelope to protobuf, stamped with the
// codec_version and schema_version p's codec writes.
func marshalEnvelope(p Profile, meta *ExchangeMeta, records []lad.Record) ([]byte, error) {
	env := &gossippb.GossipEnvelope{}
	env.CodecVersion, env.SchemaVersion = p.envelope()

	if meta != nil {
		env.Meta = metaToProto(meta)
	}

	// Each record is individually proto-marshalled as opaque bytes.
	// This keeps the envelope payload-agnostic (Whisper extraction requirement).
	for _, rec := range records {
		recBytes, err := proto.Marshal(recordToProto(rec))
		if err != nil {
			return nil, err
		}
		env.Records = append(env.Records, recBytes)
	}

	return proto.Marshal(env)
}

// unmarshalEnvelope deserialises a protobuf gossip envelope. An envelope at a
// version pair p's codec does not write is refused by checkEnvelopeVersions
// before meta or any record decodes.
func unmarshalEnvelope(p Profile, data []byte) (*ExchangeMeta, []lad.Record, error) {
	if err := checkEnvelopeVersions(p, data); err != nil {
		return nil, nil, err
	}
	var env gossippb.GossipEnvelope
	if err := proto.Unmarshal(data, &env); err != nil {
		return nil, nil, err
	}

	var meta *ExchangeMeta
	if env.Meta != nil {
		meta = protoToMeta(env.Meta)
	}

	records := make([]lad.Record, 0, len(env.Records))
	skipped := 0
	for i, recBytes := range env.Records {
		var pbRec gossippb.LADRecord
		if err := proto.Unmarshal(recBytes, &pbRec); err != nil {
			skipped++
			// Rate-limited per-record log: first failure of each
			// throttle window emits the per-record diagnostic; bulk
			// failures within the window are summarised at the end
			// of this function. Avoids a corrupted-record-storm
			// peer flooding the log.
			now := time.Now().UnixNano()
			prev := lastCorruptLogUnixNano.Load()
			if now-prev > corruptRecordLogThrottleNs &&
				lastCorruptLogUnixNano.CompareAndSwap(prev, now) {
				log.Printf("[GOSSIP] Skipping corrupted record %d/%d: %v (subsequent within %s suppressed)",
					i, len(env.Records), err, time.Duration(corruptRecordLogThrottleNs))
			}
			continue
		}
		records = append(records, protoToRecord(&pbRec))
	}

	// Envelope-level failure: every record decoded as garbage. A
	// malformed envelope with len(env.Records)==0 is treated as a
	// (possibly intentional) empty exchange and returns OK — no
	// records to decode is different from records present but all
	// undecodable.
	if len(env.Records) > 0 && len(records) == 0 {
		log.Printf("[GOSSIP] envelope-level failure: %d/%d records corrupted — surfacing as error",
			skipped, len(env.Records))
		return meta, nil, ErrAllRecordsCorrupt
	}
	if skipped > 0 {
		log.Printf("[GOSSIP] envelope decoded with %d/%d records corrupted (continuing with %d usable)",
			skipped, len(env.Records), len(records))
	}

	return meta, records, nil
}

// recordToProto converts a lad.Record to its protobuf representation.
// Mirrors every field that lad.Record carries so peers receive the
// complete state. Zero-value timestamps (DeletedAt / ExpiresAt) are
// only written when non-zero — proto3 default-zero semantics let the
// receiver distinguish "never expires" from "expired at epoch" via
// the same idiom as DeletedAtUnix.
func recordToProto(r lad.Record) *gossippb.LADRecord {
	pb := &gossippb.LADRecord{
		Topic:           string(r.Topic),
		NodeId:          r.NodeID,
		Partition:       r.Partition,
		Body:            []byte(r.Body),
		TimestampUnix:   r.Timestamp.UnixNano(),
		LamportClock:    r.LamportClock,
		Seq:             r.Seq,
		Tombstone:       r.Tombstone,
		TombstoneReason: r.TombstoneReason,
		Signature:       r.Signature,
		HlcTimestamp:    r.HLCTimestamp,
		AuthorPubkey:    r.AuthorPubKey,
		BlobCid:         r.BlobCID,
	}
	if !r.DeletedAt.IsZero() {
		pb.DeletedAtUnix = r.DeletedAt.UnixNano()
	}
	if !r.ExpiresAt.IsZero() {
		pb.ExpiresAtUnix = r.ExpiresAt.UnixNano()
	}
	return pb
}

// protoToRecord converts a protobuf LADRecord back to lad.Record.
// Decoded timestamps are clamped at zero when the wire field is zero
// (proto3 default) so the receiver's IsZero() checks for "never
// expires" / "not deleted" continue to work without surprise epoch
// values.
func protoToRecord(pb *gossippb.LADRecord) lad.Record {
	r := lad.Record{
		Topic:           lad.Topic(pb.Topic),
		NodeID:          pb.NodeId,
		Partition:       pb.Partition,
		Body:            pb.Body,
		Timestamp:       time.Unix(0, pb.TimestampUnix),
		LamportClock:    pb.LamportClock,
		Seq:             pb.Seq,
		Tombstone:       pb.Tombstone,
		TombstoneReason: pb.TombstoneReason,
		Signature:       pb.Signature,
		HLCTimestamp:    pb.HlcTimestamp,
		AuthorPubKey:    pb.AuthorPubkey,
		BlobCID:         pb.BlobCid,
	}
	if pb.DeletedAtUnix != 0 {
		r.DeletedAt = time.Unix(0, pb.DeletedAtUnix)
	}
	if pb.ExpiresAtUnix != 0 {
		r.ExpiresAt = time.Unix(0, pb.ExpiresAtUnix)
	}
	return r
}

// metaToProto converts ExchangeMeta to its protobuf representation.
func metaToProto(m *ExchangeMeta) *gossippb.ExchangeMeta {
	pb := &gossippb.ExchangeMeta{
		CacheFingerprint:   m.CacheFingerprint,
		BackpressureSignal: m.BackpressureSignal,
	}

	if len(m.PEXEntries) > 0 {
		for _, entry := range m.PEXEntries {
			// Guard zero-time SignedAt — time.Time{}.Unix() returns
			// -62135596800 (year 1 BC) which survives the wire and
			// is interpreted by IsStalePEXEntry as a 2000-year-old
			// entry, filtering it forever. Mirrors the DeletedAtUnix
			// zero-check pattern.
			var ts int64
			if !entry.SignedAt.IsZero() {
				ts = entry.SignedAt.Unix()
			}
			pb.PexEntries = append(pb.PexEntries, &gossippb.PEXEntry{
				NodeId:        entry.NodeID,
				Addresses:     entry.Addresses,
				TimestampUnix: ts,
				Signature:     entry.Signature,
				Region:        entry.Region,
			})
		}
	}

	if len(m.ConnectionCounts) > 0 {
		pb.ConnectionCounts = make(map[string]int32, len(m.ConnectionCounts))
		for k, v := range m.ConnectionCounts {
			pb.ConnectionCounts[k] = int32(v)
		}
	}

	if m.Offer != nil {
		pb.Offer = &gossippb.ConnectionOffer{
			FromNodeId: m.Offer.SenderNodeID,
			Transport:  strings.Join(m.Offer.OfferedTransports, ","),
			// to_node_id, addresses, timestamp_unix, signature are not populated
			// because the Go ConnectionOffer struct doesn't carry them.
			// TODO: populate these proto fields once ConnectionOffer carries
			// them and peer_connections.go sends it.
		}
	}

	return pb
}

// protoToMeta converts a protobuf ExchangeMeta back to ExchangeMeta.
func protoToMeta(pb *gossippb.ExchangeMeta) *ExchangeMeta {
	m := &ExchangeMeta{
		CacheFingerprint:   pb.CacheFingerprint,
		BackpressureSignal: pb.BackpressureSignal,
	}

	if len(pb.PexEntries) > 0 {
		for _, entry := range pb.PexEntries {
			// Mirror the encode-side zero guard — TimestampUnix == 0
			// decodes to the zero time, NOT to 1970-01-01, so the
			// staleness check sees IsZero() and rejects rather than
			// applying a fixed-epoch staleness math.
			var signedAt time.Time
			if entry.TimestampUnix != 0 {
				signedAt = time.Unix(entry.TimestampUnix, 0)
			}
			m.PEXEntries = append(m.PEXEntries, PEXEntry{
				NodeID:    entry.NodeId,
				Addresses: entry.Addresses,
				SignedAt:  signedAt,
				Signature: entry.Signature,
				Region:    entry.Region,
			})
		}
	}

	if len(pb.ConnectionCounts) > 0 {
		m.ConnectionCounts = make(map[string]int, len(pb.ConnectionCounts))
		for k, v := range pb.ConnectionCounts {
			m.ConnectionCounts[k] = int(v)
		}
	}

	if pb.Offer != nil {
		m.Offer = &ConnectionOffer{
			SenderNodeID:      pb.Offer.FromNodeId,
			WantDirectConnect: true,
		}
		if pb.Offer.Transport != "" {
			m.Offer.OfferedTransports = strings.Split(pb.Offer.Transport, ",")
		}
	}

	return m
}
