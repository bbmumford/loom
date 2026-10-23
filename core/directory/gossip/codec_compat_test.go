/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package gossip

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
	"github.com/bbmumford/ledger/cache"
	gossippb "github.com/bbmumford/loom/core/directory/gossip/pb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// The fixtures in this file hold the gossip codec to what a record keeps
// across two compatible encoders: the bytes its signature covers, the bytes
// its merge compares, and one rule for fields a decoder does not know. A
// compatible encoder here is one writing the negotiated version pair (1, 1)
// with fields this build's descriptor does not have, which is the form a peer
// of the next compatible build gives a rolling fleet. The fixtures then show
// what gains no record authority: a tampered or unsigned record on a signed
// topic, bytes that are not a record, and a frame past the payload ceiling.

// signedTopicACL is the gate node registers on every identity topic: a record
// whose signature does not reproduce over the ledger's signed content is
// refused before it merges.
func signedTopicACL(_ string, _ []byte, rec lad.Record) error {
	if !lad.VerifyRecord(rec) {
		return lad.ErrSignatureInvalid
	}
	return nil
}

// signedCache is a cache gating the member topic as node does.
func signedCache() *cache.DirectoryCache {
	c := cache.NewDirectoryCache()
	c.RegisterACL(lad.TopicMember, signedTopicACL)
	return c
}

// signedMember is one member record signed by a fresh author key, with every
// field the signature covers set to a non-zero value.
func signedMember(t *testing.T, node string) lad.Record {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(lad.MemberRecord{Partition: "t", NodeID: node, PubKey: pub, CreatedAt: seedTime})
	if err != nil {
		t.Fatal(err)
	}
	rec := lad.Record{
		Topic: lad.TopicMember, Partition: "t", NodeID: node, Body: body,
		Timestamp: seedTime, Seq: 3, LamportClock: 11, HLCTimestamp: 1_700_000_000_000_000_005,
		ExpiresAt: seedTime.Add(time.Hour), BlobCID: "bafy-" + node,
	}
	lad.SignRecord(&rec, priv)
	return rec
}

// withUnknownFields appends fields no descriptor of this build names to an
// encoded message: a varint at 99, bytes at 100 and a fixed32 at 101.
func withUnknownFields(encoded []byte) []byte {
	out := append([]byte(nil), encoded...)
	out = protowire.AppendTag(out, 99, protowire.VarintType)
	out = protowire.AppendVarint(out, 42)
	out = protowire.AppendTag(out, 100, protowire.BytesType)
	out = protowire.AppendBytes(out, []byte("a field of the next compatible build"))
	out = protowire.AppendTag(out, 101, protowire.Fixed32Type)
	out = protowire.AppendFixed32(out, 7)
	return out
}

// compatibleEnvelope encodes an envelope at pair (1, 1) the way a peer with a
// wider descriptor would: unknown fields in the envelope, in its meta and in
// every record. recordBytes are the record encodings it carries.
func compatibleEnvelope(t *testing.T, meta *ExchangeMeta, recordBytes ...[]byte) []byte {
	t.Helper()
	metaBytes, err := proto.Marshal(metaToProto(meta))
	if err != nil {
		t.Fatal(err)
	}
	var env []byte
	env = protowire.AppendTag(env, 1, protowire.BytesType)
	env = protowire.AppendBytes(env, withUnknownFields(metaBytes))
	for _, rec := range recordBytes {
		env = protowire.AppendTag(env, 2, protowire.BytesType)
		env = protowire.AppendBytes(env, rec)
	}
	env = protowire.AppendTag(env, 3, protowire.VarintType)
	env = protowire.AppendVarint(env, 1)
	env = protowire.AppendTag(env, 4, protowire.VarintType)
	env = protowire.AppendVarint(env, 1)
	return withUnknownFields(env)
}

// sameRecord compares the fields a lad.Record carries on the wire.
func sameRecord(t *testing.T, got, want lad.Record) {
	t.Helper()
	if got.Topic != want.Topic || got.Partition != want.Partition || got.NodeID != want.NodeID ||
		!bytes.Equal(got.Body, want.Body) || !bytes.Equal(got.Signature, want.Signature) ||
		!bytes.Equal(got.AuthorPubKey, want.AuthorPubKey) || !got.Timestamp.Equal(want.Timestamp) ||
		got.Seq != want.Seq || got.LamportClock != want.LamportClock || got.HLCTimestamp != want.HLCTimestamp ||
		!got.ExpiresAt.Equal(want.ExpiresAt) || got.Tombstone != want.Tombstone || !got.DeletedAt.Equal(want.DeletedAt) ||
		got.TombstoneReason != want.TombstoneReason || got.BlobCID != want.BlobCID {
		t.Fatalf("record differs:\n got %+v\nwant %+v", got, want)
	}
}

// TestTwoCompatibleCodecsPreserveTheSignedAndMergeBytes: an envelope written
// by this build and one written at the same version pair with fields this
// build does not know decode to the same record; its signature verifies from
// both, its content hash, the merge's identity, is the same from both, the
// decoder re-encodes both to this build's canonical bytes, dropping the
// unknown fields rather than carrying them, and a signed-topic cache admits
// the record from both forms to one state: the same member projection, the
// same identity view. Every unknown field in the envelope, its meta and the
// record is consumed and ignored; none is read as a version, a record or a
// meta field. The cache's Dump rebuilds a record from its projection and
// signs it with the node's own key when one is set (ledger
// cache.DirectoryCache.dumpSigningKey), so what the cache keeps of a record
// is its body and identity, compared here between the two forms.
func TestTwoCompatibleCodecsPreserveTheSignedAndMergeBytes(t *testing.T) {
	p := testProfile(t)
	rec := signedMember(t, "node-a")
	if !lad.VerifyRecord(rec) {
		t.Fatalf("control: the fixture's own record does not verify")
	}
	canonical, err := encodeRecord(rec)
	if err != nil {
		t.Fatal(err)
	}
	meta := &ExchangeMeta{CacheFingerprint: 7, BackpressureSignal: true}
	ours, err := marshalEnvelope(p, meta, []lad.Record{rec})
	if err != nil {
		t.Fatal(err)
	}
	theirs := compatibleEnvelope(t, meta, withUnknownFields(canonical))
	if bytes.Equal(ours, theirs) {
		t.Fatalf("control: the two envelopes are the same bytes")
	}

	decoded := map[string]lad.Record{}
	projected := map[string][]lad.Record{}
	views := map[string]map[string]string{}
	for name, env := range map[string][]byte{"this build": ours, "a wider descriptor": theirs} {
		gotMeta, records, err := unmarshalEnvelope(p, env)
		if err != nil {
			t.Fatalf("%s: unmarshalEnvelope: %v", name, err)
		}
		if gotMeta == nil || gotMeta.CacheFingerprint != 7 || !gotMeta.BackpressureSignal {
			t.Fatalf("%s: meta %+v", name, gotMeta)
		}
		if len(records) != 1 {
			t.Fatalf("%s: %d records", name, len(records))
		}
		got := records[0]
		sameRecord(t, got, rec)
		if !lad.VerifyRecord(got) {
			t.Fatalf("%s: the decoded record's signature does not verify", name)
		}
		if got.ContentHash() != rec.ContentHash() {
			t.Fatalf("%s: content hash %x, want %x", name, got.ContentHash(), rec.ContentHash())
		}
		again, err := encodeRecord(got)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(again, canonical) {
			t.Fatalf("%s: re-encoded to % x\nwant the canonical % x", name, again, canonical)
		}
		decoded[name] = got

		// The responder's codec carries the record bytes as it received
		// them and reads the same meta.
		ex, err := newLADG1Codec(p).DecodeExchange(env)
		if err != nil {
			t.Fatalf("%s: DecodeExchange: %v", name, err)
		}
		if len(ex.Records) != 1 || ex.Meta == nil || ex.Meta.CacheFingerprint != 7 || len(ex.SeenNodeIDs) != 1 || ex.SeenNodeIDs[0] != "node-a" {
			t.Fatalf("%s: exchange %+v", name, ex)
		}
		// Through the responder's store the same record lands in a
		// signed-topic cache.
		c := signedCache()
		if err := newLADStateStore(c).Apply(ex.Records[0]); err != nil {
			t.Fatalf("%s: store.Apply: %v", name, err)
		}
		dumped := c.Dump()
		if len(dumped) != 1 || dumped[0].Topic != lad.TopicMember || dumped[0].NodeID != "node-a" || dumped[0].Partition != "t" {
			t.Fatalf("%s: the cache holds %+v", name, dumped)
		}
		projected[name] = dumped
		views[name] = map[string]string{}
		for _, v := range c.IdentityViews() {
			views[name][v.NodeID+"/"+v.Topic] = string(v.ContentHash[:])
		}
	}
	sameRecord(t, decoded["a wider descriptor"], decoded["this build"])
	a, b := projected["this build"][0], projected["a wider descriptor"][0]
	if !bytes.Equal(a.Body, b.Body) || !a.Timestamp.Equal(b.Timestamp) || a.NodeID != b.NodeID || a.Partition != b.Partition {
		t.Fatalf("the cache projects different members from the two forms:\n %+v\n %+v", a, b)
	}
	if len(views["this build"]) != 1 || len(views["a wider descriptor"]) != 1 || views["this build"]["node-a/member"] == "" ||
		views["this build"]["node-a/member"] != views["a wider descriptor"]["node-a/member"] {
		t.Fatalf("the cache's identity views differ between the two forms: %v vs %v", views["this build"], views["a wider descriptor"])
	}

	// The unknown fields change no version: moving the known version
	// fields in the wider envelope is still refused.
	moved := compatibleEnvelope(t, meta, withUnknownFields(canonical))
	moved = append(moved, protowire.AppendTag(nil, 4, protowire.VarintType)...)
	moved = protowire.AppendVarint(moved, 2)
	if _, _, err := unmarshalEnvelope(p, moved); !errors.Is(err, ErrEnvelopeVersion) {
		t.Fatalf("a wider envelope at schema 2: err = %v, want ErrEnvelopeVersion", err)
	}
}

// TestBytesWithoutRecordAuthorityGainNone: through a signed-topic cache, a
// record whose body was changed after signing, a record stripped of its
// signature and a record re-signed by another key are each refused and leave
// the cache empty; an envelope carrying record bytes that are not a record
// beside a valid one applies the valid one alone; and an initiator refuses a
// peer frame past the payload ceiling after its header, reading no body and
// applying nothing.
func TestBytesWithoutRecordAuthorityGainNone(t *testing.T) {
	p := testProfile(t)
	good := signedMember(t, "node-good")

	tampered := good
	tampered.Body = append([]byte(nil), good.Body...)
	tampered.Body[len(tampered.Body)-2] ^= 0x01 // the record's closing brace moves; a different body
	unsigned := good
	unsigned.Signature = nil
	_, otherKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	resigned := good
	lad.SignRecord(&resigned, otherKey) // a different author claims the same record
	resigned.AuthorPubKey = good.AuthorPubKey

	for name, rec := range map[string]lad.Record{"body changed after signing": tampered, "signature stripped": unsigned, "signed by another key": resigned} {
		c := signedCache()
		encoded, err := encodeRecord(rec)
		if err != nil {
			t.Fatal(err)
		}
		if err := newLADStateStore(c).Apply(encoded); !errors.Is(err, lad.ErrSignatureInvalid) {
			t.Fatalf("%s: Apply err = %v, want ErrSignatureInvalid", name, err)
		}
		if len(c.Dump()) != 0 {
			t.Fatalf("%s: the cache holds %d records", name, len(c.Dump()))
		}
		// The same through the initiator's path.
		meta, records, err := unmarshalEnvelope(p, compatibleEnvelope(t, &ExchangeMeta{}, withUnknownFields(encoded)))
		if err != nil || meta == nil || len(records) != 1 {
			t.Fatalf("%s: unmarshalEnvelope %d records, %v", name, len(records), err)
		}
		if err := c.Apply(records[0]); !errors.Is(err, lad.ErrSignatureInvalid) {
			t.Fatalf("%s: cache.Apply err = %v, want ErrSignatureInvalid", name, err)
		}
		if len(c.Dump()) != 0 {
			t.Fatalf("%s: the cache holds %d records after the initiator path", name, len(c.Dump()))
		}
	}

	// Bytes that are not a record, beside a record.
	goodBytes, err := encodeRecord(good)
	if err != nil {
		t.Fatal(err)
	}
	opaque := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x00}
	_, records, err := unmarshalEnvelope(p, compatibleEnvelope(t, &ExchangeMeta{}, opaque, withUnknownFields(goodBytes), opaque))
	if err != nil || len(records) != 1 || records[0].NodeID != "node-good" {
		t.Fatalf("opaque bytes beside a record: %d records, %v", len(records), err)
	}
	c := signedCache()
	if err := c.Apply(records[0]); err != nil {
		t.Fatalf("the valid record beside opaque bytes: %v", err)
	}
	if got := memberKeys(c); len(got) != 1 || !got["t/node-good"] {
		t.Fatalf("cache members %v", got)
	}
	if _, _, err := unmarshalEnvelope(p, compatibleEnvelope(t, &ExchangeMeta{}, opaque, opaque)); !errors.Is(err, ErrAllRecordsCorrupt) {
		t.Fatalf("an envelope of only opaque bytes: err = %v, want ErrAllRecordsCorrupt", err)
	}

	// A peer frame past the payload ceiling.
	c = signedCache()
	near, far := net.Pipe()
	read := make(chan int, 1)
	go func() {
		var hdr [gossipHeaderSize]byte
		if _, err := io.ReadFull(far, hdr[:]); err != nil {
			read <- -1
			return
		}
		if _, err := io.CopyN(io.Discard, far, int64(binary.BigEndian.Uint32(hdr[2:6]))); err != nil {
			read <- -1
			return
		}
		var reply [gossipHeaderSize]byte
		binary.BigEndian.PutUint16(reply[0:2], gossipMagic)
		binary.BigEndian.PutUint32(reply[2:6], maxGossipPayload+1)
		if _, err := far.Write(reply[:]); err != nil {
			read <- -1
			return
		}
		// Count what the initiator reads of the body before its conn closes.
		n, _ := io.Copy(io.Discard, far)
		read <- int(n)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = GossipOverConn(ctx, near, c, p, &ExchangeMeta{})
	_ = near.Close()
	if err == nil || !bytes.Contains([]byte(err.Error()), []byte("payload too large")) {
		t.Fatalf("a frame of %d bytes: err = %v, want the size refusal", maxGossipPayload+1, err)
	}
	_ = far.Close()
	select {
	case n := <-read:
		if n != 0 {
			t.Fatalf("the initiator read %d body bytes of a refused frame (-1: the peer's own I/O failed)", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("the peer did not finish")
	}
	if len(c.Dump()) != 0 {
		t.Fatalf("the cache holds %d records after a refused frame", len(c.Dump()))
	}
}

// TestAWiderEnvelopeStillRefusesAnUnsupportedPair: the unknown fields of a
// wider descriptor do not open a path past the version check; each
// unsupported pair is refused before any record decodes, through both
// decoders.
func TestAWiderEnvelopeStillRefusesAnUnsupportedPair(t *testing.T) {
	p := testProfile(t)
	rec, err := encodeRecord(signedMember(t, "node-v"))
	if err != nil {
		t.Fatal(err)
	}
	for _, pair := range unsupportedPairs {
		var env []byte
		env = protowire.AppendTag(env, 2, protowire.BytesType)
		env = protowire.AppendBytes(env, withUnknownFields(rec))
		if pair[0] != 0 {
			env = protowire.AppendTag(env, 3, protowire.VarintType)
			env = protowire.AppendVarint(env, uint64(pair[0]))
		}
		if pair[1] != 0 {
			env = protowire.AppendTag(env, 4, protowire.VarintType)
			env = protowire.AppendVarint(env, uint64(pair[1]))
		}
		env = withUnknownFields(env)
		if _, _, err := unmarshalEnvelope(p, env); !errors.Is(err, ErrEnvelopeVersion) {
			t.Fatalf("pair %v: unmarshalEnvelope err = %v", pair, err)
		}
		if _, err := newLADG1Codec(p).DecodeExchange(env); !errors.Is(err, ErrEnvelopeVersion) {
			t.Fatalf("pair %v: DecodeExchange err = %v", pair, err)
		}
	}
	var _ gossippb.GossipEnvelope // the descriptor the version fields are read from
}
