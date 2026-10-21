/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package gossip

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	lad "github.com/bbmumford/ledger"
	"github.com/bbmumford/ledger/cache"
	gossippb "github.com/bbmumford/loom/core/directory/gossip/pb"
	"github.com/bbmumford/whisper"
	"google.golang.org/protobuf/proto"
)

// chosenSet is the set two peers offering the given entries choose.
func chosenSet(t *testing.T, a, b aether.WireVersions) aether.NegotiatedWireVersions {
	t.Helper()
	set, err := aether.ChooseWireVersions(a, b)
	if err != nil {
		t.Fatalf("ChooseWireVersions: %v", err)
	}
	return set
}

// testProfile is the Profile two peers of this build negotiate.
func testProfile(t *testing.T) Profile {
	t.Helper()
	p, err := NegotiatedProfile(chosenSet(t, WireVersions(), WireVersions()))
	if err != nil {
		t.Fatalf("NegotiatedProfile: %v", err)
	}
	return p
}

// The package offers gossip.codec 1 and gossip.reconcile 1, both required, and
// two peers of this build select codec 1 and the 64-bucket profile bounded to
// [1, 256].
func TestNegotiatedProfileReadsTheSessionsChoice(t *testing.T) {
	offer := WireVersions()
	if len(offer) != 2 {
		t.Fatalf("offer has %d entries, want gossip.codec and gossip.reconcile", len(offer))
	}
	for _, e := range offer {
		if !e.Required {
			t.Errorf("%s is optional; a peer without it would hold a session gossip cannot run on", e.Name)
		}
	}
	if err := offer.Validate(); err != nil {
		t.Fatalf("offer is not canonical: %v", err)
	}
	got := testProfile(t)
	want := Profile{Codec: 1, Reconcile: ReconcileProfile{Version: 1, Buckets: 64, MinBuckets: 1, MaxBuckets: 256}}
	if got != want {
		t.Fatalf("profile = %+v, want %+v", got, want)
	}
}

// A chosen set that lacks either entry, or names a version this build does not
// speak, selects no profile, and the refusal names the entry.
func TestNegotiatedProfileRefusesAnAbsentOrUnknownEntry(t *testing.T) {
	only := func(name string, v uint16) aether.WireVersions {
		var out aether.WireVersions
		for _, e := range WireVersions() {
			if e.Name == name {
				e.Versions = []uint16{v}
			}
			out = append(out, e)
		}
		return out
	}
	for _, tc := range []struct {
		why    string
		set    aether.NegotiatedWireVersions
		name   string
		chosen bool
	}{
		{"no entries", aether.NegotiatedWireVersions{}, WireVersionCodec, false},
		{"codec only", chosenSet(t, WireVersions()[:1], WireVersions()[:1]), WireVersionReconcile, false},
		{"codec 2", chosenSet(t, only(WireVersionCodec, 2), only(WireVersionCodec, 2)), WireVersionCodec, true},
		{"reconcile 2", chosenSet(t, only(WireVersionReconcile, 2), only(WireVersionReconcile, 2)), WireVersionReconcile, true},
	} {
		_, err := NegotiatedProfile(tc.set)
		var pe *WireProfileError
		if !errors.As(err, &pe) || !errors.Is(err, ErrWireProfile) {
			t.Errorf("%s: err = %v, want a *WireProfileError", tc.why, err)
			continue
		}
		if pe.Name != tc.name || pe.Chosen != tc.chosen {
			t.Errorf("%s: refusal names %s chosen=%v, want %s chosen=%v", tc.why, pe.Name, pe.Chosen, tc.name, tc.chosen)
		}
	}
}

// memberRecordBytes is one member record in the LADRecord encoding the
// envelope carries.
func memberRecordBytes(t *testing.T, node string) []byte {
	t.Helper()
	body, err := json.Marshal(lad.MemberRecord{Partition: "t", NodeID: node, CreatedAt: seedTime})
	if err != nil {
		t.Fatal(err)
	}
	b, err := proto.Marshal(recordToProto(lad.Record{
		Topic: lad.TopicMember, Partition: "t", NodeID: node, Timestamp: seedTime, Body: body,
	}))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// envelopeAt encodes an envelope carrying the given version pair and records.
func envelopeAt(t *testing.T, codec, schema uint32, records ...[]byte) []byte {
	t.Helper()
	b, err := proto.Marshal(&gossippb.GossipEnvelope{
		CodecVersion: codec, SchemaVersion: schema, Records: records,
		Meta: &gossippb.ExchangeMeta{CacheFingerprint: 7},
	})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// unsupportedPairs are version pairs codec 1 does not write: the zero pair of
// an envelope that names no version, and each half moved on its own.
var unsupportedPairs = [][2]uint32{{0, 0}, {2, 1}, {1, 2}, {1, 0}, {0, 1}, {2, 2}}

// An envelope at a pair the negotiated codec does not write is refused, typed,
// before any record decodes: records that are not LADRecords give the version
// refusal and not the all-records-corrupt error, and both the initiator's
// decoder and the responder's codec return no record and no meta.
func TestAnEnvelopeAtAnUnsupportedVersionIsRefusedBeforeAnyRecordDecodes(t *testing.T) {
	p := testProfile(t)
	codec := newLADG1Codec(p)
	garbage := []byte{0xff, 0xff, 0xff}
	for _, pair := range unsupportedPairs {
		for _, records := range [][][]byte{{garbage}, {memberRecordBytes(t, "node-x")}} {
			env := envelopeAt(t, pair[0], pair[1], records...)

			meta, recs, err := unmarshalEnvelope(p, env)
			var ve *EnvelopeVersionError
			if !errors.As(err, &ve) || !errors.Is(err, ErrEnvelopeVersion) {
				t.Fatalf("pair %v: unmarshalEnvelope err = %v, want an *EnvelopeVersionError", pair, err)
			}
			if ve.Codec != pair[0] || ve.Schema != pair[1] || ve.WantCodec != 1 || ve.WantSchema != 1 {
				t.Fatalf("pair %v: refusal names %+v", pair, ve)
			}
			if meta != nil || recs != nil {
				t.Fatalf("pair %v: unmarshalEnvelope returned meta %v and %d records with its refusal", pair, meta, len(recs))
			}

			dec, err := codec.DecodeExchange(env)
			if !errors.Is(err, ErrEnvelopeVersion) {
				t.Fatalf("pair %v: DecodeExchange err = %v, want ErrEnvelopeVersion", pair, err)
			}
			if dec.Meta != nil || dec.Records != nil || dec.SeenNodeIDs != nil {
				t.Fatalf("pair %v: DecodeExchange returned %+v with its refusal", pair, dec)
			}
		}
	}

	// The supported pair decodes, so the refusals above are about the pair.
	env := envelopeAt(t, 1, 1, memberRecordBytes(t, "node-x"))
	if _, recs, err := unmarshalEnvelope(p, env); err != nil || len(recs) != 1 {
		t.Fatalf("pair (1,1): %d records, err %v; want one record", len(recs), err)
	}
	if dec, err := codec.DecodeExchange(env); err != nil || len(dec.Records) != 1 {
		t.Fatalf("pair (1,1): DecodeExchange %d records, err %v; want one record", len(dec.Records), err)
	}
}

// Both encoders stamp the pair the negotiated codec writes, so a peer of this
// build reads what this build sends.
func TestEnvelopesCarryTheNegotiatedVersions(t *testing.T) {
	p := testProfile(t)
	a, err := marshalEnvelope(p, &ExchangeMeta{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := newLADG1Codec(p).EncodeExchange(nil, &whisper.ExchangeMeta{})
	if err != nil {
		t.Fatal(err)
	}
	for name, raw := range map[string][]byte{"marshalEnvelope": a, "EncodeExchange": b} {
		var env gossippb.GossipEnvelope
		if err := proto.Unmarshal(raw, &env); err != nil {
			t.Fatal(err)
		}
		if env.CodecVersion != 1 || env.SchemaVersion != 1 {
			t.Errorf("%s wrote (%d,%d), want (1,1)", name, env.CodecVersion, env.SchemaVersion)
		}
	}
}

// writeG1 writes one G1 frame carrying body. Its callers run it beside the
// read of the other end and ignore an error from a conn the test has closed.
func writeG1(conn net.Conn, body []byte) error {
	hdr := make([]byte, gossipHeaderSize)
	binary.BigEndian.PutUint16(hdr[0:2], gossipMagic)
	binary.BigEndian.PutUint32(hdr[2:6], uint32(len(body)))
	_, err := conn.Write(append(hdr, body...))
	return err
}

// The initiator applies nothing from a reply at an unsupported version: a
// responder answering with a valid record in a codec-2 envelope leaves the
// initiator's cache without that record, and GossipOverConn returns the typed
// refusal.
func TestGossipOverConnAppliesNothingFromAnEnvelopeAtAnUnsupportedVersion(t *testing.T) {
	p := testProfile(t)
	for _, pair := range unsupportedPairs {
		c := cache.NewDirectoryCache()
		hostile := envelopeAt(t, pair[0], pair[1], memberRecordBytes(t, "node-hostile"))
		near, far := net.Pipe()
		go func() {
			var hdr [gossipHeaderSize]byte
			if _, err := io.ReadFull(far, hdr[:]); err != nil {
				return
			}
			if _, err := io.CopyN(io.Discard, far, int64(binary.BigEndian.Uint32(hdr[2:6]))); err != nil {
				return
			}
			_ = writeG1(far, hostile)
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := GossipOverConn(ctx, near, c, p, &ExchangeMeta{})
		cancel()
		_ = near.Close()
		_ = far.Close()
		if !errors.Is(err, ErrEnvelopeVersion) {
			t.Fatalf("pair %v: GossipOverConn err = %v, want ErrEnvelopeVersion", pair, err)
		}
		if memberKeys(c)["t/node-hostile"] {
			t.Fatalf("pair %v: the initiator applied a record from a refused envelope", pair)
		}
	}
}

// The responder applies nothing from a request at an unsupported version: it
// reads a codec-2 request carrying one record, then a codec-1 request carrying
// another, and answers the second. Its cache then holds the second record and
// not the first.
func TestTheResponderAppliesNothingFromAnEnvelopeAtAnUnsupportedVersion(t *testing.T) {
	p := testProfile(t)
	resp := cache.NewDirectoryCache()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	gInit, gResp := net.Pipe()
	rInit, rResp := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		runGossipResponderLoop(ctx, gResp, rResp, resp, p, nil, nil, whisper.NewDeltaTracker(), "node-init")
	}()

	refused := envelopeAt(t, 2, 1, memberRecordBytes(t, "node-refused"))
	accepted := envelopeAt(t, 1, 1, memberRecordBytes(t, "node-accepted"))
	// The two requests are written beside the read below, so a responder that
	// answered the first would not hold the test on a pipe nobody reads.
	go func() {
		if writeG1(gInit, refused) == nil {
			_ = writeG1(gInit, accepted)
		}
	}()
	var hdr [gossipHeaderSize]byte
	_ = gInit.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.ReadFull(gInit, hdr[:]); err != nil {
		t.Fatalf("read the responder's answer: %v", err)
	}
	body := make([]byte, binary.BigEndian.Uint32(hdr[2:6]))
	if _, err := io.ReadFull(gInit, body); err != nil {
		t.Fatalf("read the responder's answer body: %v", err)
	}
	_ = gInit.Close()
	_ = rInit.Close()
	<-done

	got := memberKeys(resp)
	if got["t/node-refused"] {
		t.Fatal("the responder applied a record from an envelope at an unsupported version")
	}
	if !got["t/node-accepted"] {
		t.Fatalf("premise wrong: the supported request was not applied either (cache %v)", keysSorted(got))
	}
}

// bucketOf returns the index of the bucket rec falls in when c is split into n
// buckets, read from c's own partition.
func bucketOf(t *testing.T, c *cache.DirectoryCache, n uint16, node string) int {
	t.Helper()
	for i := 0; i < int(n); i++ {
		want := make([]bool, n)
		want[i] = true
		for _, rec := range c.DumpBuckets(n, want) {
			if rec.NodeID == node {
				return i
			}
		}
	}
	t.Fatalf("%s is in no bucket of %d", node, n)
	return -1
}

// A GB exchange compares the requester's buckets even when the responder's
// own profile names another count. The requester sends 16 buckets while the
// responder runs the 64-bucket profile: the responder answers at 16, arms the
// diverging set over the 16-bucket partition, and the follow-up G1 carries
// exactly the records of the one 16-partition bucket the two caches differ in.
func TestAGBExchangeComparesTheRequestersBuckets(t *testing.T) {
	p := testProfile(t)
	if p.Reconcile.Buckets == 16 {
		t.Fatal("premise wrong: the responder's profile already uses 16 buckets")
	}
	resp := cache.NewDirectoryCache()
	init := cache.NewDirectoryCache()
	for i := 0; i < 40; i++ {
		node := "node-" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		seedMember(t, resp, "t", node)
		seedMember(t, init, "t", node)
	}
	seedMember(t, resp, "t", "node-only-responder")
	const n = 16
	diverging := bucketOf(t, resp, n, "node-only-responder")
	want := make([]bool, n)
	want[diverging] = true
	expect := map[string]bool{}
	for _, rec := range resp.DumpBuckets(n, want) {
		expect[rec.Partition+"/"+rec.NodeID] = true
	}
	if len(expect) >= len(memberKeys(resp)) {
		t.Fatal("premise wrong: one bucket holds every record, so scoping would prove nothing")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	gInit, gResp := net.Pipe()
	rInit, rResp := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		runGossipResponderLoop(ctx, gResp, rResp, resp, p, nil, nil, whisper.NewDeltaTracker(), "node-init")
	}()
	req := p.Reconcile
	req.Buckets = n
	_ = gInit.SetDeadline(time.Now().Add(5 * time.Second))
	if err := escalateBucketDigest(gInit, init, req); err != nil {
		t.Fatalf("GB at %d buckets against a %d-bucket responder: %v", n, p.Reconcile.Buckets, err)
	}
	// The scoped follow-up: the initiator's request carries nothing new, so
	// what it applies is exactly what the responder served.
	before := memberKeys(init)
	res, err := GossipOverConn(ctx, gInit, init, p, &ExchangeMeta{}, time.Now())
	_ = gInit.Close()
	_ = rInit.Close()
	<-done
	if err != nil {
		t.Fatalf("scoped follow-up: %v", err)
	}
	if res.RecordsApplied == 0 || !memberKeys(init)["t/node-only-responder"] {
		t.Fatal("the follow-up did not carry the diverging record")
	}
	if got := len(memberKeys(init)) - len(before); got != 1 {
		t.Fatalf("the follow-up added %d records, want the one diverging record", got)
	}
	if res.RecordsApplied != len(expect) {
		t.Fatalf("the follow-up carried %d records, want the %d of 16-bucket %d", res.RecordsApplied, len(expect), diverging)
	}
}

// The requester refuses a reply at a count other than the one it sent, and
// sends no request at a count its profile does not admit.
func TestTheGBRequesterRefusesAnotherCount(t *testing.T) {
	p := testProfile(t)
	c := cache.NewDirectoryCache()
	seedMember(t, c, "t", "node-a")

	near, far := net.Pipe()
	go func() {
		var hdr [5]byte
		if _, err := io.ReadFull(far, hdr[:]); err != nil {
			return
		}
		if _, err := io.CopyN(io.Discard, far, int64(hdr[4])*8); err != nil {
			return
		}
		_ = whisper.WriteBucketDigest(far, c.BucketDigest(32), 0)
	}()
	_ = near.SetDeadline(time.Now().Add(5 * time.Second))
	err := escalateBucketDigest(near, c, p.Reconcile)
	_ = near.Close()
	_ = far.Close()
	var ce *whisper.DigestBucketCountError
	if !errors.As(err, &ce) || ce.Count != 32 || ce.Min != int(p.Reconcile.Buckets) || ce.Max != int(p.Reconcile.Buckets) {
		t.Fatalf("a 32-bucket reply to a %d-bucket request: err = %v, want a count refusal", p.Reconcile.Buckets, err)
	}

	for _, bad := range []ReconcileProfile{
		{Version: 1, Buckets: 0, MinBuckets: 1, MaxBuckets: 256},
		{Version: 1, Buckets: 300, MinBuckets: 1, MaxBuckets: 256},
		{Version: 1, Buckets: 64, MinBuckets: 65, MaxBuckets: 256},
	} {
		near, far := net.Pipe()
		wrote := make(chan bool, 1)
		go func() {
			var b [1]byte
			_ = far.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
			_, err := far.Read(b[:])
			wrote <- err == nil
		}()
		_ = near.SetDeadline(time.Now().Add(time.Second))
		err := escalateBucketDigest(near, c, bad)
		if !errors.Is(err, whisper.ErrDigestBucketCount) {
			t.Errorf("profile %+v: err = %v, want ErrDigestBucketCount", bad, err)
		}
		if <-wrote {
			t.Errorf("profile %+v: a request was written", bad)
		}
		_ = near.Close()
		_ = far.Close()
	}
}

// The responder answers only the bucket counts its own profile admits: a
// responder whose profile is bounded to [32, 128] gives a 16-bucket request no
// reply and arms no scoped follow-up, and answers a 32-bucket one.
func TestTheGBResponderAnswersOnlyItsProfilesCounts(t *testing.T) {
	p := testProfile(t)
	p.Reconcile.MinBuckets, p.Reconcile.MaxBuckets = 32, 128
	for _, tc := range []struct {
		n      uint16
		answer bool
	}{{16, false}, {32, true}} {
		resp := cache.NewDirectoryCache()
		seedMember(t, resp, "t", "node-a")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		gInit, gResp := net.Pipe()
		rInit, rResp := net.Pipe()
		done := make(chan struct{})
		go func() {
			defer close(done)
			runGossipResponderLoop(ctx, gResp, rResp, resp, p, nil, nil, whisper.NewDeltaTracker(), "node-init")
		}()
		req := ReconcileProfile{Version: 1, Buckets: tc.n, MinBuckets: 1, MaxBuckets: 256}
		_ = gInit.SetDeadline(time.Now().Add(time.Second))
		err := escalateBucketDigest(gInit, cache.NewDirectoryCache(), req)
		_ = gInit.Close()
		_ = rInit.Close()
		<-done
		cancel()
		if tc.answer && err != nil {
			t.Errorf("%d buckets inside [32,128]: %v", tc.n, err)
		}
		if !tc.answer && err == nil {
			t.Errorf("%d buckets outside [32,128] were answered", tc.n)
		}
	}
}
