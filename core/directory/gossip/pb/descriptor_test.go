package pb

import (
	"encoding/hex"
	"reflect"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// wantProtoPackage is the proto package gossip.proto declares; every message
// full name in the global protobuf registry is qualified by it.
const wantProtoPackage = "loom.mesh.gossip.v1"

// goldenEnvelope is the deterministic encoding of wireFixture. The binary
// encoding carries field numbers and wire types, never the proto package, so
// peers whose descriptors differ only in package or go_package decode each
// other's gossip frames to the same values.
const goldenEnvelope = "" +
	"0a6008091208726567696f6e2d611801221f0a066e6f64652d62120661646472" +
	"2d31180a22010b2a08726567696f6e2d622a0a0a066e6f64652d63100c32230a" +
	"066e6f64652d6412066e6f64652d651a06616464722d32220471756963280d32" +
	"010e123b0a05746f70696312066e6f64652d611a03702d312204626f64792801" +
	"30023803400148045206726561736f6e5a0105600668077201087a03636964"

// TestDescriptorPackage asserts the registered descriptor declares the neutral
// proto package and that its go_package is the import path this generated
// code compiles under, which protoc-gen-go resolves cross-file imports by.
func TestDescriptorPackage(t *testing.T) {
	fd := File_gossip_proto
	if got := string(fd.Package()); got != wantProtoPackage {
		t.Fatalf("proto package = %q, want %q", got, wantProtoPackage)
	}
	opts, ok := fd.Options().(*descriptorpb.FileOptions)
	if !ok {
		t.Fatalf("file options have type %T, want *descriptorpb.FileOptions", fd.Options())
	}
	want := reflect.TypeOf((*GossipEnvelope)(nil)).Elem().PkgPath()
	if got := opts.GetGoPackage(); got != want {
		t.Fatalf("go_package = %q, want the compiled import path %q", got, want)
	}
}

// TestWireEncodingIsFieldNumbered asserts the envelope that codec.go and
// g1_adapter.go marshal encodes to goldenEnvelope and decodes back to the
// same values.
func TestWireEncodingIsFieldNumbered(t *testing.T) {
	env := wireFixture(t)
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(env)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	if got := hex.EncodeToString(b); got != goldenEnvelope {
		t.Fatalf("envelope encoding = %s, want %s", got, goldenEnvelope)
	}
	var back GossipEnvelope
	if err := proto.Unmarshal(b, &back); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if !proto.Equal(&back, env) {
		t.Fatalf("round trip = %v, want %v", &back, env)
	}
}

// wireFixture sets every field of every gossip message to a non-zero value, so
// the golden encoding covers each field number.
func wireFixture(t *testing.T) *GossipEnvelope {
	t.Helper()
	rec, err := proto.MarshalOptions{Deterministic: true}.Marshal(&LADRecord{
		Topic:           "topic",
		NodeId:          "node-a",
		Partition:       "p-1",
		Body:            []byte("body"),
		TimestampUnix:   1,
		LamportClock:    2,
		Seq:             3,
		Tombstone:       true,
		DeletedAtUnix:   4,
		TombstoneReason: "reason",
		Signature:       []byte{0x05},
		HlcTimestamp:    6,
		ExpiresAtUnix:   7,
		AuthorPubkey:    []byte{0x08},
		BlobCid:         "cid",
	})
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}
	return &GossipEnvelope{
		Meta: &ExchangeMeta{
			CacheFingerprint:   9,
			Region:             "region-a",
			BackpressureSignal: true,
			PexEntries: []*PEXEntry{{
				NodeId:        "node-b",
				Addresses:     []string{"addr-1"},
				TimestampUnix: 10,
				Signature:     []byte{0x0b},
				Region:        "region-b",
			}},
			ConnectionCounts: map[string]int32{"node-c": 12},
			Offer: &ConnectionOffer{
				FromNodeId:    "node-d",
				ToNodeId:      "node-e",
				Addresses:     []string{"addr-2"},
				Transport:     "quic",
				TimestampUnix: 13,
				Signature:     []byte{0x0e},
			},
		},
		Records: [][]byte{rec},
	}
}
