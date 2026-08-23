/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

// Projection helpers for the transitional LAD adapter (ladlive.go): the
// LAD-record -> ports.Record mapping, the topic and watermark domains, and
// the reach-layer transport naming. Split out of ladlive.go to keep both
// files under the 500-line limit.
package directory

import (
	"encoding/json"
	"strings"

	lad "github.com/bbmumford/ledger"
	"github.com/bbmumford/loom/ports"
)

// ladProjectedTopics are the topics the LAD DirectoryCache actually keeps in
// its typed directory view.
//
// 🛑 `keyops` and `quorum` are DELIBERATELY ABSENT, and their absence is a
// measured fact about LAD, not an oversight here. LAD declares both as topic
// constants (`lad.TopicKeyOps`, `lad.TopicQuorum`) — but `applyCore`'s switch
// ends in `default: // ignore other topics for directory view`, so records on
// those topics are never stored, and `ChangesSinceHLC` has no branch for them,
// so it returns an empty slice and a nil error.
//
// ⇒ Querying them through this adapter cannot distinguish "no key rotations
// have happened" from "I cannot see key rotations at all", and the first
// reading is the dangerous one for a security consumer. RecordsByTopic returns
// ErrTopicNotProjected instead; the typed KeyOps/Quorum projections the
// cutover needs must be fed from swarm, which does carry these records.
var ladProjectedTopics = []lad.Topic{
	lad.TopicMember, lad.TopicRole, lad.TopicReach, lad.TopicLatency,
}

// ladDeclaredButUnprojected are topics LAD names but does not retain — the
// set RecordsByTopic must refuse rather than answer emptily.
var ladDeclaredButUnprojected = map[lad.Topic]bool{
	lad.TopicKeyOps: true,
	lad.TopicQuorum: true,
}

// ladTopicsFor maps a port topic onto the LAD topics that carry it. The loom
// fleet.peer topic is the member/role/reach trio; every other name is taken
// as a LAD topic directly.
func ladTopicsFor(t ports.Topic) []lad.Topic {
	if t == FleetPeerTopic {
		return []lad.Topic{lad.TopicMember, lad.TopicRole, lad.TopicReach}
	}
	if strings.HasPrefix(string(t), LatencyTopicPrefix) {
		return []lad.Topic{lad.TopicLatency}
	}
	for _, lt := range ladProjectedTopics {
		if string(lt) == string(t) {
			return []lad.Topic{lt}
		}
	}
	return nil
}

// ladToPortRecord projects one LAD record into the port envelope.
//
// 🛑 THIS FUNCTION COPIES BYTES VERBATIM AND THAT IS NOT ENOUGH TO PRESERVE
// PROVENANCE — its INPUT has already lost it. An earlier version of this
// comment claimed the projection "never re-encodes or re-signs", which is true
// of this code and false of the pipeline, and that is the more dangerous kind
// of wrong (#M-515).
//
// MEASURED against ladcache v0.0.20, the tree loom builds: the only bulk read,
// ChangesSinceHLC, SYNTHESISES records from the typed store —
// `body, _ := json.Marshal(r)` — and constructs lad.Record{Topic, TenantID,
// NodeID, Seq, Body, Timestamp} with NO Signature and NO AuthorPubKey field at
// all. So through this adapter:
//
//   - Signature and AuthorPubKey are ZERO-LENGTH. Not truncated, not stale —
//     absent. The owner's signature never reaches ports.Record.
//   - Body is a RE-MARSHAL of the typed struct, not the owner's wire bytes.
//     ⚠ In the measurement they were the SAME LENGTH (282) and different
//     bytes, so a length check passes and an equality check fails.
//   - c.GetLastReachBody is the ONLY cache API that returns the received
//     bytes, which is why that call site cannot be replaced by RecordsByTopic.
//
// ⇒ ports.Record's provenance invariant holds for SWARM-fed directories and is
// STRUCTURALLY UNAVAILABLE for LAD-fed ones. Do not verify a signature on a
// record that came through here, and do not treat a LAD Fingerprint as
// signature-covering — fingerprintRecords hashes sha256(Signature), which is
// sha256("") for every record on this path. Pinned by
// TestLADReadPathCarriesNoOwnerSignature.
func ladToPortRecord(r lad.Record) ports.Record { return r2p(r) }

func r2p(r lad.Record) ports.Record {
	topic := ports.Topic(r.Topic)
	key := ""
	switch r.Topic {
	case lad.TopicMember:
		topic, key = FleetPeerTopic, ladKeyMember
	case lad.TopicRole:
		topic, key = FleetPeerTopic, ladKeyRole
	case lad.TopicReach:
		topic, key = FleetPeerTopic, ladKeyReach
	case lad.TopicLatency:
		// One record per observed peer; the observer owns the slot and the
		// observed node is the key.
		topic = LatencyTopic(ports.NodeID(r.NodeID))
		key = latencyPeerOf(r)
	}
	out := ports.Record{
		Topic:           topic,
		NodeID:          ports.NodeID(r.NodeID),
		Key:             key,
		HLC:             ports.HLC(r.HLCTimestamp),
		Lamport:         r.LamportClock,
		Tombstone:       r.Tombstone,
		TombstoneReason: r.TombstoneReason,
		Body:            append([]byte(nil), r.Body...),
		AuthorPubKey:    append([]byte(nil), r.AuthorPubKey...),
		Signature:       append([]byte(nil), r.Signature...),
		BlobCID:         r.BlobCID,
	}
	if !r.ExpiresAt.IsZero() {
		out.ExpiresAtUnixMs = r.ExpiresAt.UnixMilli()
	}
	return out
}

// latencyPeerOf reads the observed node out of a latency body so each
// observation gets its own slot. An unreadable body yields the empty key,
// which collapses that observer's observations into one slot — visible as a
// record-count mismatch in parity rather than as silent loss.
func latencyPeerOf(r lad.Record) string {
	var lr lad.LatencyRecord
	if json.Unmarshal(r.Body, &lr) != nil {
		return ""
	}
	return lr.ToNode
}

// ladWatermark is the LAD-side ordering scalar: the record HLC when present,
// its timestamp in nanoseconds otherwise — matching ChangesSinceHLC's own
// comparison for typed topics.
func ladWatermark(r lad.Record) ports.Watermark {
	if r.HLCTimestamp != 0 {
		return ports.Watermark(r.HLCTimestamp)
	}
	if r.Timestamp.IsZero() {
		return 0
	}
	return ports.Watermark(r.Timestamp.UnixNano())
}

// normaliseReachProto maps reach-layer transport names onto the mesh address
// table's names. The reach layer calls the Noise transport "udp".
//
// 🛑 EVERY ALIAS BELOW IS A STRING A PRODUCER ACTUALLY WRITES, and the two
// added at #M-547 were the ones that mattered, because their absence failed
// SILENTLY toward "unknown transport":
//
//   - "wss"   — node/lad_reach_bridge.go:258 addressProtoToReach(WEBSOCKET).
//   - "https" — the same function for Address_HTTP, advertised by EVERY node
//     at node/swarm_integration.go:549.
//
// Both used to fall through this default and reach reachPriority as
// unrecognised, scoring 9 — BELOW every ranked transport — so the ordering
// this package documents ("noise-udp" > "ws" > "grpc" > "http") was not the
// ordering it produced. MEASURED before the fix: udp=0 ws=9 grpc=2 http=9.
//
// 🔑 The case for "ws" matched a string NO producer writes (measured across
// all 16 module roots: "udp" x13, "tls" x2, "wss" x1, "ws" ZERO), which is
// why the existing tests were green — their fixtures were written to this
// reader rather than to the wire.
//
// 🛑🛑 THIS MAPPING IS LOSSY IN A SECURITY DIMENSION — READ BEFORE EXTENDING IT
// (#R-1517 ②). "wss"->"ws" and "https"->"http" ERASE THE TLS DISTINCTION.
// That is harmless ONLY because bare "ws" and bare "http" are written ZERO
// times today, and this alias bakes that census in as a permanent assumption.
//
// ⇒ THE MOMENT ANY PRODUCER EMITS BARE "ws" OR "http", secure and insecure
// transports collapse to ONE RANK and the dialer may prefer the UNENCRYPTED
// peer. If that day comes, "wss"/"https" MUST outrank their bare forms — the
// constraint is inherited here deliberately so the next reader does not
// inherit only the shortcut. This is a merge of two security classes wearing
// the costume of a naming tidy-up.
//
// ⚠ "tls" is deliberately NOT aliased and NOT ranked, and #R-1517 ③ turned
// that into a measurement rather than a preference. MEASURED since: it is NOT
// vestigial — it has a ProtoTLS constant, live dial paths
// (node/peer_connections.go:2705 constructs one and :2712 matches it), and
// node/runtime.go:3648 grades it "B", the SAME as websocket and ABOVE grpc's
// "C". reachPriority ranks grpc=2 and leaves tls unknown at 9, so THE TWO
// RANKINGS IN THIS REPO DISAGREE about tls-vs-grpc.
//
// It is still not ranked here, because a rank asserts dialability AND a
// position, and "tls" has no Address_Transport counterpart — that pairing is
// @R's call, not a mapping decision. Routed at #M-549.
func normaliseReachProto(p string) string {
	switch strings.ToLower(p) {
	case "udp", "noise-udp":
		return "noise-udp"
	case "ws", "wss", "websocket":
		return "ws"
	case "http", "https":
		return "http"
	default:
		return strings.ToLower(p)
	}
}

// reachPriority ranks a NORMALISED transport name; lower sorts first, matching
// both LADDirectory.Reach and SwarmDirectory.Reach, which each sort ascending.
//
// 🛑 Feed this the output of normaliseReachProto, never a raw producer string.
// The default of 9 exists so an unknown transport sorts LAST, and that is also
// what makes an unnormalised input silently harmless-looking: it does not
// error, it just ranks the transport worst.
//
// ⚠ The sibling node.transportPriority has the OPPOSITE polarity (noise-udp=4,
// higher-is-better) and sorts with `>`. Same concept, same name shape, inverted
// scale, different package — do not carry an intuition from one to the other.
func reachPriority(proto string) int {
	switch proto {
	case "noise-udp":
		return 0
	case "ws":
		return 1
	case "grpc":
		return 2
	case "http":
		return 3
	default:
		return 9
	}
}

// attrServiceName reads a member's service name across BOTH conventions in
// this estate, camelCase first.
//
// 🛑 THE CAMELCASE KEY IS THE ONE THAT MATTERS AND I HAD ONLY THE OTHER
// (#M-521). The swarm→LAD bridge — the producer for every mesh member —
// writes "serviceName" (node/lad_reach_bridge.go:156,191, alongside "region"
// and comma-joined "roles"). Reading only "service_name" yields an EMPTY
// service name for every record the bridge publishes.
//
// This exact bug has now been fixed three times in this repo: help.orbtr.io
// carries two comments naming it ("HELP-01", "HELP-02" in monitoring_api.go),
// each recording that reading snake_case "always got" nothing. I made it a
// third time without looking for the sibling that had it right.
//
// Snake_case is retained as a fallback rather than deleted because it is the
// documented convention for reach-published metadata (reach/config.go:97,
// ledger/types.go:198) — both keys are genuinely in use by different
// producers, so preferring one and accepting the other is correct here, not
// defensive padding.
func attrServiceName(attrs map[string]string) string {
	if v := attrs["serviceName"]; v != "" {
		return v
	}
	return attrs["service_name"]
}

func splitAttrList(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
