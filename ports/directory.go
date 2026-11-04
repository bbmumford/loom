/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package ports

import (
	"context"
	"sort"
	"strings"
)

// Member is the typed membership projection of a fleet.peer record.
type Member struct {
	NodeID      NodeID
	ServiceName string
	// Roles are the node's service roles ("auth", "identity", …) — NOT
	// swarm.Role — returned in LEXICOGRAPHIC ORDER.
	//
	// The ordering is part of the contract, stated here for the same reason
	// Reach below states "priority-ordered": an implementation that returned
	// map order would make Member undiffable and unhashable, and a consumer
	// indexing Roles[0] would get a different answer per implementation.
	// SwarmDirectory sorts once when it projects a record, before storing;
	// LADDirectory stores nothing and sorts each projection it returns.
	Roles          []string
	PartitionID    string
	Region         string
	LastSeenUnixMs int64
	Tombstoned     bool

	// Attrs is the producer's open metadata map, verbatim.
	//
	// The typed fields above are a FIXED projection of it, and four measured
	// call sites read keys the projection does not carry (`http_port`, and
	// service name under BOTH spellings — the swarm→LAD bridge writes
	// "serviceName", reach-published records document "service_name").
	// Without this, a consumer needing any other key must bypass the port
	// entirely.
	//
	// Additive: never a substitute for the typed fields, which stay the
	// primary surface. Nil when the producer carries no metadata.
	Attrs map[string]string
}

// ReachAddress is a typed dial candidate for a node.
//
// It carries BOTH the normalised form (Protocol/Address) and the producer's
// raw form (RawProtocol/Host/Port). The raw fields are ADDITIVE — they never
// replace the normalised ones.
//
// 🛑 WHY THE RAW FORM EXISTS, because "we already have Address" is the
// tempting reading: the normalisation RENAMES transports. The reach layer
// calls the Noise transport "udp"; the mesh address table calls it
// "noise-udp". A consumer that filters on the producer's name —
// `forwarder.go` selects 6PN dial candidates with `Proto != "udp"` — matches
// NOTHING against the normalised value, and the failure is silent: direct
// dialling stops and every peer falls back to relay with no error. Address
// likewise joins Host:Port, which a consumer needing them apart must
// re-split.
//
// Scope is bounded by MEASUREMENT, not anticipation: these three fields exist
// because 8 measured call sites needed them. A future raw field needs a
// measured consumer.
type ReachAddress struct {
	// Protocol is the NORMALISED transport name: "noise-udp" > "ws" > "grpc" >
	// "http", best first.
	//
	// 🛑 THIS VOCABULARY IS NOT THE WIRE'S, AND SAYING SO IS THE POINT.
	// It is what SwarmDirectory derives from the
	// Address_Transport enum. The reach layer's records — already written, and
	// data at rest is a compatibility event with no publish event — carry a
	// DIFFERENT vocabulary, measured across all 16 module roots:
	//
	//	wire (RawProtocol):  "udp" x13 · "tls" x2 · "wss" x1 · "ws" ZERO
	//	normalised (here):   "noise-udp"    "ws"     "grpc"    "http"
	//
	// LADDirectory bridges the two with an explicitly-labelled COMPATIBILITY
	// SHIM (normaliseReachProto). The shim exists because the stored records
	// cannot be rewritten — never "fix" the producer to emit this vocabulary:
	// old readers would meet new strings and drop them into the unknown rank,
	// trading a fixed consumer bug for an unrecallable fleet-wide split.
	//
	// Match RawProtocol when you mean the producer's name; match Protocol only
	// when you mean the normalised one.
	Protocol string
	Address  string
	Scope    string // "public" / "private" / … (pex_address scope inference)

	// Priority ranks this address against the node's others. LOWER SORTS
	// FIRST — 0 is the best candidate — and both implementations return
	// their slice already sorted ascending.
	//
	// 🛑 THE DIRECTION IS STATED HERE BECAUSE THE SIBLING SCALE IS INVERTED.
	// node.transportPriority scores noise-udp=4 and sorts with `>`; this one
	// scores noise-udp=0 and sorts with `<`. Both are internally consistent
	// and they are in different packages, so nothing catches a reader who
	// carries the intuition across — and the failure is a silently reversed
	// dial preference, not an error.
	//
	// Unknown transports take a deliberately worst rank rather than 0, so an
	// unrecognised address can never outrank a known one.
	Priority int
	NATClass string // RFC 5780 class; absent/malformed → Unknown (forward-compat)

	// RawProtocol is the PRODUCER's transport name, before normalisation —
	// "udp" from the reach layer where Protocol says "noise-udp". Compare
	// against this, never against Protocol, when matching a producer's own
	// vocabulary.
	//
	// For swarm-fed directories the producer's name IS the normalised one
	// (the transport is an enum), so RawProtocol == Protocol there. That
	// equality is correct, not a projection bug.
	RawProtocol string
	// Host and Port are the unjoined form of Address, for consumers that need
	// them apart (6PN address parsing, per-host comparison).
	Host string
	Port int
}

// LatencySample is one predicted/observed RTT observation between two nodes.
// Multiple observations per node require composite record keys (Record.Key).
type LatencySample struct {
	From, To         NodeID
	RTTMs            float64
	ObservedAtUnixMs int64
}

// HandlerAdvert is a typed RPC-handler advertisement (who serves a name).
type HandlerAdvert struct {
	Name   string // FQN, e.g. "example.<domain>.<Op>"
	NodeID NodeID
	Roles  []string
	// RequiredScopes is what the advertising node asks of a caller before it
	// runs the operation, as that node's record names it. Empty when the record
	// names nothing, which is a different statement from naming "open".
	// JudgeHandlerAdverts compares it across the adverts of one Name.
	RequiredScopes []string
}

// HandlerAnswerers is what JudgeHandlerAdverts finds across the adverts of one
// operation.
type HandlerAnswerers struct {
	Name string
	// Answerers are the nodes a call may reach, ordered by NodeID. Empty when
	// the adverts disagree, because nothing here can say which of them is right.
	Answerers []NodeID
	// Undeclared are the nodes whose advert names no requirement while another
	// advert of the operation names one. They are not answerers.
	Undeclared []NodeID
	// Requirements holds each distinct requirement with the nodes naming it.
	Requirements map[string][]NodeID
}

// Disagree reports whether the adverts name different requirements.
func (a HandlerAnswerers) Disagree() bool { return len(a.Requirements) > 1 }

// JudgeHandlerAdverts compares what the adverts of one operation require of a
// caller. Several adverts naming one requirement are all answerers, however many
// there are: that is how the mesh carries load and survives a node going away.
//
// A router picks among answerers without looking at what each requires, so an
// advert asking for less than another would take the call from the caller that
// could not have passed the stricter one. So adverts naming different
// requirements yield no answerers, and an advert naming none while another names
// one is left out. When no advert names one there is nothing to compare, and all
// of them stand.
//
// node.RoleTable.Answerers runs it on the peers routing picks from, so a call is
// refused before it leaves, and the endpoint spine's fleet audit runs it on the
// results of HandlersByName to report the same finding.
func JudgeHandlerAdverts(adverts []HandlerAdvert) HandlerAnswerers {
	out := HandlerAnswerers{}
	named := map[string][]NodeID{}
	var bare []NodeID
	for _, a := range adverts {
		out.Name = a.Name
		if len(a.RequiredScopes) == 0 {
			bare = append(bare, a.NodeID)
			continue
		}
		scopes := append([]string(nil), a.RequiredScopes...)
		sort.Strings(scopes)
		key := strings.Join(scopes, "+")
		named[key] = append(named[key], a.NodeID)
	}
	if len(named) == 0 {
		out.Answerers = sortedNodeIDs(bare)
		return out
	}
	out.Undeclared = sortedNodeIDs(bare)
	out.Requirements = make(map[string][]NodeID, len(named))
	for key, nodes := range named {
		out.Requirements[key] = sortedNodeIDs(nodes)
		if len(named) == 1 {
			out.Answerers = out.Requirements[key]
		}
	}
	return out
}

func sortedNodeIDs(ids []NodeID) []NodeID {
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// RoleAdvert is one node's service-role advertisement.
//
// Roles are lexicographic, matching Member.Roles.
type RoleAdvert struct {
	NodeID NodeID
	Roles  []string
}

// RoleEnumerator is an OPTIONAL capability of a LiveDirectory: enumerate every
// node advertising a role, rather than answering about a role you already name.
//
// 🛑 WHY THIS IS A SEPARATE INTERFACE AND NOT A METHOD ON LiveDirectory.
// LiveDirectory is exported on a published module. Adding a method to a
// published INTERFACE breaks every external implementer at compile time —
// additive on a struct, not on an interface. The capability is
// therefore offered as a probe: `if re, ok := dir.(ports.RoleEnumerator); ok`.
// Both in-tree implementations satisfy it.
//
// 🔑 WHY IT CANNOT BE SERVED BY Members() + a filter, which is the tempting
// substitution: Members projects MEMBER records (plus reach-synthesised
// entries), so a node that has published a ROLE record but no member record
// is absent from it. Counting members-with-roles is therefore a DIFFERENT
// QUANTITY from counting role advertisements, and the difference is silent —
// which is why node/runtime.go's mesh-status role count probes this interface
// and reports 0 when it is absent, rather than approximating through Members.
//
// NodesByRole answers "who has role X"; this answers "who has any role".
type RoleEnumerator interface {
	// RoleAdverts returns every role advertisement in a partition, one entry per
	// advertising node. Order is by NodeID so the result is comparable.
	RoleAdverts(ctx context.Context, partition Partition) ([]RoleAdvert, error)
}

// DirectorySnapshot is a deterministic point-in-time view: two nodes with the
// same accepted-record set produce the same Fingerprint. Used for shadow
// parity comparison and anchor snapshot input.
type DirectorySnapshot struct {
	Watermark   Watermark
	Fingerprint [32]byte
	Records     []Record
}

// Partition scopes a directory query.
//
// 🛑 IT IS A LITERAL BUCKET KEY, NOT A WILDCARD — measured against ladcache:
// a record stored with Partition "p1" returns 0 from a query for "" and 1
// from a query for "p1". The empty partition selects records stored with
// an empty partition, nothing more.
//
// This parameter exists because the substrate keys every read by it. Most of
// loom's partition-scoped cache reads pass "", the bucket loom itself writes;
// passing it explicitly keeps that choice visible at each call site.
type Partition string

// LiveDirectory is the single typed read surface for live replicated state,
// so a consumer need not read a DirectoryCache or Ledger directly.
//
// Implementations: LADDirectory (directory/ladlive.go), an adapter over the
// LAD DirectoryCache; SwarmDirectory (directory/swarmlive.go), immutable
// projections fed by one accepted-record stream. Records surface here only
// AFTER TrustPolicy authorization (before the store for Swarm; before the
// projection for the shadow directory).
//
// "Swarm has a record map" is not parity: RecordsByTopic is an escape hatch
// for KeyOps/Quorum consumers, not a substitute for the typed projections.
//
// TODO: every LAD typed query/index, liveness override, ACL,
// changed-since/fingerprint/bucket view, and anchor-snapshot input must map
// explicitly to a method here or to DurableJournal before the Ledger and
// Whisper imports can be dropped.
type LiveDirectory interface {
	// Members returns the live membership projection for one partition
	// (tombstoned/TTL-expired entries excluded unless still within retention
	// for observers).
	Members(ctx context.Context, partition Partition) ([]Member, error)

	// Member returns one node's membership entry; ok=false when absent.
	Member(ctx context.Context, partition Partition, id NodeID) (m Member, ok bool, err error)

	// NodesByRole returns nodes currently advertising a service role.
	// Advertisement is not authorization — recipients of anything sensitive
	// must additionally pass TrustPolicy.
	NodesByRole(ctx context.Context, partition Partition, role string) ([]NodeID, error)

	// Reach returns priority-ordered dial candidates for a node.
	Reach(ctx context.Context, partition Partition, id NodeID) ([]ReachAddress, error)

	// Latency takes NO partition, deliberately: the substrate indexes latency by
	// observing node only (ladcache.Latency(fromNode)), and adding a parameter
	// the layer beneath does not use would be the same error as hiding one it
	// does — a port promising a distinction that does not exist. The same
	// reasoning keeps RecordsByTopic, OverrideLiveness, Snapshot, Fingerprint
	// and Subscribe partition-free.
	//
	// Latency returns the freshest RTT observation between two nodes;
	// ok=false when no observation exists. Dropping this feed silently
	// reverts route/dispatch ordering to grade-only.
	Latency(ctx context.Context, from, to NodeID) (s LatencySample, ok bool, err error)

	// HandlersByName returns the nodes advertising an RPC handler FQN, with what
	// each advert requires of a caller. The endpoint spine's fleet audit reads it;
	// rpc_forward target discovery reads handler adverts through RoleTable, not
	// through this port, because that table is what it routes on.
	HandlersByName(ctx context.Context, partition Partition, name string) ([]HandlerAdvert, error)

	// RecordsByTopic returns the accepted records of one topic (KeyOps,
	// Quorum, and other consumers without a typed projection yet).
	RecordsByTopic(ctx context.Context, topic Topic) ([]Record, error)

	// OverrideLiveness applies the live-session liveness override (the
	// connection reporter marks a directly-connected peer alive regardless
	// of gossip staleness). ttlMs bounds the override; 0 clears it.
	OverrideLiveness(id NodeID, alive bool, ttlMs int64)

	// Snapshot returns a deterministic snapshot for parity comparison and
	// signed-anchor input generation.
	Snapshot(ctx context.Context) (DirectorySnapshot, error)

	// Fingerprint returns the deterministic digest of the current accepted
	// state without materializing records.
	Fingerprint(ctx context.Context) ([32]byte, error)

	// Subscribe streams accepted records for the given topics from a
	// watermark. History→live handoff is lossless: subscribe-before-history
	// with dedup (never query-then-subscribe — the gap loses records).
	// Slow consumers must be resumable from their last watermark, never
	// silently dropped. Cancel ctx to unsubscribe.
	Subscribe(ctx context.Context, topics []Topic, from Watermark) (<-chan Record, error)
}
