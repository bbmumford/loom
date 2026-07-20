/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"strconv"
	"strings"

	"github.com/ORBTR/aether/nat"
)

// natClassMetaKey is the reach-record metadata key under which a fleet
// node advertises its own RFC 5780 NAT classification. Carried in the
// signed ReachRecord.Metadata map, so it flows fleet-wide alongside the
// reachability data and surfaces verbatim as lad.MemberRecord.Attrs on
// the read side — a peer's dialer can decode it without a separate
// publish path.
//
// The key is intentionally additive: a peer running older code that
// does not know "nat_class" simply ignores it, and DecodeNATBehaviour
// treats an absent key as a zero-value (Unknown) classification.
const natClassMetaKey = "nat_class"

// encodeNATBehaviour renders a NAT classification into the compact
// "<mapping>:<filtering>" form carried in reach metadata, where each
// side is the small integer enum value (e.g. "3:2" == APDM + ADF).
//
// Returns "" when the behaviour is fully unknown (both axes zero) — the
// caller should then omit the metadata key entirely rather than publish
// a meaningless "0:0", keeping records of unclassified nodes minimal and
// indistinguishable from older nodes that never advertised at all.
func encodeNATBehaviour(b nat.NATBehaviour) string {
	if b.Mapping == nat.MappingUnknown && b.Filtering == nat.FilteringUnknown {
		return ""
	}
	return strconv.Itoa(int(b.Mapping)) + ":" + strconv.Itoa(int(b.Filtering))
}

// DecodeNATBehaviour recovers a peer's advertised RFC 5780 NAT
// classification from its reach-record metadata (or, equivalently, its
// lad.MemberRecord.Attrs — reach.Metadata flows verbatim into Attrs).
//
// It is deliberately forgiving: a missing key, a nil map, a malformed
// value, or an out-of-range enum all decode to the zero-value
// nat.NATBehaviour (Mapping/FilteringUnknown). This keeps the caller's
// dial path safe against peers running older code (no key) or a future
// schema (unrecognised value) — an Unknown classification simply means
// "fall back to the normal direct-dial path".
func DecodeNATBehaviour(attrs map[string]string) nat.NATBehaviour {
	if attrs == nil {
		return nat.NATBehaviour{}
	}
	raw, ok := attrs[natClassMetaKey]
	if !ok || raw == "" {
		return nat.NATBehaviour{}
	}
	m, f, found := strings.Cut(raw, ":")
	if !found {
		return nat.NATBehaviour{}
	}
	mi, err := strconv.Atoi(m)
	if err != nil {
		return nat.NATBehaviour{}
	}
	fi, err := strconv.Atoi(f)
	if err != nil {
		return nat.NATBehaviour{}
	}
	// Reject values outside the known enum ranges — a peer advertising
	// a class this build does not understand is treated as Unknown.
	if mi < int(nat.MappingUnknown) || mi > int(nat.MappingAddressPortDependent) {
		return nat.NATBehaviour{}
	}
	if fi < int(nat.FilteringUnknown) || fi > int(nat.FilteringAddressPortDependent) {
		return nat.NATBehaviour{}
	}
	return nat.NATBehaviour{
		Mapping:   nat.MappingBehaviour(mi),
		Filtering: nat.FilteringBehaviour(fi),
	}
}
