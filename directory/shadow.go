/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package directory

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/bbmumford/loom/ports"
)

// ShadowReport is the result of one parity comparison between the
// authoritative directory and its shadow (Phase-0.5 stage 3). Mismatches
// are OBSERVABLE and fail the phase gate — the comparator never silently
// prefers one side.
type ShadowReport struct {
	Mismatches []string
	// Compared counts per-capability comparisons performed, so an
	// accidentally-empty comparison (both sides empty ≠ parity proven)
	// is visible.
	ComparedMembers  int
	ComparedRoles    int
	ComparedReach    int
	ComparedHandlers int
	// Degraded names comparison axes that could not be performed at all —
	// as opposed to axes that were performed and agreed. An axis lands here
	// when a side cannot supply the input the axis needs (today: handler-FQN
	// enumeration). Keeping it separate from Mismatches preserves the
	// distinction between "the two sides disagree" and "this was never
	// checked"; both fail the gate, for different reasons.
	Degraded []string
}

// InParity reports whether the comparison found zero mismatches AND performed
// every axis it set out to perform.
//
// A degraded axis fails the gate deliberately. The shadow phase compares the
// Swarm directory against a DIFFERENT implementation, so "the other side
// could not answer" is the expected shape of an unperformed check, not a rare
// edge — and an unperformed check reporting parity is precisely the silent
// pass §0.5.3 stage 3 forbids ("shadow mismatches are observable and fail the
// phase gate; do not silently prefer one side"). An unlisted item is
// unmeasured, not negative.
func (r *ShadowReport) InParity() bool {
	return len(r.Mismatches) == 0 && len(r.Degraded) == 0
}

func (r *ShadowReport) add(format string, args ...any) {
	r.Mismatches = append(r.Mismatches, fmt.Sprintf(format, args...))
}

// CompareDirectories runs the capability-by-capability parity check between
// the authoritative side and the shadow side: membership (+ roles, service,
// tenant), per-node reach sets, and handler adverts for every FQN either
// side knows. Ordering differences are normalized away; content differences
// are mismatches.
func CompareDirectories(ctx context.Context, authoritative, shadow ports.LiveDirectory, tenant ports.Tenant, roles []string) (*ShadowReport, error) {
	rep := &ShadowReport{}

	aMembers, err := authoritative.Members(ctx, tenant)
	if err != nil {
		return nil, fmt.Errorf("shadow: authoritative members: %w", err)
	}
	sMembers, err := shadow.Members(ctx, tenant)
	if err != nil {
		return nil, fmt.Errorf("shadow: shadow members: %w", err)
	}
	aByID := memberIndex(aMembers)
	sByID := memberIndex(sMembers)
	for id, am := range aByID {
		rep.ComparedMembers++
		sm, ok := sByID[id]
		if !ok {
			rep.add("member %s missing from shadow", short(id))
			continue
		}
		if am.ServiceName != sm.ServiceName {
			rep.add("member %s service %q vs %q", short(id), am.ServiceName, sm.ServiceName)
		}
		if am.Tenant != sm.Tenant {
			rep.add("member %s tenant %q vs %q", short(id), am.Tenant, sm.Tenant)
		}
		if !stringSetEqual(am.Roles, sm.Roles) {
			rep.add("member %s roles %v vs %v", short(id), am.Roles, sm.Roles)
		}
	}
	for id := range sByID {
		if _, ok := aByID[id]; !ok {
			rep.add("member %s present ONLY in shadow", short(id))
		}
	}

	// Role index parity.
	for _, role := range roles {
		rep.ComparedRoles++
		aNodes, err := authoritative.NodesByRole(ctx, tenant, role)
		if err != nil {
			return nil, err
		}
		sNodes, err := shadow.NodesByRole(ctx, tenant, role)
		if err != nil {
			return nil, err
		}
		if !nodeSetEqual(aNodes, sNodes) {
			rep.add("role %q nodes %v vs %v", role, shortAll(aNodes), shortAll(sNodes))
		}
	}

	// Reach parity per member.
	for id := range aByID {
		rep.ComparedReach++
		aReach, err := authoritative.Reach(ctx, tenant, id)
		if err != nil {
			return nil, err
		}
		sReach, err := shadow.Reach(ctx, tenant, id)
		if err != nil {
			return nil, err
		}
		if !reachSetEqual(aReach, sReach) {
			rep.add("reach %s: %v vs %v", short(id), aReach, sReach)
		}
	}

	// Handler-advert parity over the union of FQNs either side indexes. The
	// port itself has no FQN enumeration — HandlersByName answers about a
	// name you already hold — so the comparison set comes from the sides via
	// the optional HandlerEnumerator capability. A side that cannot enumerate
	// degrades the axis rather than shrinking it silently.
	fqns, degraded := collectHandlerFQNs(ctx, tenant, authoritative, shadow)
	rep.Degraded = append(rep.Degraded, degraded...)
	for _, fqn := range fqns {
		rep.ComparedHandlers++
		aH, err := authoritative.HandlersByName(ctx, tenant, fqn)
		if err != nil {
			return nil, err
		}
		sH, err := shadow.HandlersByName(ctx, tenant, fqn)
		if err != nil {
			return nil, err
		}
		aIDs := advertNodeIDs(aH)
		sIDs := advertNodeIDs(sH)
		if !nodeSetEqual(aIDs, sIDs) {
			rep.add("handler %q nodes %v vs %v", fqn, shortAll(aIDs), shortAll(sIDs))
		}
	}
	return rep, nil
}

// Record models. Two directories can only be compared record-for-record when
// they represent the same facts with the same records.
const (
	// RecordModelSwarm: one signed record per (topic, node, key) slot, as
	// published and gossiped by swarm.
	RecordModelSwarm = "swarm"
	// RecordModelLAD: LAD's typed records — a node's identity, roles and
	// reachability are THREE records, projected onto one loom topic under
	// distinct composite keys.
	RecordModelLAD = "lad"
)

// RecordModeler is the optional capability declaring which raw-record model a
// directory exposes. Implementations that do not declare one are treated as
// RecordModelSwarm, which is the model ports.Record was written for.
type RecordModeler interface {
	RecordModel() string
}

// ErrRecordModelMismatch is returned when a record-level comparison is asked
// of two directories that do not share a record model.
var ErrRecordModelMismatch = fmt.Errorf("directory: record models differ — record-level comparison is not meaningful")

// RecordModelOf reports a directory's declared record model.
func RecordModelOf(d ports.LiveDirectory) string {
	if m, ok := d.(RecordModeler); ok {
		return m.RecordModel()
	}
	return RecordModelSwarm
}

// CompareFingerprints is the cheap continuous check between full parity runs:
// two directories over the same accepted set must agree.
//
// 🛑 IT REFUSES A CROSS-MODEL PAIRING INSTEAD OF RETURNING false. The
// fingerprint hashes raw records, so a LAD directory and a Swarm directory
// holding IDENTICAL facts still disagree — LAD carries a node as three typed
// records where swarm carries one. Returning a plain false there would hand a
// lane a permanent, unfixable "divergence" during shadow mode, and the two
// available responses to it are both wrong: chase a divergence that does not
// exist, or weaken the comparator until it passes. Cross-implementation
// parity is asserted on the TYPED projections via CompareDirectories.
func CompareFingerprints(ctx context.Context, a, b ports.LiveDirectory) (bool, error) {
	if ma, mb := RecordModelOf(a), RecordModelOf(b); ma != mb {
		return false, fmt.Errorf("%w: %q vs %q — use CompareDirectories for typed parity", ErrRecordModelMismatch, ma, mb)
	}
	af, err := a.Fingerprint(ctx)
	if err != nil {
		return false, err
	}
	bf, err := b.Fingerprint(ctx)
	if err != nil {
		return false, err
	}
	return af == bf, nil
}

// HandlerEnumerator is the optional capability a LiveDirectory implements so
// the shadow comparator can build the handler-FQN comparison set. It is
// deliberately NOT part of ports.LiveDirectory: enumerating every advertised
// FQN is a parity-tooling need, not something Mesh consumers ask for, and
// widening the port would oblige every future implementation to answer it.
//
// An implementation that cannot enumerate is legitimate; what is not
// legitimate is letting its silence read as agreement. CompareDirectories
// degrades the axis instead.
type HandlerEnumerator interface {
	HandlerFQNs(ctx context.Context, tenant ports.Tenant) ([]string, error)
}

// collectHandlerFQNs derives the FQN comparison set from the sides that can
// enumerate, and names the sides that cannot. The second return value lists
// degraded axes; a non-empty list fails InParity, because the alternative is
// a handler comparison over an empty set reporting agreement.
func collectHandlerFQNs(ctx context.Context, tenant ports.Tenant, sides ...ports.LiveDirectory) ([]string, []string) {
	set := map[string]bool{}
	var degraded []string
	for i, side := range sides {
		name := "authoritative"
		if i > 0 {
			name = "shadow"
		}
		enum, ok := side.(HandlerEnumerator)
		if !ok {
			degraded = append(degraded, fmt.Sprintf(
				"handlers: %s side (%T) cannot enumerate handler FQNs — axis not compared", name, side))
			continue
		}
		fqns, err := enum.HandlerFQNs(ctx, tenant)
		if err != nil {
			degraded = append(degraded, fmt.Sprintf(
				"handlers: %s side failed to enumerate handler FQNs: %v", name, err))
			continue
		}
		for _, fqn := range fqns {
			set[fqn] = true
		}
	}
	out := make([]string, 0, len(set))
	for fqn := range set {
		out = append(out, fqn)
	}
	sort.Strings(out)
	return out, degraded
}

func memberIndex(ms []ports.Member) map[ports.NodeID]ports.Member {
	out := make(map[ports.NodeID]ports.Member, len(ms))
	for _, m := range ms {
		out[m.NodeID] = m
	}
	return out
}

func advertNodeIDs(hs []ports.HandlerAdvert) []ports.NodeID {
	out := make([]ports.NodeID, 0, len(hs))
	for _, h := range hs {
		out = append(out, h.NodeID)
	}
	return out
}

func stringSetEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	as := append([]string(nil), a...)
	bs := append([]string(nil), b...)
	sort.Strings(as)
	sort.Strings(bs)
	for i := range as {
		if as[i] != bs[i] {
			return false
		}
	}
	return true
}

func nodeSetEqual(a, b []ports.NodeID) bool {
	if len(a) != len(b) {
		return false
	}
	as := append([]ports.NodeID(nil), a...)
	bs := append([]ports.NodeID(nil), b...)
	sort.Slice(as, func(i, j int) bool { return as[i] < as[j] })
	sort.Slice(bs, func(i, j int) bool { return bs[i] < bs[j] })
	for i := range as {
		if as[i] != bs[i] {
			return false
		}
	}
	return true
}

// reachSetEqual compares the NORMALISED form only.
//
// 🛑 DO NOT ADD RawProtocol/Host/Port TO THIS KEY. The raw tier is the
// PRODUCER's vocabulary by definition, and the two sides have different
// producers: the reach layer says "udp" where swarm's enum says "noise-udp"
// for the same logical address. Including it would make cross-implementation
// parity permanently unachievable — the same category error as comparing
// fingerprints across record models (see CompareFingerprints), and it would
// fail in the direction that looks like a real divergence.
//
// Normalisation is exactly what makes the two comparable; that is its job
// here, and carrying the raw form alongside is what keeps consumers from
// having to bypass the port.
func reachSetEqual(a, b []ports.ReachAddress) bool {
	if len(a) != len(b) {
		return false
	}
	// 🛑 Priority IS PART OF THE KEY, and its absence was a measured blind
	// spot (#M-547). The two implementations derive the rank by different
	// routes — SwarmDirectory from the Address_Transport enum, LADDirectory
	// by normalising the reach layer's string — so they can disagree on the
	// rank of an address they otherwise describe identically. Both then sort
	// ascending on it, which means a disagreement here reorders the dial
	// candidates while every other field matches.
	//
	// A set comparison cannot see order, so ranking is only observable if the
	// rank itself is compared. Leaving it out let two directories that would
	// dial a peer over DIFFERENT transports report full parity.
	key := func(r ports.ReachAddress) string {
		return r.Protocol + "|" + r.Address + "|" + r.Scope + "|" +
			strconv.Itoa(r.Priority)
	}
	as := make([]string, len(a))
	bs := make([]string, len(b))
	for i := range a {
		as[i] = key(a[i])
	}
	for i := range b {
		bs[i] = key(b[i])
	}
	sort.Strings(as)
	sort.Strings(bs)
	for i := range as {
		if as[i] != bs[i] {
			return false
		}
	}
	return true
}

func shortAll(ids []ports.NodeID) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = short(id)
	}
	return out
}
