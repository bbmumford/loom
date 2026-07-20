/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package directory

import (
	"context"
	"fmt"
	"sort"

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
}

// InParity reports whether the comparison found zero mismatches.
func (r *ShadowReport) InParity() bool { return len(r.Mismatches) == 0 }

func (r *ShadowReport) add(format string, args ...any) {
	r.Mismatches = append(r.Mismatches, fmt.Sprintf(format, args...))
}

// CompareDirectories runs the capability-by-capability parity check between
// the authoritative side and the shadow side: membership (+ roles, service,
// tenant), per-node reach sets, and handler adverts for every FQN either
// side knows. Ordering differences are normalized away; content differences
// are mismatches.
func CompareDirectories(ctx context.Context, authoritative, shadow ports.LiveDirectory, roles []string) (*ShadowReport, error) {
	rep := &ShadowReport{}

	aMembers, err := authoritative.Members(ctx)
	if err != nil {
		return nil, fmt.Errorf("shadow: authoritative members: %w", err)
	}
	sMembers, err := shadow.Members(ctx)
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
		aNodes, err := authoritative.NodesByRole(ctx, role)
		if err != nil {
			return nil, err
		}
		sNodes, err := shadow.NodesByRole(ctx, role)
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
		aReach, err := authoritative.Reach(ctx, id)
		if err != nil {
			return nil, err
		}
		sReach, err := shadow.Reach(ctx, id)
		if err != nil {
			return nil, err
		}
		if !reachSetEqual(aReach, sReach) {
			rep.add("reach %s: %v vs %v", short(id), aReach, sReach)
		}
	}

	// Handler-advert parity over the union of FQNs either side indexes.
	// (The port has no FQN enumeration; SwarmDirectory sides expose their
	// index, opaque sides contribute nothing and are covered by the
	// member/role/fingerprint comparisons.)
	for _, fqn := range collectHandlerFQNs(ctx, authoritative, shadow) {
		rep.ComparedHandlers++
		aH, err := authoritative.HandlersByName(ctx, fqn)
		if err != nil {
			return nil, err
		}
		sH, err := shadow.HandlersByName(ctx, fqn)
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

// CompareFingerprints is the cheap continuous check between full parity
// runs: two directories over the same accepted set must agree.
func CompareFingerprints(ctx context.Context, a, b ports.LiveDirectory) (bool, error) {
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

// collectHandlerFQNs derives the FQN comparison set from both sides'
// SwarmDirectory-style handler indices when available; falls back to
// nothing for opaque implementations (callers then rely on member/role
// parity + fingerprints).
func collectHandlerFQNs(_ context.Context, sides ...ports.LiveDirectory) []string {
	set := map[string]bool{}
	for _, side := range sides {
		if sd, ok := side.(*SwarmDirectory); ok {
			sd.mu.RLock()
			for fqn := range sd.handlers {
				set[fqn] = true
			}
			sd.mu.RUnlock()
		}
	}
	out := make([]string, 0, len(set))
	for fqn := range set {
		out = append(out, fqn)
	}
	sort.Strings(out)
	return out
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

func reachSetEqual(a, b []ports.ReachAddress) bool {
	if len(a) != len(b) {
		return false
	}
	key := func(r ports.ReachAddress) string {
		return r.Protocol + "|" + r.Address + "|" + r.Scope
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
