/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 */

package node

import (
	"context"
	"testing"
	"time"

	"github.com/bbmumford/loom/ports"
)

// fakeParityDir answers only the LiveDirectory methods CompareDirectories and
// CompareFingerprints actually reach, and declares a record model so the
// fingerprint step short-circuits before touching the embedded nil interface.
// The embedded ports.LiveDirectory is never called — a query outside the
// overridden set would panic, which keeps the fake honest about its surface.
type fakeParityDir struct {
	ports.LiveDirectory
	model   string
	members []ports.Member
}

func (f *fakeParityDir) RecordModel() string { return f.model }

func (f *fakeParityDir) Members(context.Context, ports.Tenant) ([]ports.Member, error) {
	return f.members, nil
}

func (f *fakeParityDir) Reach(context.Context, ports.Tenant, ports.NodeID) ([]ports.ReachAddress, error) {
	return nil, nil
}

// HandlerFQNs satisfies directory.HandlerEnumerator so the handler axis is
// performed (over an empty set) rather than recorded as a degraded axis — that
// keeps InParity() driven by member content, not by an un-enumerable side.
func (f *fakeParityDir) HandlerFQNs(context.Context, ports.Tenant) ([]string, error) {
	return nil, nil
}

// The two sides carry DIFFERENT record models on purpose (as the live LAD
// directory and the Swarm shadow do), so CompareFingerprints refuses the
// pairing and the pass escalates to the typed CompareDirectories walk.
func ladShadowPair(auth, shadow []ports.Member) (*fakeParityDir, *fakeParityDir) {
	return &fakeParityDir{model: "lad", members: auth},
		&fakeParityDir{model: "swarm", members: shadow}
}

// Enabled path: a member present on one side and absent on the other must
// surface as divergence in the counters the pass writes.
func TestShadowParityPassRecordsDivergence(t *testing.T) {
	rt := &Runtime{}
	auth, shadow := ladShadowPair(
		[]ports.Member{{NodeID: "aaa", ServiceName: "svc", Tenant: "t"}},
		[]ports.Member{{NodeID: "bbb", ServiceName: "svc", Tenant: "t"}},
	)

	rt.shadowParityPass(context.Background(), auth, shadow, ports.Tenant("t"), nil)

	if got := rt.shadowParityRuns.Load(); got != 1 {
		t.Fatalf("shadowParityRuns = %d, want 1 — a divergent typed comparison did not "+
			"complete a pass", got)
	}
	if got := rt.shadowParityDiverged.Load(); got != 1 {
		t.Fatalf("shadowParityDiverged = %d, want 1 — the member mismatch did not surface", got)
	}
	// "aaa missing from shadow" + "bbb present ONLY in shadow" = two mismatch lines.
	if got := rt.shadowParityMismatches.Load(); got != 2 {
		t.Fatalf("shadowParityMismatches = %d, want 2 — the mismatch lines were not counted", got)
	}
}

// Control: identical membership must run a pass and record NO divergence, so a
// non-zero diverged counter can only mean the sides actually disagreed.
func TestShadowParityPassInParityRecordsNoDivergence(t *testing.T) {
	rt := &Runtime{}
	same := []ports.Member{{NodeID: "aaa", ServiceName: "svc", Tenant: "t"}}
	auth, shadow := ladShadowPair(same, same)

	rt.shadowParityPass(context.Background(), auth, shadow, ports.Tenant("t"), nil)

	if got := rt.shadowParityRuns.Load(); got != 1 {
		t.Fatalf("shadowParityRuns = %d, want 1", got)
	}
	if got := rt.shadowParityDiverged.Load(); got != 0 {
		t.Fatalf("shadowParityDiverged = %d, want 0 — parity was reported as divergence", got)
	}
	if got := rt.shadowParityMismatches.Load(); got != 0 {
		t.Fatalf("shadowParityMismatches = %d, want 0", got)
	}
}

// Disabled path: with no interval configured, startShadowParity must not start
// and must write nothing — a true no-op with zero writers of the counters.
func TestShadowParityDisabledIsANoOp(t *testing.T) {
	rt := &Runtime{}
	rt.cfg.ShadowParityInterval = 0

	if started := rt.startShadowParity(); started {
		t.Fatal("startShadowParity reported started with a zero interval — the gate is open by default")
	}
	if rt.shadowParityRuns.Load() != 0 ||
		rt.shadowParityDiverged.Load() != 0 ||
		rt.shadowParityMismatches.Load() != 0 {
		t.Fatal("disabled shadow parity wrote a counter — it is not a true no-op")
	}
}

// A positive interval but no shadow wired (rt.swarm nil) must also decline to
// start: the pass would have nothing to compare against.
func TestShadowParityWithoutShadowDoesNotStart(t *testing.T) {
	rt := &Runtime{}
	rt.cfg.ShadowParityInterval = time.Second
	rt.liveDir = &fakeParityDir{model: "lad"}

	if started := rt.startShadowParity(); started {
		t.Fatal("startShadowParity started with no trust shadow to compare against")
	}
}
