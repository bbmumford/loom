/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"log"
	"time"

	"github.com/bbmumford/loom/directory"
	"github.com/bbmumford/loom/journal"
	"github.com/bbmumford/loom/ports"
	"github.com/bbmumford/swarm"
)

// swarmToPortsRecord maps an accepted swarm.Record onto the ports.Record the
// SwarmDirectory ingests — the feed adapter between the swarm store's
// accepted-change stream (Config.OnAccepted, swarm-typed) and the ports-typed
// trust gate + projection. An observer-signed attestation (IsObserverAttestation)
// gets a non-empty Observer segment so the directory gate binds the OBSERVER's
// key rather than the subject NodeID (see SwarmDirectory.Ingest); the segment
// carries the observer NodeID so the mapping is not lossy.
func swarmToPortsRecord(r swarm.Record) ports.Record {
	out := ports.Record{
		Topic:        ports.Topic(string(r.Topic)),
		NodeID:       ports.NodeID(string(r.NodeID)),
		Key:          r.Key,
		HLC:          ports.HLC(r.HLC),
		Tombstone:    r.Tombstone,
		Body:         append([]byte(nil), r.Body...),
		AuthorPubKey: append([]byte(nil), r.PubKey...),
		Signature:    append([]byte(nil), r.Sig...),
	}
	if r.IsObserverAttestation() {
		out.Observer = []byte(string(r.ObserverNodeID))
	}
	return out
}

// shadowBuffer bounds the accepted-record queue between the swarm accept path
// and the shadow's ingest worker. A full buffer drops (and counts) rather than
// blocking swarm accept — the shadow must never add latency to the live path.
const shadowBuffer = 1024

// TrustShadow runs the Swarm-backed directory as a passive SHADOW of the live
// LAD directory during the Phase-0.5 cutover: it ingests the same accepted
// records — in OBSERVE mode, so a trust-gate failure is counted, never fatal —
// into a SwarmDirectory that no live consumer reads. Its two jobs are to
// accumulate the trust would-reject evidence (Stats) and to expose a directory
// (Directory) a periodic parity check can compare against the authoritative LAD
// side via directory.CompareDirectories. It touches nothing the live path
// depends on: the feed is non-blocking and the projection is private.
type TrustShadow struct {
	dir     *directory.SwarmDirectory
	journal *journal.FileJournal
	ch      chan swarm.Record
	dropped uint64 // records shed because the ingest worker fell behind
}

// newTrustShadow builds the shadow over its OWN journal (kept apart from the
// node's reserved DurableJournal seam) and a baseline-seeded observing policy,
// or returns (nil, nil) when mode is not "observe" — the shadow is an
// observe-only device, since enforcement is the live swarm gate's job, not a
// passive projection's. The worker stops when ctx is cancelled.
func newTrustShadow(ctx context.Context, mode, journalDir string, seed directory.PolicyConfig) (*TrustShadow, error) {
	if mode != "observe" {
		return nil, nil
	}
	j, err := journal.Open(journalDir)
	if err != nil {
		return nil, err
	}
	dir, err := directory.NewSwarmDirectoryObserving(ctx, j, directory.NewPolicy(seed))
	if err != nil {
		_ = j.Close()
		return nil, err
	}
	s := &TrustShadow{dir: dir, journal: j, ch: make(chan swarm.Record, shadowBuffer)}
	go s.run(ctx)
	return s, nil
}

// Observe feeds one accepted swarm record into the shadow. It is non-blocking
// and safe to call directly from swarm Config.OnAccepted: when the ingest
// worker is behind, the record is dropped and counted rather than stalling the
// accept path. A nil receiver is a no-op so the OnAccepted hook needs no guard.
func (s *TrustShadow) Observe(r swarm.Record) {
	if s == nil {
		return
	}
	select {
	case s.ch <- r:
	default:
		s.dropped++
	}
}

// run drains the queue into the shadow directory off the accept hot path. Ingest
// errors are the observe-mode gate's would-rejects (already counted inside the
// directory) or a journal error; either way the shadow swallows them — it is
// never allowed to affect the live node.
func (s *TrustShadow) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			_ = s.journal.Close()
			return
		case r := <-s.ch:
			if err := s.dir.Ingest(ctx, swarmToPortsRecord(r)); err != nil {
				// Observe mode never returns a gate refusal here, so a non-nil
				// err is a journal/projection fault on the shadow only.
				log.Printf("[TRUST-SHADOW] ingest: %v", err)
			}
		}
	}
}

// Directory exposes the shadow projection for a parity comparison against the
// authoritative directory via directory.CompareDirectories.
func (s *TrustShadow) Directory() ports.LiveDirectory {
	if s == nil {
		return nil
	}
	return s.dir
}

// Stats reports the trust would-reject evidence (checked, rejected) plus the
// count of records shed under load — the inputs an operator reads to decide
// whether the seed is complete enough to move the live gate to enforce.
func (s *TrustShadow) Stats() (checked, rejected, dropped uint64) {
	if s == nil {
		return 0, 0, 0
	}
	return s.dir.Checked(), s.dir.Rejected(), s.dropped
}

// shadowParityScope names the tenant + role set the parity pass queries on both
// directories. It reads the node's own configured tenant and roles: an empty
// tenant selects records stored under the empty tenant (ports.Tenant is a
// literal bucket key, not a wildcard), and the role list bounds the per-role
// NodesByRole comparison to roles this node actually participates in.
func (rt *Runtime) shadowParityScope() (ports.Tenant, []string) {
	tenant := ports.Tenant("")
	for _, tc := range rt.cfg.Tenants {
		if tc.TenantID != "" {
			tenant = ports.Tenant(tc.TenantID)
			break
		}
	}
	return tenant, rt.cfg.Roles
}

// startShadowParity launches the periodic directory-parity comparison and
// reports whether it started. It is a TRUE no-op — starting no goroutine and
// touching no counter — when Config.ShadowParityInterval is not positive or
// when no shadow directory exists to compare against (rt.swarm/trustShadow or
// rt.liveDir absent). When it does start, the ticker drives shadowParityPass
// with rt.liveDir as the authoritative side and the trust shadow's projection
// as the shadow side; the goroutine is enrolled in the runtime waitgroup and
// stops on ctx cancellation.
func (rt *Runtime) startShadowParity() bool {
	interval := rt.cfg.ShadowParityInterval
	if interval <= 0 {
		return false
	}
	if rt.liveDir == nil || rt.swarm == nil || rt.swarm.trustShadow == nil {
		return false
	}
	shadow := rt.swarm.trustShadow.Directory()
	if shadow == nil {
		return false
	}
	tenant, roles := rt.shadowParityScope()
	log.Printf("[SHADOW-PARITY] periodic directory comparison started (interval=%v)", interval)
	rt.GoCtx("shadow-parity", func(ctx context.Context) {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				rt.shadowParityPass(ctx, rt.liveDir, shadow, tenant, roles)
			}
		}
	})
	return true
}

// shadowParityPass runs one comparison between the authoritative directory and
// the shadow projection and records divergence into the shadowParity* counters
// plus a structured log. It is the body the ShadowParityInterval ticker drives,
// factored out so a caller can perform a single pass without a live ticker.
//
// It first tries the cheap fingerprint check: two directories over the same
// accepted set and the same record model agree byte-for-byte, so an equal
// fingerprint proves parity without the per-capability walk. CompareFingerprints
// refuses a cross-record-model pairing (the live directory and the Swarm shadow
// project the same facts into different record shapes) with an error rather than
// a false divergence — on that error, or on any fingerprint difference, the pass
// escalates to the typed CompareDirectories walk, which is the comparison that
// is meaningful across the two models. A report that is not InParity is the
// evidence that the shadow does not yet reproduce the live directory.
func (rt *Runtime) shadowParityPass(ctx context.Context, authoritative, shadow ports.LiveDirectory, tenant ports.Tenant, roles []string) {
	if authoritative == nil || shadow == nil {
		return
	}
	if equal, err := directory.CompareFingerprints(ctx, authoritative, shadow); err == nil && equal {
		rt.shadowParityRuns.Add(1)
		return
	}
	rep, err := directory.CompareDirectories(ctx, authoritative, shadow, tenant, roles)
	if err != nil {
		log.Printf("[SHADOW-PARITY] compare error: %v", err)
		return
	}
	rt.shadowParityRuns.Add(1)
	if rep.InParity() {
		return
	}
	rt.shadowParityDiverged.Add(1)
	rt.shadowParityMismatches.Add(uint64(len(rep.Mismatches)))
	first := ""
	if len(rep.Mismatches) > 0 {
		first = rep.Mismatches[0]
	}
	log.Printf("[SHADOW-PARITY] divergence: %d mismatches, %d degraded axes "+
		"(members=%d roles=%d reach=%d handlers=%d); first=%q",
		len(rep.Mismatches), len(rep.Degraded),
		rep.ComparedMembers, rep.ComparedRoles, rep.ComparedReach, rep.ComparedHandlers, first)
}
