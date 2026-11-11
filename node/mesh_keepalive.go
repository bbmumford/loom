/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/quality"
)

// meshKeepalives holds every session a runMeshKeepalive is probing, so a session runs
// one keepalive. health.Monitor holds one pending probe sequence (RecordPingSent), so a
// second prober's PING replaces the first's and the first's PONG then gives the
// estimator no round-trip sample. Keyed by the session; runMeshKeepalive adds its
// session before the first probe and deletes it when it returns.
var meshKeepalives sync.Map

// meshKeepalivePaths is the peer's multipath manager as runMeshKeepalive reads and feeds it
// (multipath.Manager).
type meshKeepalivePaths interface {
	KeepalivePlanFor(aether.Session) quality.KeepalivePlan
	RecordKeepaliveProbe(aether.Session, time.Duration, error)
}

// runMeshKeepalive is a mesh session's one keepalive: AcceptMeshConnection runs it on
// stream 2 for the session's life. aether.RunKeepalive is the keepalive of a session no
// node runtime runs one on, so nothing in this package calls it on a mesh session, and a
// second runMeshKeepalive for a session whose keepalive is running returns false at once
// and probes nothing.
//
// Every number it waits on comes from the session's keepalive plan, read again before each
// wait, before each probe and after each unanswered probe: Interval between probes on a
// healthy session, ProbeTimeout per probe, RetryGap between probes once one went
// unanswered, and Strikes unanswered probes in a row before the session is let go, held
// to the plan read after the latest unanswered probe. The plan is the peer's multipath
// manager's (paths, multipath.Manager.KeepalivePlanFor) and, with no manager,
// aether.KeepalivePlanFor;
// both derive it from the session's health monitor: SRTT, RTO, sample count and
// Instability from the PONGs the session's control handler records, and ProbeLoss from
// RecordKeepaliveAnswer, which this loop calls on every answered probe with the count of
// unanswered probes before it. So the probe wait widens on a path whose round trips jump
// and Strikes rises on a path that loses probes, as they do under aether.RunKeepalive.
//
// The manager's plan adds the peer's standby path: beside a standby whose own keepalive
// has converged and whose latest probe was answered, quality.Keepalive lowers the floor
// under Strikes from 5 to 2 and keeps the loss rule, so a silent session is let go sooner
// and the standby takes dispatch through AcceptMeshConnection's cleanup
// (OnPathFailure). Every probe's result goes to the manager (RecordKeepaliveProbe), the
// manager's own probe rounds ping nothing (TakeProbesFromKeepalive in
// addMultipathSession), and this loop is the session's one prober. paths is called per
// probe because the peer's manager is dropped with its last path and rebuilt with the
// next; a nil paths or a nil manager reads aether.KeepalivePlanFor and reports nothing.
//
// It returns true when the session is to be let go (it closed, or Strikes probes in a
// row went unanswered), and false when ctx ended or another keepalive holds s.
func runMeshKeepalive(ctx context.Context, s aether.Session, nodeID string, paths func() meshKeepalivePaths) bool {
	if _, running := meshKeepalives.LoadOrStore(s, struct{}{}); running {
		log.Printf("[AETHER] %s: keepalive already running on this session, not starting a second", truncID(nodeID))
		return false
	}
	defer meshKeepalives.Delete(s)
	grade := SessionGrade(s)
	planFor := func() quality.KeepalivePlan {
		if p := meshPathsOf(paths); p != nil {
			return p.KeepalivePlanFor(s)
		}
		return aether.KeepalivePlanFor(s)
	}
	for {
		interval := planFor().Interval
		select {
		case <-ctx.Done():
			return false
		case <-time.After(interval):
		}
		// A session that closed during the wait ends here, before a probe, so
		// dispatch and the multipath manager stop selecting it at once.
		if s.IsClosed() {
			log.Printf("[AETHER] %s: keepalive detected session closed (grade=%s, interval=%v), cancelling",
				truncID(nodeID), grade, interval)
			return true
		}
		// Once a probe goes unanswered the next follows after the plan's RetryGap,
		// not its Interval, so a silent peer is found within Strikes probe timeouts.
		for attempt := 1; ; attempt++ {
			plan := planFor()
			pingCtx, pingCancel := context.WithTimeout(ctx, plan.ProbeTimeout)
			rtt, err := s.Ping(pingCtx)
			pingCancel()
			if p := meshPathsOf(paths); p != nil && ctx.Err() == nil {
				p.RecordKeepaliveProbe(s, rtt, err)
			}
			if err == nil {
				if h := s.Health(); h != nil {
					h.RecordKeepaliveAnswer(attempt - 1)
				}
				break
			}
			// The Strikes this run of unanswered probes is held to, and the RetryGap
			// before the next, come from the plan as it stands once this probe failed:
			// the peer's standby can die while the probe waits (AcceptMeshConnection's
			// cleanup calls OnPathFailure for it), and multipath.Manager.KeepalivePlanFor
			// then reads no answering standby and raises Strikes to the no-standby floor.
			plan = planFor()
			log.Printf("[AETHER] %s: keepalive ping failed (%d/%d, grade=%s, timeout=%v, interval=%v): %v",
				truncID(nodeID), attempt, plan.Strikes, grade, plan.ProbeTimeout, plan.Interval, err)
			if attempt >= plan.Strikes {
				log.Printf("[AETHER] %s: keepalive dead after %d failures (grade=%s), closing",
					truncID(nodeID), attempt, grade)
				return true
			}
			// A session closed under the probe answers no retry.
			if s.IsClosed() {
				log.Printf("[AETHER] %s: keepalive detected session closed mid-retry (grade=%s), cancelling",
					truncID(nodeID), grade)
				return true
			}
			select {
			case <-ctx.Done():
				return false
			case <-time.After(plan.RetryGap):
			}
		}
	}
}

// meshPathsOf is the peer's multipath manager paths returns now, or nil when paths is nil
// or the peer has none.
func meshPathsOf(paths func() meshKeepalivePaths) meshKeepalivePaths {
	if paths == nil {
		return nil
	}
	return paths()
}
