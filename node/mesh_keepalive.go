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
)

// meshKeepalives holds every session a runMeshKeepalive is probing, so a session runs
// one keepalive. health.Monitor holds one pending probe sequence (RecordPingSent), so a
// second prober's PING replaces the first's and the first's PONG then gives the
// estimator no round-trip sample. Keyed by the session; runMeshKeepalive adds its
// session before the first probe and deletes it when it returns.
var meshKeepalives sync.Map

// runMeshKeepalive is a mesh session's one keepalive: AcceptMeshConnection runs it on
// stream 2 for the session's life. aether.RunKeepalive is the keepalive of a session no
// node runtime runs one on, so nothing in this package calls it on a mesh session, and a
// second runMeshKeepalive for a session whose keepalive is running returns false at once
// and probes nothing.
//
// Every number it waits on comes from aether.KeepalivePlanFor, read again before each
// wait and each probe: Interval between probes on a healthy session, ProbeTimeout per
// probe, RetryGap between probes once one went unanswered, and Strikes unanswered probes
// in a row before the session is let go. The plan derives them from the session's
// health monitor: SRTT, RTO, sample count and Instability from the PONGs the session's
// control handler records, and ProbeLoss from RecordKeepaliveAnswer, which this loop
// calls on every answered probe with the count of unanswered probes before it. So the
// probe wait widens on a path whose round trips jump and Strikes rises on a path that
// loses probes, as they do under aether.RunKeepalive.
//
// A session whose peer has a standby path is held to the same Strikes. Strikes is the
// fewest unanswered probes a healthy peer on this path's measured loss reaches with
// chance at most 1 in 1000, and a standby path does not change that chance; a live
// session let go early costs the peer that session and the redial that replaces it,
// standby or not. The standby takes dispatch when this loop lets the session go, through
// AcceptMeshConnection's cleanup (OnPrimaryFailure).
//
// It returns true when the session is to be let go (it closed, or Strikes probes in a
// row went unanswered), and false when ctx ended or another keepalive holds s.
func runMeshKeepalive(ctx context.Context, s aether.Session, nodeID string) bool {
	if _, running := meshKeepalives.LoadOrStore(s, struct{}{}); running {
		log.Printf("[AETHER] %s: keepalive already running on this session, not starting a second", truncID(nodeID))
		return false
	}
	defer meshKeepalives.Delete(s)
	grade := SessionGrade(s)
	for {
		interval := aether.KeepalivePlanFor(s).Interval
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
			plan := aether.KeepalivePlanFor(s)
			pingCtx, pingCancel := context.WithTimeout(ctx, plan.ProbeTimeout)
			_, err := s.Ping(pingCtx)
			pingCancel()
			if err == nil {
				if h := s.Health(); h != nil {
					h.RecordKeepaliveAnswer(attempt - 1)
				}
				break
			}
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
