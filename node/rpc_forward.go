/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bbmumford/route"
	"github.com/ORBTR/aether/rpc/pb"
	"github.com/ORBTR/aether"
	"github.com/bbmumford/loom/pkg/rpc"
	"google.golang.org/protobuf/proto"
)

// forwardStreamCounter allocates dynamic stream IDs (10+) for forwarded RPCs.
// Matches the dispatch package's convention: IDs 0-3 are well-known, 10+ are dynamic.
var forwardStreamCounter uint64

// runtimeForwarder implements RPCForwarder using the runtime's dispatch and
// active sessions. When an RPC arrives for a handler this node doesn't own,
// the forwarder discovers which node handles it and sends the RPC there.
//
// Forwarding is protocol-agnostic — it works over any Aether session (noise-udp,
// WS, TLS, QUIC, gRPC). The transport layer handles protocol selection.
//
// Priority (parallel-probed after the FAST PATH RouteList check):
//  1. Direct Aether session to the target node
//  2. LAD-routed — sessions to peers with recent latency / signed-route
//     advertisements to the target (one extra hop)
//  3. Role-aware — sessions to peers that also serve the same role
//
// The legacy blind-relay (Path 4) and direct-dial (Path 5) paths were
// removed in the parallel-reroute refactor; Forward() now fails fast
// when none of Paths 1-3 produce a candidate, leaving recovery to the
// dispatch caller.
type runtimeForwarder struct {
	rt *Runtime

	// Forwarding counters for monitoring. Three disjoint buckets:
	//   forwardDirect    — Path 1 hit (direct session to target node)
	//   forwardLADRouted — Path 2 hit (intermediate routed hop) OR the
	//                      fast-path RouteList (upstream-supplied route)
	//   forwardRole      — Path 3 hit (same-role peer fallback)
	// Read by the monitoring/mesh-debug endpoint; sum should equal total
	// successful forwards.
	forwardDirect    int64 // atomic
	forwardRole      int64 // atomic
	forwardLADRouted int64 // atomic
}

// Forward sends an RPC request to the correct handler node.
// It discovers which node handles the method, finds a path to it, and sends
// the RPC. If no direct path exists, it forwards to any connected peer
// (which will forward onward, up to MaxRPCHops).
func (f *runtimeForwarder) Forward(ctx context.Context, req *pb.RPCRequest) (*pb.RPCResponse, error) {
	// Extract role from FQN: "hstles.tenant.CheckHealth" → "platform.tenant"
	// Uses last dot to split namespace.domain from Operation.
	role := req.Handler
	if dot := strings.LastIndexByte(req.Handler, '.'); dot > 0 {
		role = req.Handler[:dot]
	}

	selfID := string(f.rt.identity.NodeID)

	// FAST PATH: If request has a RouteList, try the intended hop first.
	// This is the common case for relayed requests from parallel probing.
	if len(req.RouteList) > 0 {
		nextHop := req.RouteList[0]
		remaining := req.RouteList[1:]

		session, ok := f.rt.getSession(nextHop)
		if ok && !session.IsClosed() {
			dbgForward.Printf("Forward %s (hop %d) → %s (routed)", req.Handler, req.Hops, aether.NodeID(nextHop).Short())

			// Pop the route and forward
			fwdReq := proto.Clone(req).(*pb.RPCRequest)
			fwdReq.RouteList = remaining
			resp, err := f.callOverMeshSession(ctx, session, fwdReq)
			if err == nil {
				atomic.AddInt64(&f.forwardLADRouted, 1)
				return resp, nil
			}
			// L3 #15: stamp a relay-failure on the broken hop so the
			// FindRoutes scoring (and any peer-selection logic that
			// reads dial-failure history) deprioritises it next time.
			// Without this, a relay whose session keeps breaking
			// stays in the route list until LAD removes it on
			// liveness timeout (minutes). Use the session's actual
			// protocol so the failure is recorded on the right
			// transport key in the quality.Tracker.
			if f.rt.connMgr != nil {
				proto := unmapProtocol(session.Protocol())
				f.rt.connMgr.recordDialFailure(nextHop, proto)
			}
			// Intended path broken — fall through to re-route
			log.Printf("[RPC] Forward %s: intended route to %s broken (%v), re-routing via LAD",
				req.Handler, aether.NodeID(nextHop).Short(), err)
		} else {
			log.Printf("[RPC] Forward %s: no session to routed hop %s, re-routing via LAD",
				req.Handler, aether.NodeID(nextHop).Short())
		}
	}

	// RE-ROUTE: Use own LAD knowledge to find a path.
	// This runs when: (a) no RouteList provided, (b) RouteList hop was broken.

	// Early deadline check — don't waste time re-routing if deadline already passed
	if req.Deadline > 0 && time.Now().UnixNano() > req.Deadline {
		return nil, fmt.Errorf("forward %s: deadline exceeded before re-route", req.Handler)
	}

	targetNodeID := req.TargetNodeId
	if targetNodeID == "" {
		// Canonical source: swarm RoleTable. The LAD DirectoryCache.Roles
		// publisher was retired in the swarm migration (project_swarm_pubkey
		// 2026-05-27); querying it returns empty fleet-wide. Without this
		// switch every TargetNodeId-empty request returned "no node serves
		// role" even when the receiving node's own swarm table knew the
		// role.
		if f.rt.swarm != nil && f.rt.swarm.RoleTable != nil {
			nodes := f.rt.swarm.RoleTable.Lookup(role, req.Handler)
			if len(nodes) == 0 {
				nodes = f.rt.swarm.RoleTable.Lookup(role, "")
			}
			if len(nodes) > 0 {
				targetNodeID = nodes[0].NodeID
			}
		}
	}

	// No target found — nobody serves this role. Fail fast instead of bouncing.
	if targetNodeID == "" {
		return nil, fmt.Errorf("no node serves role %s (handler %s)", role, req.Handler)
	}

	// Parallel re-route: build up to 3 candidate paths and probe simultaneously.
	// This only runs when the RouteList fast path is unavailable or broken.
	type fwdCandidate struct {
		session  aether.Session
		nodeID   string
		targetID string
		route    []string
	}
	var paths []fwdCandidate

	// Path 1: Direct session to target
	if targetNodeID != selfID {
		if session, ok := f.rt.getSession(targetNodeID); ok && !session.IsClosed() {
			paths = append(paths, fwdCandidate{
				session: session, nodeID: targetNodeID,
				targetID: targetNodeID, route: nil,
			})
		}
	}

	// Path 2+: LAD-routed peers with connectivity to target
	if len(paths) < 3 && f.rt.cache != nil {
		for _, c := range f.findNextHops(targetNodeID, selfID) {
			if len(paths) >= 3 {
				break
			}
			session, ok := f.rt.getSession(c.nodeID)
			if !ok || session.IsClosed() {
				continue
			}
			paths = append(paths, fwdCandidate{
				session: session, nodeID: c.nodeID,
				targetID: targetNodeID, route: []string{targetNodeID},
			})
		}
	}

	// Path 3+: Other nodes serving the same role (swarm RoleTable)
	if len(paths) < 3 && f.rt.swarm != nil && f.rt.swarm.RoleTable != nil {
		roleNodes := f.rt.swarm.RoleTable.Lookup(role, req.Handler)
		if len(roleNodes) == 0 {
			roleNodes = f.rt.swarm.RoleTable.Lookup(role, "")
		}
		for _, rn := range roleNodes {
			if len(paths) >= 3 {
				break
			}
			if rn.NodeID == selfID || rn.NodeID == targetNodeID {
				continue
			}
			session, ok := f.rt.getSession(rn.NodeID)
			if !ok || session.IsClosed() {
				continue
			}
			paths = append(paths, fwdCandidate{
				session: session, nodeID: rn.NodeID,
				targetID: rn.NodeID, route: nil,
			})
		}
	}

	if len(paths) == 0 {
		return nil, fmt.Errorf("no route to handler %s (role: %s, target: %s)", req.Handler, role, targetNodeID)
	}

	// Composite path scoring so same-region direct paths win over
	// cross-region multi-hop paths, even when the latter has a
	// higher-grade session. Sorting purely on SessionGrade is wrong:
	// a Grade-A noise-UDP hop through a remote-region relay loses to
	// a Grade-C direct websocket because the relay round-trip dominates
	// the grade-level latency difference (e.g. 77 ms via relay vs <5 ms
	// direct to a peer in the same region).
	//
	// Score structure (lower is better):
	//   1. Hop penalty: +100ms per intermediate hop (routed paths).
	//      Reflects the real cost of double traversal (req+resp
	//      through the intermediate). Direct paths (len(route)==0)
	//      score +0.
	//   2. LAD latency: if we have a Latency record for this edge,
	//      use 2*RTT as the baseline. Otherwise use a grade-based
	//      estimate (GradeA=5ms, GradeC=40ms, GradeF=1000ms).
	//   3. Grade tiebreaker: within a hop-tier, higher grade still
	//      preferred so a grade-A direct beats a grade-C direct.

	// Precompute measured latency to each candidate peer so the sort
	// comparator does a map lookup instead of scanning all records per
	// compare. At high RPC rate this was the top source of sort-time
	// allocation churn.
	var rttByPeer map[string]int64
	if f.rt.cache != nil {
		records := f.rt.cache.Latency("")
		rttByPeer = make(map[string]int64, len(records))
		for _, lat := range records {
			if lat.FromNode != selfID || lat.RTTMs <= 0 {
				continue
			}
			// Keep the best (lowest) latency if there are multiple records.
			if prev, ok := rttByPeer[lat.ToNode]; !ok || lat.RTTMs < prev {
				rttByPeer[lat.ToNode] = lat.RTTMs
			}
		}
	}

	// Use the shared scoreRoute helper (mesh/node/route_score.go) so
	// the forwarder and FindRoutes can't drift on the scoring
	// formula. Audit H6 — there were two independent copies of this
	// math before, both with the same 5/20/40/1000 + RTT-override +
	// 100ms/hop formula. Future tuning of one would silently miss
	// the other.
	pathScore := func(fc fwdCandidate) int64 {
		// rttByPeer holds keyed-by-first-hop RTT in ms; if missing,
		// scoreRoute falls back to its grade-derived synthetic.
		return scoreRoute(fc.session, len(fc.route), rttByPeer[fc.nodeID])
	}
	sort.Slice(paths, func(i, j int) bool {
		si := pathScore(paths[i])
		sj := pathScore(paths[j])
		if si != sj {
			return si < sj
		}
		// Tiebreak: higher grade wins when scores are equal.
		return SessionGrade(paths[i].session) > SessionGrade(paths[j].session)
	})

	// Hard grade-gate for the operation class. The wire context "opClass"
	// hint is the opt-in signal: the originating dispatcher derives it from
	// the handler's registered rpc.Handler.OpClass (dispatch.WithOpClass →
	// req.Context["opClass"]). Critical/Realtime/Standard operations must
	// not land on a transport below their MinGrade, so if NO available path
	// meets the minimum we fail closed with rpc.ErrGradeTooLow rather than
	// silently forwarding over an emergency-fallback link. Callers
	// distinguish this from transport failures via errors.Is.
	//
	// Fail-closed is gated behind an explicitly-set, non-bulk opClass so
	// existing flows do not regress: an empty opClass — the zero/default
	// OpClass, or any handler that never declared one — skips this block
	// entirely and keeps the prior best-effort behavior. OpClassBulk maps
	// to MinGrade GradeF, which every connected path satisfies, so it can
	// never trip the gate either.
	if opClass := req.Context["opClass"]; opClass != "" {
		var oc OperationClass
		switch opClass {
		case "critical":
			oc = OpClassCritical
		case "realtime":
			oc = OpClassRealtime
		case "standard":
			oc = OpClassStandard
		default:
			oc = OpClassBulk
		}
		minGrade := oc.MinGrade()
		filtered := paths[:0]
		for _, p := range paths {
			if SessionGrade(p.session) >= minGrade {
				filtered = append(filtered, p)
			}
		}
		if len(filtered) > 0 {
			paths = filtered
		} else if minGrade > GradeF {
			// Every candidate path is below the required grade and the
			// operation declared a real minimum — fail closed instead of
			// forwarding over an unacceptable transport.
			return nil, fmt.Errorf("forward %s: no path meets minimum grade %s for opClass %q: %w",
				req.Handler, minGrade, opClass, rpc.ErrGradeTooLow)
		}
	}

	// MESH-D04: bound fan-out to the ORIGIN. Each probe forwards to a next-hop
	// that itself re-routes; letting every hop fan out across all its paths
	// produced up to ~fanout^MaxRPCHops (≈3^6 = 729) in-flight messages per
	// request. A forwarded (Hops>0) request therefore takes a SINGLE best path,
	// so total in-flight is bounded to ~originFanout × depth (linear). paths is
	// ordered best-first, so paths[:1] keeps the strongest candidate.
	if req.Hops > 0 && len(paths) > 1 {
		paths = paths[:1]
	}

	// Launch parallel probes — first response wins. probeResult carries
	// the winning candidate so we can credit the right counter.
	type probeResult struct {
		resp     *pb.RPCResponse
		err      error
		winner   fwdCandidate
		// originalTarget captures the target node before path-3 may have
		// substituted a same-role peer; used to classify Path 1 vs Path 3.
		originalTarget string
	}
	resultCh := make(chan probeResult, len(paths))
	probeCtx, probeCancel := context.WithCancel(ctx)
	defer probeCancel()

	originalTarget := targetNodeID

	for _, p := range paths {
		fc := p
		safeGoCh("rpc_forward.probe",
			resultCh,
			probeResult{err: fmt.Errorf("forward probe panic for %s", req.Handler), winner: fc, originalTarget: originalTarget},
			func() {
				fwdReq := proto.Clone(req).(*pb.RPCRequest)
				fwdReq.TargetNodeId = fc.targetID
				fwdReq.RouteList = fc.route
				dbgForward.Printf("Forward %s (hop %d) → %s (parallel re-route)",
					req.Handler, req.Hops, aether.NodeID(fc.nodeID).Short())

				// Multi-hop deadline extension. If the caller's remaining
				// deadline is too tight for the forward hop's path, extend
				// it via mergeCtxWithTimeout — that gives the call the
				// full hop budget while STILL propagating probeCtx's
				// first-win cancellation. The previous WithTimeout(
				// context.Background(), minTimeout) rebuild orphaned the
				// goroutine from probeCtx, so a sibling probe winning
				// couldn't stop it (H-Forward-CtxLoss).
				fwdCtx := probeCtx
				minTimeout := adaptiveRPCTimeout(fc.session)
				var cancel context.CancelFunc
				if callerDeadline, hasDeadline := probeCtx.Deadline(); hasDeadline {
					if remaining := time.Until(callerDeadline); remaining < minTimeout {
						fwdCtx, cancel = mergeCtxWithTimeout(probeCtx, minTimeout)
						defer cancel()
					}
				}

				resp, err := f.callOverMeshSession(fwdCtx, fc.session, fwdReq)
				resultCh <- probeResult{resp: resp, err: err, winner: fc, originalTarget: originalTarget}
			})
	}

	var lastErr error
	for i := 0; i < len(paths); i++ {
		select {
		case result := <-resultCh:
			if result.err == nil {
				probeCancel()
				// Credit the right counter so monitoring can distinguish
				// LAD-routed and role fallback from direct dispatch.
				switch {
				case len(result.winner.route) > 0:
					atomic.AddInt64(&f.forwardLADRouted, 1)
				case result.winner.targetID != result.originalTarget:
					atomic.AddInt64(&f.forwardRole, 1)
				default:
					atomic.AddInt64(&f.forwardDirect, 1)
				}
				return result.resp, nil
			}
			lastErr = result.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return nil, fmt.Errorf("forward %s: all %d re-route paths failed: %v", req.Handler, len(paths), lastErr)
}

// callOverMeshSession sends an RPC request via the peer's BidiRPC (Stream 1).
// Falls back to a dynamic stream if no BidiRPC is available.
func (f *runtimeForwarder) callOverMeshSession(ctx context.Context, session aether.Session, req *pb.RPCRequest) (*pb.RPCResponse, error) {
	// Prefer BidiRPC on Stream 1 — avoids opening a dynamic stream.
	remoteID := string(session.RemoteNodeID())
	if f.rt.connMgr != nil {
		if bidi, ok := f.rt.connMgr.GetBidiRPC(remoteID); ok {
			return bidi.Call(ctx, req)
		}
	}

	// Fallback: open a dynamic stream (ID 10+) for this RPC.
	reqBytes, err := pb.MarshalRequest(req)
	if err != nil {
		return nil, fmt.Errorf("marshal forward request: %w", err)
	}
	id := atomic.AddUint64(&forwardStreamCounter, 1) + 9
	rpcStream, err := session.OpenStream(ctx, aether.StreamConfig{
		StreamID:    id,
		Reliability: aether.ReliableOrdered,
		Priority:    128,
	})
	if err != nil {
		return nil, fmt.Errorf("open RPC stream: %w", err)
	}
	// MESH-D03: close the dynamic stream when we're done. Returning without
	// closing leaked one client-side stream per fallback forward, and left the
	// responder's per-stream `go ServeMeshStream` goroutine blocked forever on
	// its next Receive (the client never sent FIN) — a paired stream+goroutine
	// leak that grew for the session lifetime.
	defer rpcStream.Close()

	if err := rpcStream.Send(ctx, reqBytes); err != nil {
		return nil, fmt.Errorf("forward RPC via aether: %w", err)
	}
	respBytes, err := rpcStream.Receive(ctx)
	if err != nil {
		return nil, fmt.Errorf("receive forwarded response: %w", err)
	}

	resp, err := pb.UnmarshalResponse(respBytes)
	if err != nil {
		return nil, fmt.Errorf("unmarshal forward response: %w", err)
	}
	return resp, nil
}

// nextHopCandidate represents a peer that can potentially reach the target.
type nextHopCandidate struct {
	nodeID    string
	transport string        // how the candidate connects to the target
	rttMs     int64         // latency between candidate and target
	age       time.Duration // how recently the connection was measured
}

// findNextHops returns peers that can forward RPC traffic toward the
// target. The route engine's BestPath (when wired) is the authoritative
// source — it carries dual-signed path advertisements with RFC2439
// damping. LAD latency records are a fallback for partitions where no
// route advertisement has propagated yet, sorted by freshness + RTT.
// Checks both directions of latency since gossip connections are
// bidirectional.
func (f *runtimeForwarder) findNextHops(targetNodeID, selfID string) []nextHopCandidate {
	var candidates []nextHopCandidate

	// Route-engine candidates first: explicit signed advertisements
	// override the LAD-derived heuristics.
	if ri := f.rt.route.Load(); ri != nil && ri.Router != nil {
		for _, p := range ri.Router.AllPaths(route.NodeID(targetNodeID)) {
			if string(p.NextHop) == selfID || p.NextHop == "" {
				continue
			}
			candidates = append(candidates, nextHopCandidate{
				nodeID:    string(p.NextHop),
				transport: "route",
				rttMs:     int64(p.RTTSampleMs),
				age:       0,
			})
		}
	}

	if f.rt.cache == nil {
		return candidates
	}
	allLatency := f.rt.cache.Latency("")

	now := time.Now()
	for _, lat := range allLatency {
		var nextHop string
		if lat.ToNode == targetNodeID && lat.FromNode != selfID {
			nextHop = lat.FromNode
		} else if lat.FromNode == targetNodeID && lat.ToNode != selfID {
			nextHop = lat.ToNode
		}
		if nextHop == "" {
			continue
		}

		candidates = append(candidates, nextHopCandidate{
			nodeID:    nextHop,
			transport: lat.Transport,
			rttMs:     lat.RTTMs,
			age:       now.Sub(lat.MeasuredAt),
		})
	}

	// Sort by composite end-to-end grade, then freshness, then RTT.
	sort.Slice(candidates, func(i, j int) bool {
		// Local→candidate dispatch grade (our Aether connection quality to this peer)
		iLocal := f.rt.peerGrade(candidates[i].nodeID)
		jLocal := f.rt.peerGrade(candidates[j].nodeID)
		// Candidate→target transport grade (from LAD latency record)
		iRemote := GradeForProtocol(ParseProtocol(candidates[i].transport))
		jRemote := GradeForProtocol(ParseProtocol(candidates[j].transport))
		iScore := int(iLocal) + int(iRemote)
		jScore := int(jLocal) + int(jRemote)
		if iScore != jScore {
			return iScore > jScore
		}
		// Then: prefer freshest measurement, then lowest RTT
		iFresh := candidates[i].age < 2*time.Minute
		jFresh := candidates[j].age < 2*time.Minute
		if iFresh != jFresh {
			return iFresh
		}
		return candidates[i].rttMs < candidates[j].rttMs
	})

	// Deduplicate by nodeID (keep best per peer)
	seen := map[string]bool{}
	var deduped []nextHopCandidate
	for _, c := range candidates {
		if seen[c.nodeID] {
			continue
		}
		seen[c.nodeID] = true
		deduped = append(deduped, c)
	}

	return deduped
}

// Compile-time interface check.
var _ RPCForwarder = (*runtimeForwarder)(nil)

// ForwardingStats returns counters for each forwarding path.
func (f *runtimeForwarder) ForwardingStats() map[string]int64 {
	return map[string]int64{
		"direct":     atomic.LoadInt64(&f.forwardDirect),
		"role":       atomic.LoadInt64(&f.forwardRole),
		"lad_routed": atomic.LoadInt64(&f.forwardLADRouted),
	}
}

// newRuntimeForwarder creates a forwarder backed by the runtime's sessions.
func newRuntimeForwarder(rt *Runtime) *runtimeForwarder {
	return &runtimeForwarder{rt: rt}
}

// wireForwarder connects the RPC forwarder to the RPC server.
// Called during runtime initialization after the RPC server is created.
func (rt *Runtime) wireForwarder() {
	if rt.rpcServer != nil {
		fwd := newRuntimeForwarder(rt)
		rt.rpcForwarder = fwd
		rt.rpcServer.SetForwarder(fwd)
		log.Printf("[RPC] Mesh-routed forwarding enabled (max hops: %d)", pb.MaxRPCHops)
	}
}
