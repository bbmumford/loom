/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/rpc/pb"
	lad "github.com/bbmumford/ledger"
	"github.com/bbmumford/loom/pkg/dispatch"
	"github.com/bbmumford/route"
	"google.golang.org/protobuf/proto"
)

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
//  3. Answerer-aware — sessions to other peers that advertise the operation
//     (RoleTable.Answerers), never to a peer of the role that does not list it
//
// There is no blind-relay or direct-dial fallback: Forward() fails fast when
// none of Paths 1-3 produce a candidate, leaving recovery to the dispatch
// caller.
type runtimeForwarder struct {
	rt *Runtime

	// Forwarding counters for monitoring. Three disjoint buckets:
	//   forwardDirect    — Path 1 hit (direct session to target node)
	//   forwardLADRouted — Path 2 hit (intermediate routed hop) OR the
	//                      fast-path RouteList (upstream-supplied route)
	//   forwardRole      — Path 3 hit (another answerer of the operation)
	// Read by the monitoring/mesh-debug endpoint; sum should equal total
	// successful forwards.
	forwardDirect    int64 // atomic
	forwardRole      int64 // atomic
	forwardLADRouted int64 // atomic
}

// GradeTooLowError is Forward's refusal when no candidate path meets the floor
// of the request's operation class, or when the request names a class the gate
// does not know. It unwraps to rpc.ErrGradeTooLow, which callers test with
// errors.Is. Current is the best grade among the candidates and Required the
// class's floor; for an unknown class both are zero, since it has no floor.
//
// It is dispatch's type under node's name: dispatch.HWPCaller returns the same
// refusal for the first hop, and pkg/dispatch cannot import this package.
type GradeTooLowError = dispatch.GradeTooLowError

// RouteDeadlineError is Forward's refusal when the caller's remaining deadline
// is shorter than every graded candidate's need (hopNeed of its session at the
// request's hop count), or when the request's absolute deadline has already
// passed, and RPCServer's refusal of a request that arrives after that
// deadline. It unwraps to rpc.ErrRouteDeadline. Need is the smallest need among
// the candidates, so Remaining < Need always holds; an expired request has no
// candidates yet, so Need is zero and Remaining negative. Like
// GradeTooLowError it is dispatch's type under node's name.
type RouteDeadlineError = dispatch.RouteDeadlineError

// hopNeed is the time Forward requires a caller to have left before it sends a
// request over session at hop count hops (the count RPCServer has already
// advanced for this forward): the session's default call deadline,
// adaptiveRPCTimeout, divided across the most hops a request can take (the
// origin's send and pb.MaxRPCHops forwards), times the hops this forward and
// every later one may still take. A caller that gave the default deadline
// therefore has each hop's need left as the request travels, where requiring
// the whole default at every hop refused it at the first forward, because the
// origin's send had already spent part of it; and a caller with less than
// the share of the hops left is still refused before the send.
func hopNeed(session aether.Session, hops int32) time.Duration {
	left := int64(pb.MaxRPCHops) - int64(hops) + 1
	if left < 1 {
		left = 1
	}
	return adaptiveRPCTimeout(session) * time.Duration(left) / time.Duration(pb.MaxRPCHops+1)
}

// hopFits reports whether ctx's remaining deadline covers a forward over
// session at hop count hops, whose need is hopNeed. A context without a
// deadline always fits, because there is no budget to exceed.
func hopFits(ctx context.Context, session aether.Session, hops int32) bool {
	dl, ok := ctx.Deadline()
	if !ok {
		return true
	}
	return time.Until(dl) >= hopNeed(session, hops)
}

// keepCallerBudget sets a forwarded request's TimeoutNs and Deadline to no more
// than the time ctx has left. RPCServer.handleRequest at the next hop bounds
// its handler with TimeoutNs from the moment the request arrives, so a
// TimeoutNs copied unchanged from the origin restarts the clock at every hop;
// Deadline is the absolute instant it refuses an expired request at. A ctx
// without a deadline leaves both as the request carried them.
func keepCallerBudget(ctx context.Context, req *pb.RPCRequest) {
	dl, ok := ctx.Deadline()
	if !ok {
		return
	}
	left := time.Until(dl)
	if left <= 0 {
		// TimeoutNs 0 means "unbounded" to the next hop, so an exhausted
		// budget travels as the smallest bound instead.
		left = time.Nanosecond
	}
	if req.TimeoutNs <= 0 || time.Duration(req.TimeoutNs) > left {
		req.TimeoutNs = int64(left)
	}
	if abs := dl.UnixNano(); req.Deadline <= 0 || req.Deadline > abs {
		req.Deadline = abs
	}
}

// Forward sends an RPC request toward a node that advertises its handler:
// the target the request names, or else the first of the operation's
// answerers in the role table. It sends over a direct session, through a peer
// LAD or the route engine says reaches that node, or to another answerer, and
// refuses before sending, typed, when there is no answerer and no target, no
// path, or no path that meets the class floor or the caller's budget.
func (f *runtimeForwarder) Forward(ctx context.Context, req *pb.RPCRequest) (*pb.RPCResponse, error) {
	// Extract role from FQN: "example.health.CheckHealth" → "example.health"
	// Uses last dot to split namespace.domain from Operation.
	role := req.Handler
	if dot := strings.LastIndexByte(req.Handler, '.'); dot > 0 {
		role = req.Handler[:dot]
	}

	selfID := string(f.rt.identity.NodeID)

	// The operation class sets the lowest route grade this request may leave
	// on. The "opClass" hint is what dispatch.WithOpClass put in req.Context; a
	// request that names none is Standard. A name ParseOperationClass does not
	// know is refused here, before any path is tried, rather than held to some
	// other class's floor.
	opClass := req.Context["opClass"]
	class, known := ParseOperationClass(opClass)
	if !known {
		return nil, &GradeTooLowError{Handler: req.Handler, Class: opClass, UnknownClass: true}
	}
	minGrade := class.MinGrade()

	// The effect class decides what may follow a send that went unanswered:
	// another path only for a Repeatable handler. Any other handler is sent on
	// one path, and its unanswered send ends the forward as ambiguous.
	repeatable := f.rt.handlerEffect(req.Handler).Repeatable()

	// FAST PATH: If request has a RouteList, try the intended hop first.
	// This is the common case for relayed requests from parallel probing.
	// The hop is held to the same floor and budget as a re-routed path; one
	// that falls short is not sent to, and the re-route below either finds
	// another path or returns the typed refusal.
	if len(req.RouteList) > 0 {
		nextHop := req.RouteList[0]
		remaining := req.RouteList[1:]

		session, ok := f.rt.getSession(nextHop)
		switch {
		case !ok || session.IsClosed():
			log.Printf("[RPC] Forward %s: no session to routed hop %s, re-routing via LAD",
				req.Handler, aether.NodeID(nextHop).Short())
		case SessionGrade(session) < minGrade:
			log.Printf("[RPC] Forward %s: routed hop %s grades %s, below the %s floor of operation class %s; re-routing via LAD",
				req.Handler, aether.NodeID(nextHop).Short(), SessionGrade(session), minGrade, class)
		case !hopFits(ctx, session, req.Hops):
			log.Printf("[RPC] Forward %s: routed hop %s needs %v, more than the caller has left; re-routing via LAD",
				req.Handler, aether.NodeID(nextHop).Short(), hopNeed(session, req.Hops))
		default:
			dbgForward.Printf("Forward %s (hop %d) → %s (routed)", req.Handler, req.Hops, aether.NodeID(nextHop).Short())

			// Pop the route and forward
			fwdReq := proto.Clone(req).(*pb.RPCRequest)
			fwdReq.RouteList = remaining
			keepCallerBudget(ctx, fwdReq)
			resp, err := f.callOverMeshSession(ctx, session, fwdReq)
			if err == nil {
				atomic.AddInt64(&f.forwardLADRouted, 1)
				return resp, nil
			}
			// Stamp a relay-failure on the broken hop so the
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
			// The intended hop took the request and gave no answer, so its
			// target may have run it: only a Repeatable handler, or a request
			// that never left, is re-routed.
			if !repeatable && !dispatch.Unsent(err) {
				return nil, &dispatch.AmbiguousEffectError{Handler: req.Handler, Cause: err}
			}
			// Intended path broken — fall through to re-route
			log.Printf("[RPC] Forward %s: intended route to %s broken (%v), re-routing via LAD",
				req.Handler, aether.NodeID(nextHop).Short(), err)
		}
	}

	// RE-ROUTE: Use own LAD knowledge to find a path.
	// This runs when: (a) no RouteList provided, (b) RouteList hop was broken.

	// Early deadline check — don't waste time re-routing if deadline already
	// passed. It is the same typed refusal as the budget check below, which
	// RPCServer stamps on its response; a plain error here would reach the
	// origin as a handler rejection.
	if now := time.Now().UnixNano(); req.Deadline > 0 && now > req.Deadline {
		return nil, &RouteDeadlineError{Handler: req.Handler, Remaining: time.Duration(req.Deadline - now)}
	}

	// The peers a re-route may pick from, read once for the target below and for
	// Path 3. Its source is the swarm RoleTable, the table every node's handler
	// adverts reach on fleet.peer. An advert that does not hold the requirement
	// its operation's other adverts name is left out, and adverts that disagree
	// refuse the request here, before anything is sent, so a relay never takes a
	// request to a holder the origin would have refused to pick. RPCServer stamps
	// the refusal on its response so the origin gets it typed.
	//
	// Only the operation's answerers are picked, never another node of the role:
	// a peer whose record does not list req.Handler answers it as not found
	// (RPCServer.handleRequest), and the origin's findNodeForRole picks from the
	// same answerers, so the target a relay picks is one the origin could have
	// picked. With none and no target named, the request is refused here with
	// the typed NoAnswererError naming req.Handler.
	var roleNodes []lad.RoleRecord
	if f.rt.swarm != nil && f.rt.swarm.RoleTable != nil {
		answerers, err := f.rt.swarm.RoleTable.Answerers(role, req.Handler)
		if err != nil {
			return nil, err
		}
		roleNodes = answerers
	}

	targetNodeID := req.TargetNodeId
	if targetNodeID == "" && len(roleNodes) > 0 {
		targetNodeID = roleNodes[0].NodeID
	}

	if targetNodeID == "" {
		return nil, &dispatch.NoAnswererError{Handler: req.Handler}
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
	if len(paths) < 3 {
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
		return nil, dispatch.NotSent(fmt.Errorf("no route to handler %s (role: %s, target: %s)", req.Handler, role, targetNodeID))
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

	// Use the shared scoreRoute helper (route_score.go) so the
	// forwarder and FindRoutes can't drift on the scoring formula
	// (5/20/40/1000 + RTT-override + 100ms/hop): with two copies,
	// tuning one would silently miss the other.
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

	// The grade gate, for every request: only paths at or above the class's
	// floor stay candidates. With none left the request is refused with the
	// best grade on offer and the floor it missed, rather than forwarded over
	// a transport the class does not admit.
	best := GradeF
	graded := paths[:0]
	for _, p := range paths {
		g := SessionGrade(p.session)
		if g > best {
			best = g
		}
		if g >= minGrade {
			graded = append(graded, p)
		}
	}
	if len(graded) == 0 {
		return nil, &GradeTooLowError{Handler: req.Handler, Class: opClass, Current: best, Required: minGrade}
	}
	paths = graded

	// The remaining budget: a path whose need (hopNeed) exceeds what the caller
	// has left is not tried. The forward runs on the caller's context and never
	// gains time, so with no path that fits the request is refused here, before
	// anything is sent, instead of being handed to a hop that cannot finish it.
	if dl, ok := ctx.Deadline(); ok {
		left := time.Until(dl)
		var need time.Duration
		fit := paths[:0]
		for _, p := range paths {
			n := hopNeed(p.session, req.Hops)
			if need == 0 || n < need {
				need = n
			}
			if left >= n {
				fit = append(fit, p)
			}
		}
		if len(fit) == 0 {
			return nil, &RouteDeadlineError{Handler: req.Handler, Remaining: left, Need: need}
		}
		paths = fit
	}

	// Bound fan-out to the ORIGIN. Each probe forwards to a next-hop
	// that itself re-routes; letting every hop fan out across all its paths
	// produced up to ~fanout^MaxRPCHops (≈3^6 = 729) in-flight messages per
	// request. A forwarded (Hops>0) request therefore takes a SINGLE best path,
	// so total in-flight is bounded to ~originFanout × depth (linear). paths is
	// ordered best-first, so paths[:1] keeps the strongest candidate.
	//
	// A handler that is not Repeatable takes the single best path at every
	// hop, the origin included, because two paths could each run it.
	if (req.Hops > 0 || !repeatable) && len(paths) > 1 {
		paths = paths[:1]
	}

	// Launch parallel probes — first response wins. probeResult carries
	// the winning candidate so we can credit the right counter.
	type probeResult struct {
		resp   *pb.RPCResponse
		err    error
		winner fwdCandidate
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

				// The probe runs on probeCtx, derived from the caller's
				// context: its deadline and a sibling probe's first-win
				// cancellation both reach the hop, and the request carries
				// no more time than that context has left.
				keepCallerBudget(probeCtx, fwdReq)
				resp, err := f.callOverMeshSession(probeCtx, fc.session, fwdReq)
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
			if !repeatable {
				return nil, &dispatch.AmbiguousEffectError{Handler: req.Handler, Cause: ctx.Err()}
			}
			return nil, ctx.Err()
		}
	}

	// A handler that is not Repeatable went down one path. Its failure is
	// passed on as it stands when nothing left, so the hop before may try
	// elsewhere, and as ambiguous otherwise.
	if !repeatable {
		if dispatch.Unsent(lastErr) {
			return nil, fmt.Errorf("forward %s: the re-route path refused it before sending: %w", req.Handler, lastErr)
		}
		return nil, &dispatch.AmbiguousEffectError{Handler: req.Handler, Cause: lastErr}
	}
	return nil, fmt.Errorf("forward %s: all %d re-route paths failed: %v", req.Handler, len(paths), lastErr)
}

// callOverMeshSession sends an RPC request via the peer's BidiRPC (Stream 1).
// Falls back to a dynamic stream if no BidiRPC is available.
func (f *runtimeForwarder) callOverMeshSession(ctx context.Context, session aether.Session, req *pb.RPCRequest) (*pb.RPCResponse, error) {
	// Prefer BidiRPC on Stream 1 — avoids opening a dynamic stream.
	remoteID := string(session.RemoteNodeID())

	// 🔴 THE ONLY WRITER OF PER-PEER RPC TRAFFIC. updatePriorities' ladder and
	// the connection scaler's TrafficWeight factor both read what
	// ConnectionManager.RecordRPC writes; see its note in peer_connections.go.
	//
	// 🔑 WHY HERE AND NOWHERE ELSE. This function is the single funnel for
	// every outbound RPC that crosses a peer session: both the BidiRPC branch
	// below and the dynamic-stream fallback pass through it, and both of its
	// call sites in Forward (the fast path and the re-route probe) reach it.
	// One call site keeps a single writer and makes it impossible for the two
	// consumer counters to drift apart.
	//
	// Recorded BEFORE the call, not after: the counters measure OFFERED load on
	// this peer connection, which is what both consumers want. Sizing a
	// connection pool and prioritising against drain must both account for
	// traffic that was attempted and failed — crediting only successes would
	// shrink the pool of exactly the peer whose connections are struggling.
	//
	// ⚠ OUTBOUND ONLY, DELIBERATELY. Inbound RPCs served FOR a peer are that
	// peer's demand on us; TrafficWeight sizes OUR connection target TO them,
	// so our own offered load is the correct signal. Counting inbound here
	// would inflate the target for a chatty client we never call.
	if f.rt != nil && f.rt.connMgr != nil {
		f.rt.connMgr.RecordRPC(remoteID)
	}

	if f.rt.connMgr != nil {
		if bidi, ok := f.rt.connMgr.GetBidiRPC(remoteID); ok {
			resp, err := bidi.Call(ctx, req)
			// A bidi reopening Stream 1 took nothing, so the dynamic stream below is
			// this request's first send.
			if !dispatch.Unsent(err) {
				return resp, err
			}
		}
	}

	// Fallback: a dynamic stream from the session's one ID table (pkg/dispatch), the
	// table HWPCaller's calls draw from, so this forward cannot take an ID one of them
	// holds or walk out of the dynamic band.
	reqBytes, err := pb.MarshalRequest(req)
	if err != nil {
		return nil, dispatch.NotSent(fmt.Errorf("marshal forward request: %w", err))
	}
	rpcStream, err := dispatch.OpenDynamicStream(ctx, session, aether.StreamConfig{
		Reliability: aether.ReliableOrdered,
		Priority:    128,
	})
	if err != nil {
		return nil, dispatch.NotSent(fmt.Errorf("open RPC stream: %w", err))
	}
	// Reset the stream when done: the responder's per-stream ServeMeshStream goroutine
	// blocks on its next Receive until this side's end reaches it, and it never closes
	// the stream itself, so a FIN would leave the stream half-closed in both sessions'
	// tables until aether's idle GC.
	defer dispatch.ReleaseDynamicStream(session, rpcStream)

	if err := rpcStream.Send(ctx, reqBytes); err != nil {
		dispatch.NoteStreamEnd(session, err)
		if dispatch.SendNeverLeft(err) {
			return nil, dispatch.NotSent(fmt.Errorf("forward RPC via aether: %w", err))
		}
		return nil, fmt.Errorf("forward RPC via aether: %w", err)
	}
	respBytes, err := rpcStream.Receive(ctx)
	if err != nil {
		dispatch.NoteStreamEnd(session, err)
		if errors.Is(err, aether.ErrStreamRefused) {
			return nil, dispatch.NotSent(fmt.Errorf("receive forwarded response: %w", err))
		}
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

	// fromRoute marks a candidate produced by the route engine's signed path
	// advertisements rather than derived from LAD latency heuristics.
	//
	// It is a separate field because it is a separate question. Carrying it by
	// setting transport to the literal "route" feeds "route" to
	// remoteTransportGradeForRanking — and "route" is not a transport name, so
	// ParseProtocolStrict rejects it and the candidate scores GradeF. The origin
	// that findNextHops documents as authoritative then ranks below every
	// candidate whose transport string happens to
	// parse. A provenance flag and a transport name do not belong in one value
	// space; keeping them apart is what stops the ranker from grading one as the
	// other.
	fromRoute bool
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
				nodeID: string(p.NextHop),
				// The route advertisement carries a next hop and an RTT sample,
				// not the transport that hop will use, so this stays empty
				// rather than naming a transport nobody measured.
				transport: "",
				rttMs:     int64(p.RTTSampleMs),
				age:       0,
				fromRoute: true,
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
	//
	// 🔴 RANKING-LOCAL FAIL-CLOSED. The transport label comes
	// straight off a gossip-published LAD record, so it is PEER-SUPPLIED. Grading
	// an unparseable or absent label through ParseProtocol's lossy default gave
	// it ProtoNoiseUDP — the zero value, and GradeA, the BEST grade — so a peer
	// that published nothing outranked a peer that published honestly. That is an
	// incentive inversion on hostile input: omitting a field was rewarded with
	// best-hop selection, and `Transport` is `omitempty`, so absent is the
	// ORDINARY wire case.
	//
	// Ranking-last is safe where a GATE would not be: if every candidate lacks a
	// transport they all tie at worst and the ranker still returns them all. A
	// ranker has no outage mode.
	//
	// Deliberately scoped to this call site — ParseProtocol's default is
	// unchanged because five other callers depend on it.
	sort.Slice(candidates, func(i, j int) bool {
		return f.nextHopLess(candidates[i], candidates[j])
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

// nextHopLess is the forwarder's next-hop ordering, named so the ordering
// itself can be exercised. Inlined in the sort it was reachable only through
// findNextHops, which needs a loaded route engine and a populated LAD cache,
// so a test could only re-implement the comparison and would then pass while
// the production order said something else.
func (f *runtimeForwarder) nextHopLess(a, b nextHopCandidate) bool {
	// Route-engine provenance outranks every heuristic score. findNextHops
	// documents the route engine as the authoritative source — dual-signed
	// path advertisements with RFC2439 damping — and LAD latency records as
	// the fallback for partitions no advertisement has reached. Ordering on
	// the transport grade alone inverted that, because a route candidate
	// names no transport and so scored below any candidate that did.
	if a.fromRoute != b.fromRoute {
		return a.fromRoute
	}
	// Local→candidate dispatch grade (our Aether connection quality to this peer)
	iLocal := f.rt.peerGrade(a.nodeID)
	jLocal := f.rt.peerGrade(b.nodeID)
	// Candidate→target transport grade (from LAD latency record)
	iRemote := remoteTransportGradeForRanking(a.transport)
	jRemote := remoteTransportGradeForRanking(b.transport)
	iScore := int(iLocal) + int(iRemote)
	jScore := int(jLocal) + int(jRemote)
	if iScore != jScore {
		return iScore > jScore
	}
	// Then: prefer freshest measurement, then lowest RTT
	iFresh := a.age < 2*time.Minute
	jFresh := b.age < 2*time.Minute
	if iFresh != jFresh {
		return iFresh
	}
	return a.rttMs < b.rttMs
}

// remoteTransportGradeForRanking grades a candidate's LAD-reported transport
// for RANKING ONLY, failing closed on anything it cannot recognise.
//
// The distinction ParseProtocol cannot express is "this really is
// noise-udp" versus "I could not parse this at all" — both return
// ProtoNoiseUDP, which grades A. ParseProtocolStrict reports which one it was,
// and here an unrecognised or empty label is ranked WORST rather than best.
//
// GradeForProtocol's `default: GradeF` arm is unreachable through ParseProtocol
// (it only ever returns one of the five named constants, each with an explicit
// case), so GradeF is named directly. The defensive arm stays where it is.
func remoteTransportGradeForRanking(transport string) Grade {
	proto, known := ParseProtocolStrict(transport)
	if !known {
		return GradeF
	}
	return GradeForProtocol(proto)
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
