/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/rpc/pb"
	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/pkg/dispatch"
)

// Message type prefixes for bidirectional RPC on Stream 1.
// These allow the read loop to distinguish requests from responses.
const (
	msgTypeRequest  byte = 0x10
	msgTypeResponse byte = 0x20
)

// bidiInflight bounds the number of concurrent handleIncomingRequest
// goroutines per BidiRPC. Without a cap, a chatty (or hostile) peer that
// streams 10k requests in a burst would spawn 10k goroutines, each
// holding the request bytes + handler stack. 256 is comfortably above
// peak observed concurrent dispatch and below memory-pressure thresholds.
const bidiInflight = 256

// bidiServerSendTimeout bounds the per-response Send on the server side.
// Without a deadline, a wedged stream blocks the response-send goroutine
// forever and ties up an inflight semaphore slot. 5 s lets a slow but
// healthy stream finish; longer hangs surface as logged drops.
const bidiServerSendTimeout = 5 * time.Second

// BidiRPC multiplexes bidirectional RPC on a single Aether stream (Stream 1).
// Both sides can initiate requests. Responses are correlated by request ID.
//
// Wire format per message: [type:1][payload...]
// - type=0x10: payload is RPCRequest (binary TLV)
// - type=0x20: payload is RPCResponse (binary TLV)
type BidiRPC struct {
	stream   aether.Stream
	remoteID aether.NodeID
	server   *RPCServer

	// transport is the underlying mesh transport label for this bidi
	// ("noise-udp" / "ws" / "tcp"). Captured at construction so the
	// bidirpc_call_latency_ms histogram can tag samples by
	// transport without re-resolving the session.Protocol() on every
	// Call. Stable for the bidi's lifetime — the session it rides on
	// has exactly one transport.
	transport string

	// scope is one of the three origin/region classes the session
	// registrar assigns as bidiScope in mesh_connection.go (same origin
	// and same region, same origin and another region, another origin),
	// snapshotted from the peer's crossOrigin flag and region at
	// construction. A peer's origin classification can re-evaluate as
	// addresses are learned, but the bidi only sees one classification
	// per lifetime; the histogram absorbs the rare reclassification
	// mid-bidi without harm. Same origin = both peers share one Fly.io
	// organisation's private network; another origin = the path
	// traverses the 6PN hairpin or edge-WS path.
	scope string

	// partition is the transport partition the session this bidi rides on
	// was bound to by its handshake (AcceptMeshConnectionOpts.Partition).
	// handleIncomingRequest stamps it on every request's context, because
	// RPCServer.handleRequest reads the transport partition from there and
	// bindAuthenticatedPrincipal refuses a request's principal when it is
	// empty. "" means the session names none, and a principal sent over it
	// is refused.
	partition string

	// Pending outbound calls awaiting responses, keyed by wire-side ID
	// (the bidi-unique id we stamp at Send time — NOT the caller's id).
	mu      sync.Mutex
	pending map[string]chan *pb.RPCResponse

	// Monotonic counter for generating unique wire-side request IDs.
	callID uint64

	// wireIDPrefix makes wire IDs globally unique, not just unique within this
	// BidiRPC. Stamping req.Id = "b"+counter from a counter that restarts at 0
	// per instance makes "b1"/"b2"… collide across every peer/session. The
	// responder's single process-wide
	// responseCache is keyed on that id, so Peer A's cached success was returned
	// to Peer B's unrelated request. A random per-instance prefix
	// keeps the id unique across bidis while local correlation (b.pending) is
	// unaffected.
	wireIDPrefix string

	// ctx is canceled when the read loop exits. All server-side dispatch
	// goroutines and the per-response Send derive from this — a closing
	// session terminates in-flight handlers cleanly instead of letting
	// them block forever on context.Background().
	ctx       context.Context
	ctxCancel context.CancelFunc

	// done is closed when readLoop exits. Kept distinct from ctx.Done()
	// because IsAlive() is a hot-path check and a channel-close test is
	// cheaper than ctx.Err().
	done chan struct{}

	// inflightSem caps concurrent handleIncomingRequest goroutines so a
	// chatty peer can't exhaust process memory. Acquire by sending into
	// the channel; release by receiving. Capacity = bidiInflight.
	inflightSem chan struct{}

	// inflightCalls is the current number of outbound Call() invocations
	// waiting for a response. Incremented on Call entry, decremented on
	// Call exit (response, ctx.Done, or bidi teardown). Read by
	// InflightCalls() for the bidirpc_inflight_gauge metric.
	// Atomic so monitoring reads don't need to acquire the bidi lock.
	inflightCalls atomic.Int64
}

// NewBidiRPC creates a bidirectional RPC channel on the given stream.
// Starts a read loop that dispatches incoming requests to the server
// and routes responses to pending callers.
//
// transport ("noise-udp" / "ws" / "tcp") and scope (the origin/region
// class described on BidiRPC.scope) tag the bidirpc_call_latency_ms
// histogram so an operator can see whether WS sessions to another origin
// are dragging RPC tail latency vs same-origin noise-UDP. Pass empty strings to skip
// instrumentation (test bidis, ad-hoc construction without a peer
// context).
func NewBidiRPC(stream aether.Stream, remoteID aether.NodeID, server *RPCServer, transport, scope string) *BidiRPC {
	return NewPartitionedBidiRPC(stream, remoteID, server, transport, scope, "")
}

// NewPartitionedBidiRPC is NewBidiRPC for a session its handshake bound to a
// transport partition. Every request the peer sends on it is served under
// that partition, so the principal it carries is judged by the consumer's
// admitter against the transport it arrived on, exactly as a request on an
// accepted RPC session is. AcceptMeshConnection builds every session's bidi
// through it.
func NewPartitionedBidiRPC(stream aether.Stream, remoteID aether.NodeID, server *RPCServer, transport, scope, partition string) *BidiRPC {
	ctx, cancel := context.WithCancel(context.Background())
	// 5 random bytes → a per-instance wire-ID namespace so ids never
	// collide across bidis/peers in the responder's shared dedup cache.
	var pfx [5]byte
	_, _ = rand.Read(pfx[:])
	b := &BidiRPC{
		stream:       stream,
		remoteID:     remoteID,
		server:       server,
		transport:    transport,
		scope:        scope,
		partition:    partition,
		wireIDPrefix: "b" + hex.EncodeToString(pfx[:]) + "-",
		pending:      make(map[string]chan *pb.RPCResponse),
		ctx:          ctx,
		ctxCancel:    cancel,
		done:         make(chan struct{}),
		inflightSem:  make(chan struct{}, bidiInflight),
	}
	go b.readLoop()
	return b
}

// IsAlive reports whether the read loop is still running. Returns false
// once the underlying stream has errored out and readLoop has closed
// b.done. Callers that can fall back to a different transport (dynamic
// stream, alternate route) should consult IsAlive before issuing a Call
// — a dead bidi otherwise returns "bidi stream closed" from Call, and
// the dispatch layer treats that as a routing failure rather than
// falling through to the dynamic-stream path. Cheap: non-blocking
// receive on a channel.
func (b *BidiRPC) IsAlive() bool {
	select {
	case <-b.done:
		return false
	default:
		return true
	}
}

// InflightCalls returns the number of outbound Call() invocations currently
// awaiting a response. Used by the bidirpc_inflight_gauge metric
// surface in MeshMetrics. Atomic load — safe to call from any goroutine.
func (b *BidiRPC) InflightCalls() int64 {
	return b.inflightCalls.Load()
}

// nextWireID allocates a bidi-unique wire-side ID. Always used to stamp
// req.Id before sending so two upstream forwarder probes that happen to
// share the same caller-side ID (e.g. retried RPC on parallel paths)
// can't overwrite each other's pending channel.
func (b *BidiRPC) nextWireID() string {
	n := atomic.AddUint64(&b.callID, 1)
	// Prefix with the per-instance namespace so the id is globally
	// unique (not just unique within this bidi). strconv is ~6x faster than
	// fmt.Sprintf for hot paths.
	return b.wireIDPrefix + strconv.FormatUint(n, 36)
}

// Call sends an RPC request and waits for the correlated response.
// Uses Stream 1's bidirectional channel — no dynamic stream opened.
//
// The caller's req.Id (if any) is preserved on the wire by the
// SERVER's echo (resp.Id = req.Id), but pending-map correlation uses a
// bidi-local wire ID we stamp here. That decouples upstream forwarder
// requests (which reuse the caller's id across hops) from per-bidi
// pending-channel correlation — no two probes can collide on the same
// pending slot regardless of what the caller's req.Id was.
func (b *BidiRPC) Call(ctx context.Context, req *pb.RPCRequest) (*pb.RPCResponse, error) {
	// bidirpc_inflight_gauge: track how many outbound Call()
	// invocations are currently waiting for a response. The gauge is
	// an atomic int64 — increment on entry, decrement when the call
	// exits by any path (response received, ctx cancelled, bidi closed).
	b.inflightCalls.Add(1)
	defer b.inflightCalls.Add(-1)

	// bidirpc_call_latency_ms: full client-side round-trip
	// (marshal → send → wait → correlate). The defer fires regardless
	// of which select arm wins below; successful and bidi-teardown
	// exits feed the success histogram, but DeadlineExceeded exits go
	// to the dedicated bidiTimeout histogram (RecordBidiTimeout) so
	// timed-out calls don't pin the success p99 at the 30s
	// callerRequestTTL. Tagging happens at
	// construction (b.transport / b.scope) so the per-Call cost is just
	// the time.Now() + one map lookup inside whichever Record* fires.
	t0 := time.Now()
	timedOut := false
	defer func() {
		if b.server == nil {
			return
		}
		elapsed := time.Since(t0)
		if timedOut {
			b.server.RecordBidiTimeout(b.transport, b.scope, elapsed)
		} else {
			b.server.RecordBidiLatency(b.transport, b.scope, elapsed)
		}
	}()

	// Id leaves as this channel's wire id, which the pending map correlates the
	// answer by. The logical id rides on in Context under dispatch.LogicalIDKey,
	// written here for a request built without it, so the receiver's dedup key
	// joins this attempt with the call's attempts on other paths.
	originalID := req.Id
	if originalID != "" && req.Context[dispatch.LogicalIDKey] == "" {
		if req.Context == nil {
			req.Context = make(map[string]string, 1)
		}
		req.Context[dispatch.LogicalIDKey] = originalID
	}
	wireID := b.nextWireID()
	req.Id = wireID

	// Register pending response channel keyed by wire ID.
	ch := make(chan *pb.RPCResponse, 1)
	b.mu.Lock()
	b.pending[wireID] = ch
	b.mu.Unlock()

	defer func() {
		b.mu.Lock()
		delete(b.pending, wireID)
		b.mu.Unlock()
	}()

	// Phase decomposition: split the aggregate bidirpc latency
	// into marshal / send / wait so an elevated p50 can be attributed to
	// codec cost vs transport stall vs peer-side dispatch delay. Each
	// phase is timed end-to-end of its respective sub-operation; sum ≈
	// aggregate (modulo pending-map mutex). Recorded only on success;
	// error paths return early without contributing skewed samples.
	marshalStart := time.Now()
	reqBytes, err := pb.MarshalRequest(req)
	if err != nil {
		req.Id = originalID
		return nil, dispatch.NotSent(fmt.Errorf("bidi marshal request: %w", err))
	}

	msg := make([]byte, 1+len(reqBytes))
	msg[0] = msgTypeRequest
	copy(msg[1:], reqBytes)
	marshalDur := time.Since(marshalStart)

	sendStart := time.Now()
	if err := b.stream.Send(ctx, msg); err != nil {
		req.Id = originalID
		return nil, fmt.Errorf("bidi send request: %w", err)
	}
	sendDur := time.Since(sendStart)
	if b.server != nil {
		b.server.RecordBidiPhaseMarshal(b.transport, b.scope, marshalDur)
		b.server.RecordBidiPhaseSend(b.transport, b.scope, sendDur)
	}

	// Wait for correlated response, context cancellation, or stream death.
	// Record the wait phase on EVERY exit arm so the histogram reflects
	// total round-trip distribution including ctx-cancelled and bidi-
	// closed exits. Without this the histogram is skewed toward only
	// the successful-response tail and obscures the real p50.
	waitStart := time.Now()
	defer func() {
		if b.server != nil {
			b.server.RecordBidiPhaseWait(b.transport, b.scope, time.Since(waitStart))
		}
	}()
	select {
	case resp := <-ch:
		// Restore caller's id on the resp for upstream consumers that
		// log/correlate by it. The server echoes our wire id; we swap it
		// back here so callers see what they sent.
		if resp != nil {
			resp.Id = originalID
		}
		req.Id = originalID
		return resp, nil
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			timedOut = true
		}
		req.Id = originalID
		return nil, ctx.Err()
	case <-b.done:
		req.Id = originalID
		return nil, fmt.Errorf("bidi stream closed")
	}
}

// readLoop runs for the lifetime of the stream, demuxing incoming messages.
// On exit it drains every pending caller with a synthetic "stream closed"
// response so no Call survives only because of stream.Send's error path.
func (b *BidiRPC) readLoop() {
	defer func() {
		// Cancel the bidi-scoped ctx so any in-flight server handlers
		// notice the session closed and stop sending. Close done so
		// IsAlive() flips false. Drain the pending map so callers
		// waiting on a channel get a deterministic error instead of
		// hanging until their own ctx fires.
		b.ctxCancel()
		close(b.done)
		//
		// Each drained response is stamped ambiguous (dispatch.StampRefusal):
		// its request was handed to the peer, which may have run it, so
		// HWPCaller must read it as an unknown outcome and never as a handler's
		// rejection.
		b.mu.Lock()
		for id, ch := range b.pending {
			closed := &pb.RPCResponse{Id: id, Success: false, Error: "bidi stream closed"}
			dispatch.StampRefusal(closed, &dispatch.AmbiguousEffectError{Cause: dispatch.ErrChannelClosed})
			// Non-blocking send; receivers drain via select with b.done.
			select {
			case ch <- closed:
			default:
			}
		}
		b.mu.Unlock()
	}()

	for {
		// This framing relies on aether's Stream.Receive being
		// message-oriented — exactly one Send unit per Receive, boundaries
		// preserved. That is aether's documented Stream contract for the
		// ReliableOrdered stream this bidi rides (Stream 1), and the TCP
		// transport preserves those boundaries too. A transport that
		// splits/coalesces stream payloads would make data[0] an unreliable
		// type tag, so adding one requires switching this loop to a
		// length-prefixed reassembly framing.
		data, err := b.stream.Receive(b.ctx)
		if err != nil {
			log.Printf("[BIDI-RPC] %s: stream %d read loop exit: %v",
				b.remoteID.Short(), b.stream.StreamID(), err)
			return
		}

		if len(data) < 2 {
			continue
		}

		msgType := data[0]
		payload := data[1:]

		switch msgType {
		case msgTypeRequest:
			// Spawn a goroutine per inbound request so a slow handler can't
			// head-of-line block the readLoop. CRITICAL: the semaphore
			// acquire MUST happen INSIDE the spawned goroutine, NOT in
			// readLoop itself.
			//
			// Why: readLoop demultiplexes BOTH msgTypeRequest (incoming)
			// AND msgTypeResponse (responses to our outgoing Call()s).
			// If 256 slow request-handlers are inflight, an inline
			// semaphore acquire here would BLOCK readLoop entirely —
			// outgoing Call() responses would queue in the underlying
			// stream buffer, never demultiplexed, and every BidiRPC
			// Caller would hang until ctx fires: BidiRPC stream 1's
			// inFlight grows, rack.fackAge climbs to minutes, and TLP
			// probes fruitlessly. An inline acquire removes request-side
			// HOL but creates response-side HOL.
			//
			// In-goroutine acquire decouples readLoop from handler
			// pressure: readLoop spawns + continues; the goroutine
			// may queue on the semaphore but readLoop keeps draining
			// the stream, so msgTypeResponse messages flow even when
			// every handler slot is busy. Worst case bounds:
			// - if N concurrent requests arrive: N goroutines spawn,
			// min(256, N) acquire immediately and run, rest park
			// on the semaphore. Memory cost per parked goroutine
			// is ~2KB stack + the payload slice; ~256 KB at full
			// saturation per BidiRPC. Acceptable vs the wedge.
			// - if peer is malicious + opens 100k streams at once:
			// bounded by aether-level stream caps + per-session
			// scheduler limits, not by us here.
			go func(p []byte) {
				select {
				case b.inflightSem <- struct{}{}:
				case <-b.ctx.Done():
					return
				}
				b.dispatchRequest(p)
			}(payload)
		case msgTypeResponse:
			b.handleIncomingResponse(payload)
		default:
			log.Printf("[BIDI-RPC] %s: unknown message type 0x%02x on stream %d",
				b.remoteID.Short(), msgType, b.stream.StreamID())
		}
	}
}

// dispatchRequest is the goroutine body for an inbound request. Wraps
// handleIncomingRequest with semaphore release + outer panic recover
// (defence in depth: handleIncomingRequest's IIFE recovers handler
// panics but anything outside it — marshal, stream.Send — is the outer
// recover's job).
func (b *BidiRPC) dispatchRequest(payload []byte) {
	defer func() {
		<-b.inflightSem
		if r := recover(); r != nil {
			log.Printf("[BIDI-RPC] %s: dispatch panic: %v", b.remoteID.Short(), r)
		}
	}()
	b.handleIncomingRequest(payload)
}

// handleIncomingRequest deserializes and dispatches an RPC request, then
// sends the response. Recovers from handler panics to prevent crashing
// the entire process; protects against nil resp from a panicked handler
// and against context.Background() smuggling on the response send.
//
// Diagnostic trace (DEBUG=mesh.node.rpc) covers the full lifecycle —
// arrival, dispatch start/end with duration, send result — so a hung
// handler is unambiguous when reading fly logs.
func (b *BidiRPC) handleIncomingRequest(payload []byte) {
	req, err := pb.UnmarshalRequest(payload)
	if err != nil {
		log.Printf("[BIDI-RPC] %s: unmarshal request error: %v", b.remoteID.Short(), err)
		return
	}

	tArrive := time.Now()
	dbgRPC.Printf("BidiRPC request from %s: handler=%s id=%s", b.remoteID.Short(), req.Handler, req.Id)

	// Dispatch to handler with panic recovery. resp is captured by the
	// IIFE; if handleRequest panics the deferred recover stamps a synthetic
	// response. If handleRequest returns nil (defensive: future bug or
	// off-by-one in a refactor) we synthesise a response below — the
	// outer code MUST NOT deref a nil resp.
	var resp *pb.RPCResponse
	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[BIDI-RPC] %s: handler panic for %s: %v", b.remoteID.Short(), req.Handler, r)
				resp = &pb.RPCResponse{
					Id:      req.Id,
					Success: false,
					Error:   fmt.Sprintf("handler panic: %v", r),
				}
			}
		}()
		// Derive the handler ctx from the bidi-scoped ctx so a session
		// closing mid-handler propagates cancellation. Apply the
		// request's TimeoutNs as a hard cap.
		// The delivering peer scopes this request's dedup entry.
		hctx := withCallerNode(b.ctx, string(b.remoteID))
		if b.partition != "" {
			hctx = handlers.WithTransportPartition(hctx, b.partition)
		}
		if req.TimeoutNs > 0 {
			var hcancel context.CancelFunc
			hctx, hcancel = context.WithTimeout(hctx, time.Duration(req.TimeoutNs))
			defer hcancel()
		}
		resp = b.server.handleRequest(hctx, req)
	}()
	if resp == nil {
		// Defensive: handleRequest must always return non-nil. If a
		// future refactor breaks that invariant, surface as an error
		// response instead of panicking on the field reads below.
		resp = &pb.RPCResponse{
			Id:      req.Id,
			Success: false,
			Error:   "bidi: handleRequest returned nil response",
		}
		log.Printf("[BIDI-RPC] %s: nil resp for %s id=%s", b.remoteID.Short(), req.Handler, req.Id)
	}

	dur := time.Since(tArrive)
	dbgRPC.Printf("BidiRPC request from %s: handler=%s id=%s done dur=%v success=%v err=%q",
		b.remoteID.Short(), req.Handler, req.Id, dur, resp.Success, resp.Error)
	resp.Id = req.Id // Ensure response ID matches request for correlation

	respBytes, err := pb.MarshalResponse(resp)
	if err != nil {
		log.Printf("[BIDI-RPC] %s: marshal response error: %v", b.remoteID.Short(), err)
		return
	}

	msg := make([]byte, 1+len(respBytes))
	msg[0] = msgTypeResponse
	copy(msg[1:], respBytes)

	// Bound the Send so a wedged stream can't hold a dispatch goroutine
	// forever. ctx is bidi-scoped (cancels on session close) and the
	// per-response timeout caps the per-send wait. Without the cap the
	// goroutine + its semaphore slot leak until ctx (= readLoop exit)
	// fires, which on a healthy peer is "indefinitely".
	sendCtx, sendCancel := context.WithTimeout(b.ctx, bidiServerSendTimeout)
	defer sendCancel()
	if err := b.stream.Send(sendCtx, msg); err != nil {
		log.Printf("[BIDI-RPC] %s: send response error for %s id=%s: %v",
			b.remoteID.Short(), req.Handler, req.Id, err)
		return
	}
	dbgRPC.Printf("BidiRPC response sent to %s: handler=%s id=%s bytes=%d",
		b.remoteID.Short(), req.Handler, req.Id, len(msg))
}

// handleIncomingResponse routes a response to the pending caller by ID.
func (b *BidiRPC) handleIncomingResponse(payload []byte) {
	resp, err := pb.UnmarshalResponse(payload)
	if err != nil {
		log.Printf("[BIDI-RPC] %s: unmarshal response error: %v", b.remoteID.Short(), err)
		return
	}

	b.mu.Lock()
	ch, ok := b.pending[resp.Id]
	b.mu.Unlock()

	if ok {
		select {
		case ch <- resp:
		default:
		}
	} else {
		dbgBidiRPC.Printf("%s: orphan response id=%s", b.remoteID.Short(), resp.Id)
	}
}
