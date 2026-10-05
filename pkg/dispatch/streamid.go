/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package dispatch

import (
	"context"
	"errors"
	"sync"

	"github.com/ORBTR/aether"
	"github.com/bbmumford/loom/core/streams"
)

// Dynamic stream IDs are drawn from [dynamicStreamIDFloor,
// dynamicStreamIDCeiling). Both bounds come from core/streams, the one table
// of stream assignments, so this allocator carries no stream number of its
// own and cannot drift from it. Below the floor are the setup streams
// (gossip, RPC, keepalive, control, reconcile) and BidiRPC; at the ceiling
// and above is aether's application band, where core/streams binds one
// stream per peer pair (StreamSwarm, StreamMeshDB) and the peer claims it by
// ID: an RPC opened on one of those IDs lands on that consumer's stream,
// because aether's createStream hands a duplicate OPEN the stream already
// open under that ID.
const (
	dynamicStreamIDFloor   = streams.DynamicRPCBase
	dynamicStreamIDCeiling = streams.ApplicationBand
)

// errDynamicStreamIDsExhausted is returned when every ID in the dynamic range
// is held by a stream this package opened on the session and that stream is
// still open. The caller fails that one RPC; it never receives an ID in the
// application band.
var errDynamicStreamIDsExhausted = errors.New("dispatch: every dynamic stream ID on this session is held by an open stream")

// sessionStreamIDs is one session's dynamic ID table. held maps an ID to the
// stream opened on it; a nil value is an ID reserved while its OpenStream is
// in flight. next is the offset from the floor at which the next search
// starts, so a freed ID is reused only after every other ID has been tried
// and a late frame of a reset stream is unlikely to meet its successor.
type sessionStreamIDs struct {
	mu   sync.Mutex
	next uint64
	held map[uint64]aether.Stream
}

// sessionStreamTables is keyed by aether.Session. Every allocator on a
// session (HWPCaller.openDynamicStream, StreamPool.Fill, StreamPool.Get)
// draws from the same table, so two callers never open the same ID.
var sessionStreamTables sync.Map // map[aether.Session]*sessionStreamIDs

func streamIDsFor(sess aether.Session) *sessionStreamIDs {
	if v, ok := sessionStreamTables.Load(sess); ok {
		return v.(*sessionStreamIDs)
	}
	v, _ := sessionStreamTables.LoadOrStore(sess, &sessionStreamIDs{held: map[uint64]aether.Stream{}})
	return v.(*sessionStreamIDs)
}

// reserve returns the first ID, searching round-robin from next, that no
// open stream holds, and marks it reserved. An ID whose stream has closed or
// been reset is free again; the entry is dropped as the search passes it.
func (t *sessionStreamIDs) reserve() (uint64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	span := dynamicStreamIDCeiling - dynamicStreamIDFloor
	for i := uint64(0); i < span; i++ {
		id := dynamicStreamIDFloor + (t.next+i)%span
		st, held := t.held[id]
		if held && (st == nil || st.IsOpen()) {
			continue
		}
		t.held[id] = nil
		t.next = (t.next + i + 1) % span
		return id, nil
	}
	return 0, errDynamicStreamIDsExhausted
}

// bind records the stream opened on a reserved ID, or frees the ID when the
// open failed.
func (t *sessionStreamIDs) bind(id uint64, st aether.Stream) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if st == nil {
		delete(t.held, id)
		return
	}
	t.held[id] = st
}

// release frees the ID st holds, if st is still the stream recorded for it.
func (t *sessionStreamIDs) release(st aether.Stream) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if cur, ok := t.held[st.StreamID()]; ok && cur == st {
		delete(t.held, st.StreamID())
	}
}

// openDynamicStreamOn opens a stream with cfg on the next free dynamic ID of
// sess; cfg.StreamID is ignored. Callers hand the stream to
// releaseDynamicStream once its request and response are done.
func openDynamicStreamOn(ctx context.Context, sess aether.Session, cfg aether.StreamConfig) (aether.Stream, error) {
	t := streamIDsFor(sess)
	id, err := t.reserve()
	if err != nil {
		return nil, err
	}
	cfg.StreamID = id
	st, err := sess.OpenStream(ctx, cfg)
	if err != nil {
		t.bind(id, nil)
		return nil, err
	}
	t.bind(id, st)
	return st, nil
}

// releaseDynamicStream resets st and frees its ID. The RPC server
// (node.RPCServer.ServeMeshStream) never closes a dynamic stream, and a FIN
// leaves the stream half-closed in both sessions' stream tables until
// aether's five-minute idle GC; a reset removes it from both at once, so the
// ID is free for the next call.
func releaseDynamicStream(sess aether.Session, st aether.Stream) {
	if st == nil {
		return
	}
	defer func() { _ = recover() }()
	_ = st.Reset(aether.ResetCancel)
	if sess != nil {
		streamIDsFor(sess).release(st)
	}
}

// forgetSession discards the session's ID table. Safe to call more than
// once; a later allocation on the same session starts a new table.
func forgetSession(sess aether.Session) {
	sessionStreamTables.Delete(sess)
}
