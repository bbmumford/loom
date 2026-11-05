/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package dispatch

import (
	"errors"
	"fmt"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/adapter"
	"github.com/ORBTR/aether/flow"
	"github.com/bbmumford/loom/pkg/rpc/effect"
)

// LogicalIDKey is the request Context key that carries a call's logical
// request id. buildRPCRequestCtx writes it beside Id on every request this
// package builds, and node's BidiRPC.Call writes it on a request that arrives
// without it, because BidiRPC.Call replaces Id with its channel's own wire id
// for correlation. node's RPCServer reads it as the logical id its dedup key
// is built on, so every attempt of one call joins one entry whichever path it
// took.
const LogicalIDKey = "logicalId"

// ErrNotSent is what an attempt's error unwraps to when the request provably
// reached no handler: it failed to marshal, no stream opened for it, no channel
// took it, or the peer refused its stream at admission (aether.ErrStreamRefused,
// RESET(ResetRefused) from a peer that kept no state and reached no acceptor).
// Only such an attempt may be followed by another for an operation that is not
// effect.Class.Repeatable.
var ErrNotSent = errors.New("the request was not sent")

// SendNeverLeft reports whether err from a stream's Send says the message reached
// no consumer: the stream had already ended with a typed reset (aether.ErrStreamRefused,
// aether.ErrStreamOverrun), the stream's room stayed taken past the session's limit
// (flow.ErrCreditExhausted), or the session cannot carry a message that long
// (adapter.ErrPayloadTooLarge). Each answers before the message's last byte is
// handed on, and a message incomplete at the peer is never delivered. HWPCaller's
// dynamic paths, BidiRPC.Call and node's forwarder read it, to report the attempt as
// NotSent.
func SendNeverLeft(err error) bool {
	return errors.Is(err, aether.ErrStreamRefused) || errors.Is(err, aether.ErrStreamOverrun) ||
		errors.Is(err, flow.ErrCreditExhausted) || errors.Is(err, adapter.ErrPayloadTooLarge)
}

// notSentError keeps the cause's text and type while also unwrapping to
// ErrNotSent.
type notSentError struct{ cause error }

func (e *notSentError) Error() string   { return e.cause.Error() }
func (e *notSentError) Unwrap() []error { return []error{ErrNotSent, e.cause} }

// NotSent marks err as an attempt that never left this node. A nil err stays
// nil.
func NotSent(err error) error {
	if err == nil {
		return nil
	}
	return &notSentError{cause: err}
}

// Unsent reports whether err leaves the request certainly unexecuted: an
// attempt marked NotSent, a circuit that refused it, or a typed grade or
// deadline refusal, which every refusing hop gives before it sends or runs
// anything. HWPCaller fails an Unsafe call over to its next route only on such
// an error, and node's RPCServer and forwarder try another path for one only on
// such an error.
func Unsent(err error) bool {
	return errors.Is(err, ErrNotSent) || errors.Is(err, ErrCircuitOpen) || isRouteRefusal(err)
}

// ErrChannelClosed is what a bidi attempt unwraps to when its channel closed
// after the request was handed to it. The peer may have run it, so HWPCaller
// sends the request again over a dynamic stream only for an
// effect.Class.Repeatable operation.
var ErrChannelClosed = errors.New("the bidi channel closed after the request was handed to it")

// ErrAmbiguousEffect is what every AmbiguousEffectError unwraps to. pkg/rpc
// re-exports it, and the HTTP bridge (pkg/rpc/http classifyMeshError) answers
// it with a 502 it marks not retryable.
var ErrAmbiguousEffect = errors.New("the call was sent and its outcome is unknown")

// AmbiguousEffectError is the outcome of a call to an operation that is not
// effect.Class.Repeatable when it was sent and no answer settles whether the
// handler ran. Cause is what ended the attempt.
//
// TODO: query or park an ambiguous effect, so its caller learns whether it ran
// instead of receiving this error; no operation exists to ask a holder that.
type AmbiguousEffectError struct {
	Handler string
	Cause   error
}

func (e *AmbiguousEffectError) Error() string {
	return fmt.Sprintf("%s: %v: %v", e.Handler, ErrAmbiguousEffect, e.Cause)
}

func (e *AmbiguousEffectError) Unwrap() []error { return []error{ErrAmbiguousEffect, e.Cause} }

// EffectFinder is the optional SessionFinder method HWPCaller asks for an
// operation's effect class before it chooses how many routes to send on. node's
// ConnectionManager implements it from the classes the holders' peer records
// advertise. A finder that does not implement it leaves every operation Unsafe.
type EffectFinder interface {
	HandlerEffect(role, handler string) effect.Class
}

// handlerEffect returns the class the finder reports for handler, and
// effect.Unsafe when the finder reports none.
func (c *HWPCaller) handlerEffect(role, handler string) effect.Class {
	if ef, ok := c.finder.(EffectFinder); ok {
		return ef.HandlerEffect(role, handler)
	}
	return effect.Unsafe
}
