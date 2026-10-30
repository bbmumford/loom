/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

// Package effect holds the effect class an operation declares at
// registration: what running it a second time does to the state it acts on.
//
// The declaration is read in three places. rpc.Handler carries it into the
// node/handlers registry (handlers.EffectDeclarer). node's PeerPublisher
// advertises it beside the operation's FQN in the node's peer record, and
// RoleTable reads it back. dispatch.HWPCaller and node's forwarder ask for it
// before they send, because only a Repeatable operation may be sent down more
// than one route, or sent again after a send whose outcome is unknown.
package effect

// Class is an operation's effect class. The zero value is Unsafe, so an
// operation that declares nothing, a peer record that carries no class for it,
// and a value this package does not know are all held to the one-execution
// rule.
type Class uint8

const (
	// Unsafe: a second execution can make a second change, such as a second
	// mail, a second minted token or a second consumed code. The operation is
	// sent on one route, and sent on another only when the first send provably
	// never left this node.
	Unsafe Class = iota

	// Idempotent: a second execution with the same request leaves the state as
	// one execution left it.
	Idempotent

	// ReadOnly: the operation changes no state.
	ReadOnly
)

// String returns the class's name as Parse reads it.
func (c Class) String() string {
	switch c {
	case Idempotent:
		return "idempotent"
	case ReadOnly:
		return "readonly"
	default:
		return "unsafe"
	}
}

// Parse maps a class name to its Class. A name it does not know is Unsafe and
// reports false.
func Parse(name string) (Class, bool) {
	switch name {
	case "unsafe":
		return Unsafe, true
	case "idempotent":
		return Idempotent, true
	case "readonly":
		return ReadOnly, true
	default:
		return Unsafe, false
	}
}

// Repeatable reports whether running the operation twice leaves the state one
// run leaves: true for Idempotent and ReadOnly, false for Unsafe and for any
// value outside the three declared classes.
func (c Class) Repeatable() bool {
	return c == Idempotent || c == ReadOnly
}

// Least returns the less repeatable of a and b. Callers that hear one
// operation's class from several holders take the least of them, so one holder
// declaring Unsafe, or declaring nothing, holds every call to the one-execution
// rule. A value outside the three declared classes counts as Unsafe.
func Least(a, b Class) Class {
	if !a.Repeatable() || !b.Repeatable() {
		return Unsafe
	}
	if a == Idempotent || b == Idempotent {
		return Idempotent
	}
	return ReadOnly
}
