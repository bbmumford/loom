/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package scope

// Axis names label the opaque identity axes loom carries on the wire. The wire
// itself is neutral: an AuthenticatedIdentity's Scopes marshal as a repeated
// pb.ScopeAxis{Name,Value} map (pkg/dispatch) and reconstruct generically from
// it (node/rpc.go bindAuthenticatedPrincipal); loom binds no axis to a named
// scalar proto field. The received values are the sending peer's assertion; a
// consumer's ports.PrincipalAdmitter decides whether a transport may carry them.
// The names below describe an axis by its TRANSPORT RELATIONSHIP, not by an
// identity level: they are the vocabulary a producer stamps and a consumer reads
// by, and both sides must agree on the string.
const (
	// AxisTransportPinned is the axis the reference presence policy reads and
	// node/handlers validateScope reconciles against the transport binding;
	// WithTransportPartition stamps it from the server-resolved value.
	AxisTransportPinned = "transport_pinned"

	// AxisWithinTransport is the within-transport axis: it is deliberately NOT
	// the pinned axis, so a narrower scope derives from it within the pinned
	// partition after the forge gate.
	AxisWithinTransport = "within_transport"
)
