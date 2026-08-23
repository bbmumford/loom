/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package grade

import (
	"fmt"

	"github.com/ORBTR/aether"
)

// Grade represents the quality tier of a transport connection.
// Higher numeric value = better quality. HSTLES-specific — derived from
// aether.TransportCapabilities. Other Aether consumers define their own
// quality models.
type Grade int

const (
	GradeF Grade = 0 // No connection / disconnected
	GradeC Grade = 2 // WebSocket, TLS Bootstrap — TCP-based, proxy-compatible
	GradeB Grade = 3 // QUIC, gRPC — reliable with multiplexing
	GradeA Grade = 4 // Noise UDP — direct encrypted UDP, lowest latency
)

// GradeFromCapabilities derives a Grade from Aether transport capabilities.
func GradeFromCapabilities(caps aether.TransportCapabilities) Grade {
	if !caps.NativeReliability && caps.NativeEncryption {
		return GradeA // Noise-UDP: unreliable but encrypted
	}
	if caps.NativeMux {
		return GradeB // QUIC/gRPC: reliable + native mux
	}
	if caps.NativeReliability {
		return GradeC // WebSocket/TCP: reliable only
	}
	return GradeF
}

// SessionGrade returns the Grade for any Aether session based on its transport capabilities.
// This is the clean consumer pattern — derive Grade from what the session can do.
func SessionGrade(session aether.Session) Grade {
	return GradeFromCapabilities(session.Protocol().Capabilities())
}

// ProtocolGrade returns the Grade for an Aether protocol.
func ProtocolGrade(proto aether.Protocol) Grade {
	return GradeFromCapabilities(proto.Capabilities())
}

func (g Grade) String() string {
	switch g {
	case GradeA:
		return "A"
	case GradeB:
		return "B"
	case GradeC:
		return "C"
	case GradeF:
		return "F"
	default:
		return fmt.Sprintf("?(%d)", g)
	}
}

func (g Grade) Weight() float64 {
	switch g {
	case GradeA:
		return 1.0
	case GradeB:
		return 1.2
	case GradeC:
		return 1.5
	default:
		return 2.0
	}
}

// RTT/timeout helpers used to live on Grade. They moved to:
//
//   - quality.RouteClass for expected-RTT bands (same-region /
//     cross-region / inter-continental). Classifying paths by physical
//     distance rather than transport tier means a healthy cross-region
//     noise-udp path is no longer permanently demoted just for having
//     >30 ms RTT.
//   - mesh/node.adaptiveKeepaliveInterval(session) for ping cadence:
//     clamp(2 × SRTT, 5s, 30s). Reads the session's own measured RTT.
//   - mesh/node.adaptiveKeepaliveTimeout(rto) for the per-ping wait:
//     clamp(4 × RTO, 1s, 5s).
//   - mesh/node.adaptiveRPCTimeout(session) for the default RPC
//     deadline: clamp(20 × SRTT, 5s, 30s).

func (g Grade) BetterThan(other Grade) bool { return g > other }
func (g Grade) AtLeast(minimum Grade) bool  { return g >= minimum }
func (g Grade) IsConnected() bool           { return g > GradeF }

// CanCoexistWith reports whether two sessions of these grades may be held to
// the same peer at once: they may exactly when their grades DIFFER, because a
// lower-grade session is useful only as a dormant fallback for a different
// transport class.
//
// 🛑 REGISTRATION DOES NOT CONSULT THIS (#R-1576 ②). registerMeshSession's
// lower-grade arm sits inside `newGrade < oldGrade`, which already implies the
// grades differ — so calling this there was a tautology and the rejection it
// guarded could never fire. That arm and its comment are gone; a strictly
// lower grade is now unconditionally accepted as a dormant fallback.
//
// It is retained because it is EXPORTED on a published module
// (github.com/bbmumford/loom, tags v0.0.1-v0.0.3) with consumers pinned across
// the estate: a workspace census bounds in-tree callers only and says nothing
// about off-wire ones, and the module proxy caches by content permanently.
// Deleting it would be a breaking change to an already-shipped API.
func (g Grade) CanCoexistWith(other Grade) bool { return g != other }

func (g Grade) UpgradeTarget() Grade {
	switch g {
	case GradeF, GradeC:
		return GradeB
	case GradeB:
		return GradeA
	default:
		return GradeF
	}
}

func (g Grade) PreferredProtocol() aether.Protocol {
	switch g {
	case GradeA:
		return aether.ProtoNoise
	case GradeB:
		return aether.ProtoQUIC
	case GradeC:
		return aether.ProtoWebSocket
	default:
		return aether.ProtoUnknown
	}
}

// MaxSupportedGrade estimates the highest grade based on NAT type.
func MaxSupportedGrade(natType aether.NATType) Grade {
	switch natType {
	case aether.NATOpen, aether.NATFullCone:
		return GradeA
	case aether.NATRestricted, aether.NATPortRestricted:
		return GradeA
	case aether.NATSymmetric:
		return GradeB
	case aether.NATBlocked:
		return GradeC
	default:
		return GradeC
	}
}

// OperationClass categorizes RPC operations by transport requirements.
type OperationClass int

const (
	OpClassBulk OperationClass = iota
	OpClassStandard
	OpClassRealtime
	OpClassCritical
)

func (oc OperationClass) MinGrade() Grade {
	switch oc {
	case OpClassBulk:
		return GradeF
	case OpClassStandard:
		return GradeC
	case OpClassRealtime, OpClassCritical:
		return GradeB
	default:
		return GradeC
	}
}

func (oc OperationClass) String() string {
	switch oc {
	case OpClassBulk:
		return "bulk"
	case OpClassStandard:
		return "standard"
	case OpClassRealtime:
		return "realtime"
	case OpClassCritical:
		return "critical"
	default:
		return "unknown"
	}
}
