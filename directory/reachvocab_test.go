/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package directory

import "testing"

// 🔴 THE VOCABULARY AT THE SEAM, TESTED AGAINST THE PRODUCER'S ACTUAL
// STRINGS RATHER THAN THE ONES MY READER HAPPENED TO HANDLE.
//
// normaliseReachProto + reachPriority translate the reach layer's names
// into the contract vocabulary that ports/directory.go declares on
// ReachAddress.Protocol:
//
//	"noise-udp" > "ws" > "grpc" > "http" priority order
//
// The inputs below are NOT invented. They are the exact strings the
// producers emit, measured:
//
//   - node/lad_reach_bridge.go:258 addressProtoToReach —
//     NOISE_UDP->"udp", WEBSOCKET->"wss", GRPC->"grpc", HTTP->"https"
//   - literal Proto: assignments across the estate — "udp" x13,
//     "tls" x2, "wss" x1, and "ws" written by NOBODY.
//
// 🛑 THAT LAST FACT IS THE WHOLE POINT. reachPriority has a case for
// "ws", a string no producer writes, while "wss" — which one does —
// falls through normaliseReachProto's default passthrough and lands on
// reachPriority's default of 9. My own ladlive_test.go fixtures used
// "ws", so they matched my READER and never the wire. Same defect as
// the serviceName/service_name case, one layer along.

func TestNormaliseReachProtoAcceptsTheProducersRealVocabulary(t *testing.T) {
	cases := []struct {
		raw      string // what a producer actually writes
		want     string // the contract vocabulary
		wantPrio int
		producer string
	}{
		{"udp", "noise-udp", 0, "addressProtoToReach(NOISE_UDP); 13 literal sites"},
		{"noise-udp", "noise-udp", 0, "address table's own name"},
		{"wss", "ws", 1, "addressProtoToReach(WEBSOCKET); 1 literal site"},
		{"ws", "ws", 1, "contract vocabulary itself"},
		{"websocket", "ws", 1, "transportString(WEBSOCKET) in node/address_table.go"},
		{"grpc", "grpc", 2, "addressProtoToReach(GRPC)"},
		{"https", "http", 3, "addressProtoToReach(HTTP)"},
		{"http", "http", 3, "contract vocabulary itself"},
	}

	for _, tc := range cases {
		t.Run(tc.raw, func(t *testing.T) {
			got := normaliseReachProto(tc.raw)
			if got != tc.want {
				t.Errorf("normaliseReachProto(%q) = %q, want %q — producer is %s; "+
					"an unnormalised value flows into ReachAddress.Protocol and "+
					"disagrees with the swarm-backed implementation for the same "+
					"logical transport", tc.raw, got, tc.want, tc.producer)
			}
			if p := reachPriority(got); p != tc.wantPrio {
				t.Errorf("reachPriority(normaliseReachProto(%q)) = %d, want %d — "+
					"9 is the unknown-transport rank, so this transport sorts "+
					"BELOW every ranked one and the documented "+
					"\"noise-udp > ws > grpc > http\" order is inverted",
					tc.raw, p, tc.wantPrio)
			}
		})
	}
}

// The ordering the contract promises must actually hold across the real
// vocabulary, not just for the two transports that happened to be
// spelled the way reachPriority expected.
func TestReachPriorityOrdersTheRealVocabularyAsDocumented(t *testing.T) {
	udp := reachPriority(normaliseReachProto("udp"))
	ws := reachPriority(normaliseReachProto("wss"))    // producer's spelling
	grpc := reachPriority(normaliseReachProto("grpc")) //nolint:gocritic
	http := reachPriority(normaliseReachProto("https"))

	if !(udp < ws && ws < grpc && grpc < http) {
		t.Fatalf("priority order is udp=%d ws=%d grpc=%d http=%d — "+
			"ports/directory.go documents \"noise-udp\" > \"ws\" > \"grpc\" > "+
			"\"http\", and lower sorts first, so this ranking does not implement "+
			"the contract it is written against", udp, ws, grpc, http)
	}
}

// An genuinely unknown transport must still rank last, or the fix above
// would have been "delete the default", which ranks unknowns BEST.
func TestReachPriorityStillRanksUnknownTransportsLast(t *testing.T) {
	unknown := reachPriority(normaliseReachProto("smoke-signal"))
	http := reachPriority(normaliseReachProto("https"))
	if unknown <= http {
		t.Fatalf("unknown transport ranks %d against http %d — an unrecognised "+
			"transport now sorts at or above a known one and would be dialled "+
			"first", unknown, http)
	}
}
