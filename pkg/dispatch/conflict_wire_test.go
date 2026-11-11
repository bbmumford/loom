/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package dispatch

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/ORBTR/aether/rpc/pb"
	rpcscope "github.com/bbmumford/loom/pkg/rpc/scope"
)

// errStaleRevision stands for a host's refusal of a write whose revision is
// older than the stored one: a conflict with the stored state, which the host
// wraps around ErrConflict, and no refusal of the caller's authority.
var errStaleRevision = fmt.Errorf("%w: host: role revision 3 is stale; the stored revision is 4", ErrConflict)

// A conflict stamped on a failed response comes back from every response path
// as ErrConflict, under the answering node's own text, and stays the handler's
// answer: no refusal of the caller's authority and no route refusal.
func TestAConflictStampedOnAResponseReachesTheCallerAsConflict(t *testing.T) {
	conflicted := &pb.RPCResponse{Id: "req-1", Success: false, Error: errStaleRevision.Error()}
	StampConflict(conflicted, fmt.Errorf("role update: %w", errStaleRevision))
	for _, path := range responsePaths {
		t.Run(path.name, func(t *testing.T) {
			out, err := path.call(conflicted)
			if err == nil || out != nil {
				t.Fatalf("returned (%q, %v) for a conflicted response", out, err)
			}
			if !errors.Is(err, ErrConflict) {
				t.Fatalf("returned %v; want the handler's conflict, matching ErrConflict", err)
			}
			if errors.Is(err, rpcscope.ErrDenied) {
				t.Fatalf("returned %v, which matches scope.ErrDenied; a conflict refuses no authority", err)
			}
			if !errors.Is(err, ErrHandlerError) || isRouteRefusal(err) {
				t.Fatalf("returned %v; want the handler's answer (ErrHandlerError) and no route refusal", err)
			}
			if !strings.Contains(err.Error(), "revision 3 is stale") {
				t.Errorf("the error text %q lost the answering node's own words", err)
			}
		})
	}
}

// A conflict stamped with no text still reads as ErrConflict.
func TestAConflictStampedWithNoTextReadsAsErrConflict(t *testing.T) {
	conflicted := &pb.RPCResponse{Id: "req-1"}
	StampConflict(conflicted, ErrConflict)
	_, err := responsePaths[0].call(conflicted)
	if !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), ErrConflict.Error()) {
		t.Fatalf("returned %v; want ErrConflict under its own text", err)
	}
}

// StampConflict stamps nothing for an error that is no conflict, on a
// successful response, for a conflict that also refuses the caller's
// authority, or over a kind already stamped; StampDenial and StampRefusal
// never stamp the conflict kind. Conflict answers the same way.
func TestOnlyAConflictIsStampedAsOne(t *testing.T) {
	denial := fmt.Errorf("%w: host: the caller holds no grant", rpcscope.ErrDenied)
	deniedConflict := fmt.Errorf("%w: %w", denial, errStaleRevision)
	for _, tc := range []struct {
		name  string
		resp  *pb.RPCResponse
		stamp func(*pb.RPCResponse)
		want  map[string]string
	}{
		{"another error", &pb.RPCResponse{Id: "req-1", Error: "not found"},
			func(r *pb.RPCResponse) { StampConflict(r, errors.New("not found")) }, nil},
		{"a nil error", &pb.RPCResponse{Id: "req-1", Error: "failed"},
			func(r *pb.RPCResponse) { StampConflict(r, nil) }, nil},
		{"a successful response", &pb.RPCResponse{Id: "req-1", Success: true},
			func(r *pb.RPCResponse) { StampConflict(r, errStaleRevision) }, nil},
		{"a conflict that also refuses the caller", &pb.RPCResponse{Id: "req-1", Error: deniedConflict.Error()},
			func(r *pb.RPCResponse) { StampConflict(r, deniedConflict) }, nil},
		{"StampDenial on a conflict", &pb.RPCResponse{Id: "req-1", Error: errStaleRevision.Error()},
			func(r *pb.RPCResponse) { StampDenial(r, errStaleRevision) }, nil},
		{"StampRefusal on a conflict", &pb.RPCResponse{Id: "req-1", Error: errStaleRevision.Error()},
			func(r *pb.RPCResponse) { StampRefusal(r, errStaleRevision) }, nil},
		{"over a denial already stamped", &pb.RPCResponse{Id: "req-1", Error: deniedConflict.Error()},
			func(r *pb.RPCResponse) {
				StampDenial(r, deniedConflict)
				StampConflict(r, errStaleRevision)
			}, map[string]string{refusalKindKey: refusalKindDenied}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.stamp(tc.resp)
			if len(tc.resp.Metadata) != len(tc.want) || tc.resp.Metadata[refusalKindKey] != tc.want[refusalKindKey] {
				t.Fatalf("stamped %v on the response; want %v", tc.resp.Metadata, tc.want)
			}
		})
	}

	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"a conflict", fmt.Errorf("role update: %w", errStaleRevision), true},
		{"a conflict that also refuses the caller", deniedConflict, false},
		{"a denial", denial, false},
		{"another error", errors.New("not found"), false},
		{"nil", nil, false},
	} {
		t.Run("Conflict/"+tc.name, func(t *testing.T) {
			if got := Conflict(tc.err); (got != nil) != tc.want || (got != nil && got != tc.err) {
				t.Fatalf("Conflict(%v) = %v; want the error itself: %v", tc.err, got, tc.want)
			}
		})
	}
}
