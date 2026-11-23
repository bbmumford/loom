/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

package dispatch

import (
	"errors"
	"fmt"
	"testing"

	"github.com/ORBTR/aether/rpc/pb"
	rpcscope "github.com/bbmumford/loom/pkg/rpc/scope"
)

// StampFailed records the one kind each refusal carries, and the response it
// stamps is read back (responseError) as the same refusal; RefusalKind names that
// kind, and nothing for an error no stamp records.
func TestStampFailedRecordsEachRefusalsKindAndItReadsBackTyped(t *testing.T) {
	for _, c := range []struct {
		err  error
		kind string
		is   error
	}{
		{&GradeTooLowError{Handler: "t.d.Op", Class: "standard"}, "grade", ErrGradeTooLow},
		{&RouteDeadlineError{Handler: "t.d.Op"}, "deadline", ErrRouteDeadline},
		{&AnswerersDisagreeError{Handler: "t.d.Op", Requirements: []string{"a", "b"}}, "adverts", ErrAnswerersDisagree},
		{&NoAnswererError{Handler: "t.d.Op"}, "no-answerer", ErrNoAnswerer},
		{fmt.Errorf("lost: %w", ErrAmbiguousEffect), "ambiguous", ErrAmbiguousEffect},
		{fmt.Errorf("principal: %w", rpcscope.ErrDenied), "denied", rpcscope.ErrDenied},
		{fmt.Errorf("stale: %w", ErrConflict), "conflict", ErrConflict},
		{errors.New("a handler's own failure"), "", ErrHandlerError},
	} {
		if got := RefusalKind(c.err); got != c.kind {
			t.Errorf("RefusalKind(%v) = %q, want %q", c.err, got, c.kind)
		}
		resp := &pb.RPCResponse{Error: c.err.Error()}
		StampFailed(resp, c.err)
		if back := responseError("t.d.Op", resp); !errors.Is(back, c.is) {
			t.Errorf("%v stamped %v reads back as %v, which is not %v", c.err, resp.Metadata, back, c.is)
		}
	}
	if RefusalKind(nil) != "" {
		t.Error("a nil error has a kind")
	}
	for _, err := range []error{&NoAnswererError{Handler: "t.d.Op"}, rpcscope.ErrDenied} {
		ok := &pb.RPCResponse{Success: true}
		StampFailed(ok, err)
		if len(ok.Metadata) != 0 {
			t.Errorf("a successful response was stamped %v for %v", ok.Metadata, err)
		}
	}
}
