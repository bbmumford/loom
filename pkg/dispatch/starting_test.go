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
)

// A starting node's refusal crosses the wire as its own kind: the origin reads it
// back as an error wrapping ErrStarting that sent nothing (Unsent, so HWPCaller
// and the forwarder move to the next route) and is not a handler's answer.
func TestAStartingRefusalCrossesTheWireAsARefusalThatSentNothing(t *testing.T) {
	refusal := fmt.Errorf("%w: test.start.Op", ErrStarting)
	resp := &pb.RPCResponse{Success: false, Error: refusal.Error()}
	StampRefusal(resp, refusal)
	if got := resp.Metadata[refusalKindKey]; got != refusalKindStarting {
		t.Fatalf("the starting refusal is stamped %q, want %q", got, refusalKindStarting)
	}
	err := responseError("test.start.Op", resp)
	if !errors.Is(err, ErrStarting) {
		t.Fatalf("the origin reads %v, want an error wrapping ErrStarting", err)
	}
	if !Unsent(err) {
		t.Error("a starting refusal is not read as one that sent nothing, so an Unsafe call is never failed over")
	}
	if errors.Is(err, ErrHandlerError) {
		t.Error("a starting refusal is read as a handler's answer")
	}
	if err.Error() != refusal.Error() {
		t.Errorf("the origin's error reads %q, want the refusing node's words %q", err, refusal)
	}

	// A failed response carrying no kind stays a handler's answer, whatever its
	// words, so only the refusing node's stamp makes a starting refusal.
	plain := &pb.RPCResponse{Success: false, Error: refusal.Error()}
	if err := responseError("test.start.Op", plain); errors.Is(err, ErrStarting) {
		t.Error("an unstamped failure was read as a starting refusal")
	}
}
