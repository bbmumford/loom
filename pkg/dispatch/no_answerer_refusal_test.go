/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package dispatch

import (
	"errors"
	"testing"

	"github.com/ORBTR/aether/rpc/pb"
)

// A relay's refusal of an operation no node advertises reaches the origin as the
// typed refusal naming the operation, never as a handler rejection, and counts
// as unsent, so the origin may try its next route.
func TestANoAnswererRefusalFromALaterHopKeepsItsType(t *testing.T) {
	refused := &pb.RPCResponse{Error: "the hop's sentence"}
	StampRefusal(refused, NotSent(&NoAnswererError{Handler: "svc.Op"}))

	err := responseError("svc.Op", refused)
	if !errors.Is(err, ErrNoAnswerer) || errors.Is(err, ErrHandlerError) {
		t.Fatalf("responseError = %v, want the typed refusal and no handler error", err)
	}
	var typed *NoAnswererError
	if !errors.As(err, &typed) || typed.Handler != "svc.Op" {
		t.Fatalf("responseError = %v, want a *NoAnswererError naming svc.Op", err)
	}
	if !Unsent(err) {
		t.Fatalf("Unsent(%v) = false, want true", err)
	}
	if err.Error() != "the hop's sentence" {
		t.Fatalf("Error() = %q, want the refusing hop's own text", err.Error())
	}
}
