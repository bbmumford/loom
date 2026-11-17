/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package rpc

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/bbmumford/loom/pkg/dispatch"
)

// A stream failure that crossed the mesh is reported as its far side reported
// it: HandlerStatus answers the carried status, not the status its rebuilt
// cause would classify as, and StreamFailureOf the carried fields unchanged.
func TestACarriedStreamFailureIsReportedAsItsFarSideReportedIt(t *testing.T) {
	carried := dispatch.StreamFailure{Error: "portClosed: the port closed", Code: "portClosed", Status: http.StatusConflict}
	err := fmt.Errorf("relay: %w", &dispatch.StreamFailureError{
		Handler: "orbtr.test.Stream", Failure: carried, Err: errors.New("portClosed: the port closed"),
	})
	if got := HandlerStatus(err); got != http.StatusConflict {
		t.Fatalf("HandlerStatus = %d, want the carried %d", got, http.StatusConflict)
	}
	if got := StreamFailureOf(err); got != carried {
		t.Fatalf("StreamFailureOf = %+v, want the carried %+v", got, carried)
	}
}

// A local failure is reported with the HTTP body's fields: a 4xx keeps its text
// and a Coded refusal's wire code, a 5xx reports its status text only.
func TestALocalStreamFailureIsReportedWithTheUnaryBodysFields(t *testing.T) {
	coded := Refuse(http.StatusConflict, "portClosed", errors.New("the port closed"))
	if got := StreamFailureOf(coded); got.Status != http.StatusConflict || got.Code != "portClosed" || got.Error != coded.Error() {
		t.Fatalf("a coded refusal reports %+v", got)
	}
	plain := errors.New("the cage went away")
	if got := StreamFailureOf(plain); got.Status != http.StatusInternalServerError || got.Error != http.StatusText(http.StatusInternalServerError) || got.Code != "" {
		t.Fatalf("a handler's failure reports %+v", got)
	}
}
