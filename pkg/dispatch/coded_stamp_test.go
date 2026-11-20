/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package dispatch

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/ORBTR/aether/rpc/pb"
	rpcscope "github.com/bbmumford/loom/pkg/rpc/scope"
)

// failedAs is the failed response RPCServer.executeLocal builds for err and
// stamps: the failure's words, then its kind, then a coded refusal's status
// and code.
func failedAs(err error) *pb.RPCResponse {
	resp := &pb.RPCResponse{Success: false, Error: err.Error()}
	StampDenial(resp, err)
	StampConflict(resp, err)
	StampCoded(resp, err)
	return resp
}

// A coded refusal stamped on a failed response is read back by responseError
// as the same Coded, under the same text, unwrapping to the kind it also is
// and to ErrHandlerError.
func TestACodedRefusalStampedOnTheWireIsReadBackAsTheSameCoded(t *testing.T) {
	denied := fmt.Errorf("%w: host: no grant", rpcscope.ErrDenied)
	conflict := fmt.Errorf("%w: host: revision 3 is stale", rpcscope.ErrConflict)
	for _, tc := range []struct {
		name             string
		err              error
		status           int
		code             string
		denied, conflict bool
	}{
		{"no kind", rpcscope.Refuse(http.StatusForbidden, "feature_not_in_plan", errors.New("not in the plan")), http.StatusForbidden, "feature_not_in_plan", false, false},
		{"a denial", rpcscope.Refuse(http.StatusForbidden, "grant_missing", denied), http.StatusForbidden, "grant_missing", true, false},
		{"a conflict", rpcscope.Refuse(http.StatusConflict, "revision_stale", conflict), http.StatusConflict, "revision_stale", false, true},
		{"wrapped by the host", fmt.Errorf("test.Op: %w", rpcscope.Refuse(http.StatusTooManyRequests, "quota_spent", errors.New("the day's quota is spent"))), http.StatusTooManyRequests, "quota_spent", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := responseError("test.Op", failedAs(tc.err))
			c, ok := rpcscope.CodedOf(got)
			if !ok || c.Status != tc.status || c.Code != tc.code {
				t.Fatalf("responseError = %v (Coded %+v, %v); want status %d code %s", got, c, ok, tc.status, tc.code)
			}
			if got.Error() != tc.err.Error() {
				t.Errorf("the rebuilt text %q is not the handler's %q", got, tc.err)
			}
			if errors.Is(got, rpcscope.ErrDenied) != tc.denied || errors.Is(got, ErrConflict) != tc.conflict {
				t.Errorf("responseError = %v: denied %v conflict %v; want %v %v", got, errors.Is(got, rpcscope.ErrDenied), errors.Is(got, ErrConflict), tc.denied, tc.conflict)
			}
			if !errors.Is(got, ErrHandlerError) {
				t.Errorf("responseError = %v, which is not the handler's answer", got)
			}
		})
	}
}

// StampCoded leaves a response as it is when there is no coded 4xx refusal to
// record, or when the response already carries a route refusal's kind, which
// is the hop's outcome and not the handler's; and responseError rebuilds no
// Coded from a status outside 4xx.
func TestStampCodedRecordsOnlyAHandlersCoded4xx(t *testing.T) {
	plain := failedAs(errors.New("host: the index is being rebuilt"))
	if _, ok := rpcscope.CodedOf(responseError("test.Op", plain)); ok || plain.Metadata[refusalCodeKey] != "" {
		t.Fatalf("a failure that is not coded was stamped %v", plain.Metadata)
	}
	fault := &rpcscope.Coded{Status: http.StatusInternalServerError, Code: "internal", Cause: errors.New("boom")}
	if resp := failedAs(fault); resp.Metadata[refusalCodeKey] != "" {
		t.Fatalf("a 500 Coded was stamped %v", resp.Metadata)
	}
	route := &pb.RPCResponse{Success: false, Error: "no route"}
	StampRefusal(route, &NoAnswererError{Handler: "test.Op"})
	StampCoded(route, rpcscope.Refuse(http.StatusForbidden, "grant_missing", errors.New("no grant")))
	if route.Metadata[refusalCodeKey] != "" {
		t.Fatalf("a route refusal's response was stamped a handler's code %v", route.Metadata)
	}
	succeeded := &pb.RPCResponse{Success: true}
	StampCoded(succeeded, rpcscope.Refuse(http.StatusForbidden, "grant_missing", errors.New("no grant")))
	if succeeded.Metadata != nil {
		t.Fatalf("a response that succeeded was stamped %v", succeeded.Metadata)
	}
	forged := &pb.RPCResponse{Success: false, Error: "x: y", Metadata: map[string]string{refusalStatusKey: "503", refusalCodeKey: "x"}}
	if _, ok := rpcscope.CodedOf(responseError("test.Op", forged)); ok {
		t.Fatal("responseError rebuilt a Coded from a 503 status")
	}
}
