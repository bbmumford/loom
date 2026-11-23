/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

package handlers

import (
	"context"
	"errors"
	"fmt"
	"testing"

	rpcscope "github.com/bbmumford/loom/pkg/rpc/scope"
)

// Outcome reads one dispatch result into what a caller hands on: the error
// first, whatever response stands beside it; no response as ErrNoResponse; a
// success as its payload; a failed response as its Denied, else its Conflict,
// else an error of its Error text naming neither kind.
func TestOutcomeReadsOneDispatchResult(t *testing.T) {
	stale := fmt.Errorf("%w: host: role revision 3 is stale", rpcscope.ErrConflict)
	denied := fmt.Errorf("%w: host: the caller holds no grant", rpcscope.ErrDenied)
	plain := errors.New("the handler failed")
	for _, tc := range []struct {
		name    string
		resp    *RPCResponse
		err     error
		payload string
		failure error  // the failure handed on as it is, or nil
		text    string // the words of a failure Outcome builds itself
	}{
		{"an error beside a denied response", &RPCResponse{Error: denied.Error(), Denied: denied}, plain, "", plain, ""},
		{"an error beside a success", &RPCResponse{Success: true, Payload: []byte("answered")}, plain, "", plain, ""},
		{"no response and no error", nil, nil, "", ErrNoResponse, ""},
		{"a success", &RPCResponse{Success: true, Payload: []byte("answered")}, nil, "answered", nil, ""},
		{"a refusal of the caller", &RPCResponse{Error: denied.Error(), Denied: denied}, nil, "", denied, ""},
		{"a conflict", &RPCResponse{Error: stale.Error(), Conflict: stale}, nil, "", stale, ""},
		{"a response naming both kinds", &RPCResponse{Error: denied.Error(), Denied: denied, Conflict: stale}, nil, "", denied, ""},
		{"a failure of neither kind", &RPCResponse{Error: "the record is not found"}, nil, "", nil, "the record is not found"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload, failure := Outcome(tc.resp, tc.err)
			if string(payload) != tc.payload {
				t.Errorf("Outcome payload = %q; want %q", payload, tc.payload)
			}
			switch {
			case tc.failure != nil:
				if failure != tc.failure {
					t.Errorf("Outcome failure = %v; want %v as it is", failure, tc.failure)
				}
			case tc.text != "":
				if failure == nil || failure.Error() != tc.text || namesKind(failure) {
					t.Errorf("Outcome failure = %v; want %q naming neither kind", failure, tc.text)
				}
			default:
				if failure != nil {
					t.Errorf("Outcome failure = %v; want none", failure)
				}
			}
		})
	}
}

// The failure Dispatch answers in place of a handler's answer of nothing is
// ErrNoResponse, under an After chain answering nothing as well; an After
// that answers a response of its own over it is left as it answered.
func TestAHandlerAnsweringNothingFailsWithErrNoResponse(t *testing.T) {
	nothing := func(*RPCResponse, error) (*RPCResponse, error) { return nil, nil }
	answered := func(*RPCResponse, error) (*RPCResponse, error) {
		return &RPCResponse{Success: true, Payload: []byte("from the After")}, nil
	}
	for _, tc := range []struct {
		name    string
		after   func(*RPCResponse, error) (*RPCResponse, error)
		payload string
	}{
		{"an After answering nothing", nothing, ""},
		{"an After answering a response of its own", answered, "from the After"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := NewHandlerRegistry()
			reg.Use(&answeringMiddleware{answer: tc.after})
			h := &answeringHandler{resolvedRPCProbe: resolvedRPCProbe{name: "handler.answering.nothing"}, answer: func() (*RPCResponse, error) {
				return nil, nil
			}}
			if err := reg.RegisterRPC(h); err != nil {
				t.Fatalf("RegisterRPC: %v", err)
			}
			payload, err := Outcome(reg.Dispatch(context.Background(), &RPCRequest{Handler: h.name}))
			if tc.payload != "" {
				if err != nil || string(payload) != tc.payload {
					t.Fatalf("Dispatch read as (%q, %v); want the After's answer %q", payload, err, tc.payload)
				}
				return
			}
			if !errors.Is(err, ErrNoResponse) {
				t.Fatalf("Dispatch read as (%q, %v); want ErrNoResponse", payload, err)
			}
		})
	}
}
