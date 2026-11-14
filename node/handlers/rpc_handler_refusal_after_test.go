/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package handlers

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	rpcscope "github.com/bbmumford/loom/pkg/rpc/scope"
)

// answeringHandler is an RPC handler that answers every call with what answer
// returns, as a handler refusing a call itself does: a failed response naming
// its kind, the form pkg/rpc's registryShim gives a Func's error, or an error.
type answeringHandler struct {
	resolvedRPCProbe
	answer func() (*RPCResponse, error)
}

func (h *answeringHandler) ExecuteRPC(context.Context, *RPCRequest) (*RPCResponse, error) {
	h.ran.Add(1)
	return h.answer()
}

// answeredKind reads a dispatch answer as RPCServer.executeLocal, node's local
// dispatcher and dispatch.LocalCaller read it: the error when there is one,
// otherwise the failed response's Denied and then its Conflict. It returns the
// kind and the text the reader hands on.
func answeredKind(resp *RPCResponse, err error) (kind, text string) {
	carried := err
	switch {
	case err != nil:
	case resp == nil:
		return "no answer", ""
	case resp.Success:
		return "success", ""
	case resp.Denied != nil:
		carried = resp.Denied
	case resp.Conflict != nil:
		carried = resp.Conflict
	default:
		return "neither", resp.Error
	}
	switch {
	case Denial(carried) != nil:
		return "denied", carried.Error()
	case Conflict(carried) != nil:
		return "conflict", carried.Error()
	}
	return "neither", carried.Error()
}

// A handler's own refusal, a conflict with the state it holds or a refusal of
// the caller's authority, keeps its kind through an outer middleware whose
// After answers it with a failure naming neither kind, as a Before refusal
// does: a failed response of the After's own carries the kind under its own
// text, an error of the After's own carries it under the error's text, and an
// After chain answering nothing at all leaves the handler's answer standing.
// The kind is read from the error the chain ended with or, when that names
// none, from the handler's answer, whether the handler answered a failed
// response naming its kind or an error. A kind the After set itself, a success
// it answered, and a handler failure of neither kind are left as the After
// answered.
func TestAnOuterAfterAnsweringAHandlersRefusalKeepsItsKind(t *testing.T) {
	stale := fmt.Errorf("%w: host: role revision 3 is stale", rpcscope.ErrConflict)
	denied := fmt.Errorf("%w: host: the caller holds no grant", rpcscope.ErrDenied)
	both := fmt.Errorf("%w: %w", stale, denied)

	type answer = func() (*RPCResponse, error)
	type after = func(*RPCResponse, error) (*RPCResponse, error)
	conflictResponse := func() (*RPCResponse, error) {
		return &RPCResponse{Success: false, Error: stale.Error(), Conflict: stale}, nil
	}
	deniedResponse := func() (*RPCResponse, error) {
		return &RPCResponse{Success: false, Error: denied.Error(), Denied: denied}, nil
	}
	conflictError := func() (*RPCResponse, error) { return nil, stale }
	deniedError := func() (*RPCResponse, error) { return nil, denied }

	words := func(r *RPCResponse, e error) string {
		if r != nil {
			return r.Error
		}
		return errString(e)
	}
	restated := func(r *RPCResponse, e error) (*RPCResponse, error) {
		return &RPCResponse{Success: false, Error: "masked: " + words(r, e)}, nil
	}
	restatedWithItsError := func(r *RPCResponse, e error) (*RPCResponse, error) {
		return &RPCResponse{Success: false, Error: "masked: " + words(r, e)}, e
	}

	for _, tc := range []struct {
		name    string
		handler answer
		outer   []after
		kind    string
		words   string
	}{
		{"a conflict response restated", conflictResponse, []after{restated}, "conflict", "masked: " + stale.Error()},
		{"a denial response restated", deniedResponse, []after{restated}, "denied", "masked: " + denied.Error()},
		{"a conflict error restated, the error passed on", conflictError, []after{restatedWithItsError}, "conflict", stale.Error()},
		{"a conflict error restated, the error dropped", conflictError, []after{restated}, "conflict", "masked: " + stale.Error()},
		{"a denial error restated, the error dropped", deniedError, []after{restated}, "denied", "masked: " + denied.Error()},
		{"an error of both kinds restated", func() (*RPCResponse, error) { return nil, both }, []after{restated}, "denied", "masked: " + both.Error()},
		{"a conflict answered with a failed response of no text", conflictResponse, []after{func(*RPCResponse, error) (*RPCResponse, error) {
			return &RPCResponse{Success: false}, nil
		}}, "conflict", stale.Error()},
		{"a conflict answered with a plain error", conflictResponse, []after{func(*RPCResponse, error) (*RPCResponse, error) {
			return &RPCResponse{Success: false, Error: "x"}, errors.New("the outer middleware failed")
		}}, "conflict", "the outer middleware failed"},
		{"a conflict answered with a plain error and no response", conflictResponse, []after{func(*RPCResponse, error) (*RPCResponse, error) {
			return nil, errors.New("the outer middleware failed")
		}}, "conflict", "the outer middleware failed"},
		{"a conflict response answered with nothing", conflictResponse, []after{func(*RPCResponse, error) (*RPCResponse, error) {
			return nil, nil
		}}, "conflict", stale.Error()},
		{"a conflict error answered with nothing", conflictError, []after{func(*RPCResponse, error) (*RPCResponse, error) {
			return nil, nil
		}}, "conflict", stale.Error()},
		{"a conflict restated by two outer middlewares", conflictResponse, []after{restated, restated}, "conflict", "masked: masked: " + stale.Error()},
		{"a conflict the After answered as a denial by its error", conflictResponse, []after{func(r *RPCResponse, _ error) (*RPCResponse, error) {
			return &RPCResponse{Success: false, Error: "masked: " + r.Error}, fmt.Errorf("%w: %s", rpcscope.ErrDenied, r.Error)
		}}, "denied", stale.Error()},
		{"a conflict the After answered as a denial on its response", conflictResponse, []after{func(*RPCResponse, error) (*RPCResponse, error) {
			return &RPCResponse{Success: false, Error: denied.Error(), Denied: denied}, nil
		}}, "denied", denied.Error()},
		{"a conflict the After answered as success", conflictResponse, []after{func(*RPCResponse, error) (*RPCResponse, error) {
			return &RPCResponse{Success: true}, nil
		}}, "success", ""},
		{"a failure of neither kind restated", func() (*RPCResponse, error) {
			return &RPCResponse{Success: false, Error: "the record is not found"}, nil
		}, []after{restated}, "neither", "masked: the record is not found"},
		{"an error of neither kind passed on", func() (*RPCResponse, error) {
			return nil, errors.New("the record is not found")
		}, []after{restatedWithItsError}, "neither", "the record is not found"},
		{"a failure of neither kind answered with nothing", func() (*RPCResponse, error) {
			return &RPCResponse{Success: false, Error: "the record is not found"}, nil
		}, []after{func(*RPCResponse, error) (*RPCResponse, error) { return nil, nil }}, "no answer", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := NewHandlerRegistry()
			for _, a := range tc.outer {
				reg.Use(&answeringMiddleware{answer: a})
			}
			h := &answeringHandler{resolvedRPCProbe: resolvedRPCProbe{name: "refusal.under.outer.after"}, answer: tc.handler}
			if err := reg.RegisterRPC(h); err != nil {
				t.Fatalf("RegisterRPC: %v", err)
			}
			resp, err := reg.Dispatch(context.Background(), &RPCRequest{Handler: h.name})
			kind, text := answeredKind(resp, err)
			if kind != tc.kind {
				t.Fatalf("Dispatch = (%+v, %v), read as %s; want %s", resp, err, kind, tc.kind)
			}
			if !strings.Contains(text, tc.words) {
				t.Errorf("the %s is carried under %q; want the words %q", kind, text, tc.words)
			}
			// A failed response naming a kind carries it under its own text,
			// the contract RPCResponse.Denied and RPCResponse.Conflict state.
			if resp != nil && !resp.Success {
				if resp.Conflict != nil && resp.Denied != nil {
					t.Errorf("the response names both kinds: Denied %v, Conflict %v", resp.Denied, resp.Conflict)
				}
				for _, carried := range []error{resp.Denied, resp.Conflict} {
					if carried != nil && carried.Error() != resp.Error {
						t.Errorf("the response carries %q under the text %q", carried, resp.Error)
					}
				}
			}
			if n := h.ran.Load(); n != 1 {
				t.Errorf("the handler ran %d time(s); want once", n)
			}
		})
	}
}

// A handler's refusal the After chain answered with nothing is handed on as
// the handler answered it: the same response, or the same error.
func TestAnAfterChainAnsweringNothingOverAHandlersRefusalLeavesItsAnswer(t *testing.T) {
	stale := fmt.Errorf("%w: host: role revision 3 is stale", rpcscope.ErrConflict)
	refused := &RPCResponse{Success: false, Error: stale.Error(), Conflict: stale}
	for _, tc := range []struct {
		name string
		resp *RPCResponse
		err  error
	}{
		{"a failed response naming its kind", refused, nil},
		{"an error", nil, stale},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := NewHandlerRegistry()
			reg.Use(&answeringMiddleware{answer: func(*RPCResponse, error) (*RPCResponse, error) { return nil, nil }})
			h := &answeringHandler{resolvedRPCProbe: resolvedRPCProbe{name: "refusal.answered.with.nothing"}, answer: func() (*RPCResponse, error) {
				return tc.resp, tc.err
			}}
			if err := reg.RegisterRPC(h); err != nil {
				t.Fatalf("RegisterRPC: %v", err)
			}
			resp, err := reg.Dispatch(context.Background(), &RPCRequest{Handler: h.name})
			if resp != tc.resp || err != tc.err {
				t.Fatalf("Dispatch = (%+v, %v); want the handler's own answer (%+v, %v)", resp, err, tc.resp, tc.err)
			}
		})
	}
}

// An error of its own that an After answers a handler's refusal with comes
// back carrying the refusal's kind under the After's words, and still matches
// the After's error, so a reader looking for that error finds it beside the
// kind.
func TestAnAftersOwnErrorOverAHandlersRefusalCarriesTheKindAndStaysMatchable(t *testing.T) {
	stale := fmt.Errorf("%w: host: role revision 3 is stale", rpcscope.ErrConflict)
	errOuter := errors.New("the outer middleware failed")
	reg := NewHandlerRegistry()
	reg.Use(&answeringMiddleware{answer: func(*RPCResponse, error) (*RPCResponse, error) {
		return nil, fmt.Errorf("after: %w", errOuter)
	}})
	h := &answeringHandler{resolvedRPCProbe: resolvedRPCProbe{name: "refusal.under.failing.after"}, answer: func() (*RPCResponse, error) {
		return &RPCResponse{Success: false, Error: stale.Error(), Conflict: stale}, nil
	}}
	if err := reg.RegisterRPC(h); err != nil {
		t.Fatalf("RegisterRPC: %v", err)
	}
	_, err := reg.Dispatch(context.Background(), &RPCRequest{Handler: h.name})
	if Conflict(err) == nil || !errors.Is(err, stale) {
		t.Fatalf("Dispatch returned %v; want the handler's conflict", err)
	}
	if !errors.Is(err, errOuter) || err.Error() != "after: "+errOuter.Error() {
		t.Errorf("Dispatch returned %q; want the After's error, matchable, under its own words", err)
	}
}
