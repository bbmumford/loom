/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/pkg/dispatch"
	"github.com/bbmumford/loom/pkg/rpc"
	rpchttp "github.com/bbmumford/loom/pkg/rpc/http"
)

// These tests carry a handler's coded refusal (rpc.Coded: its own 4xx status
// and a stable wire code) across one hop, beside the denial and conflict
// tests: the far node runs a handler exported from an rpc.Registry whose Func
// answers with rpc.Refuse. The origin's caller gets an error carrying the same
// Coded under the far handler's words, the denial or conflict the refusal also
// is stays what errors.Is reads, and the origin's HTTP bridge proxying the
// operation answers the far handler's status with its code beside the words.

// codedCases are the far handler's answers: a refusal of a plan the caller is
// not on (no kind), a coded denial, a coded conflict, a 413, and a failure that
// is not coded.
var codedCases = []struct {
	name             string
	answer           error
	status           int
	code             string
	denied, conflict bool
	words            string
}{
	{"a plan refusal", rpc.Refuse(http.StatusForbidden, "feature_not_in_plan", errors.New("ai.assistant(refer) is not in the caller's plan")),
		http.StatusForbidden, "feature_not_in_plan", false, false, "not in the caller's plan"},
	{"a coded denial", rpc.Refuse(http.StatusForbidden, "grant_missing", errHostRefused),
		http.StatusForbidden, "grant_missing", true, false, "holds no grant"},
	{"a coded conflict", rpc.Refuse(http.StatusConflict, "revision_stale", errStaleRevision),
		http.StatusConflict, "revision_stale", false, true, "revision 3 is stale"},
	{"a payload too large", rpc.Refuse(http.StatusRequestEntityTooLarge, "payload_too_large", errors.New("the payload is 9 MiB over a limit of 8 MiB")),
		http.StatusRequestEntityTooLarge, "payload_too_large", false, false, "over a limit of 8 MiB"},
	{"a failure that is not coded", errors.New("host: the index is being rebuilt"),
		0, "", false, false, "the index is being rebuilt"},
}

// A far handler's coded refusal reaches the origin's caller as the same Coded:
// its status and code, the far handler's words once (the code is not written
// twice), the kind it also is, and still the handler's answer
// (dispatch.ErrHandlerError). A failure that is not coded reaches it with no
// Coded.
func TestAFarHandlersCodedRefusalReachesTheOriginWithItsStatusAndCode(t *testing.T) {
	for _, tc := range codedCases {
		t.Run(tc.name, func(t *testing.T) {
			op := deniedOp(rpc.ScopeNone, tc.answer)
			caller := farServing(t, op)
			_, err := caller.Call(context.Background(), "test", op.FQN(), nil)
			if err == nil {
				t.Fatal("the call succeeded; the far handler refused it")
			}
			c, ok := rpc.CodedOf(err)
			if tc.code == "" {
				if ok {
					t.Fatalf("the origin got a Coded %+v for a failure that was not coded", c)
				}
			} else if !ok || c.Status != tc.status || c.Code != tc.code {
				t.Fatalf("rpc.CodedOf(%v) = %+v, %v; want status %d code %s", err, c, ok, tc.status, tc.code)
			}
			if got := errors.Is(err, rpc.ErrScopeDenied); got != tc.denied {
				t.Errorf("errors.Is(%v, rpc.ErrScopeDenied) = %v, want %v", err, got, tc.denied)
			}
			if got := errors.Is(err, rpc.ErrConflict); got != tc.conflict {
				t.Errorf("errors.Is(%v, rpc.ErrConflict) = %v, want %v", err, got, tc.conflict)
			}
			if !errors.Is(err, dispatch.ErrHandlerError) {
				t.Errorf("the origin got %v, which is not the handler's answer", err)
			}
			if !strings.Contains(err.Error(), tc.words) {
				t.Errorf("the error text %q lost the far node's words %q", err, tc.words)
			}
			if tc.code != "" && strings.Count(err.Error(), tc.code) != 1 {
				t.Errorf("the error text %q names the code %s other than once", err, tc.code)
			}
		})
	}
}

// A far handler's coded refusal, proxied by the origin's HTTP bridge over the
// mesh, answers the far handler's status with its code and words in the body;
// a failure that is not coded answers 400 with its words and no code, as the
// bridge answers every far handler's failure.
func TestAFarCodedRefusalProxiedByTheBridgeAnswersItsStatusAndCode(t *testing.T) {
	for _, tc := range codedCases {
		t.Run(tc.name, func(t *testing.T) {
			op := deniedOp(rpc.ScopeNone, tc.answer)
			caller := farServing(t, op)
			previous := dispatch.GetCaller()
			dispatch.SetCaller(caller)
			t.Cleanup(func() { dispatch.SetCaller(previous) })

			status, body := postThroughBridge(t, proxyFor(t, op), op.FQN())
			want := tc.status
			if tc.code == "" {
				want = http.StatusBadRequest
			}
			if status != want {
				t.Fatalf("the bridge answered %d %s; want %d", status, body, want)
			}
			var got struct{ Error, Code string }
			if err := json.Unmarshal([]byte(body), &got); err != nil {
				t.Fatalf("the body %q is not the bridge's error JSON: %v", body, err)
			}
			if got.Code != tc.code || !strings.Contains(got.Error, tc.words) {
				t.Errorf("the body %q; want code %s and the far handler's words %q", body, tc.code, tc.words)
			}
		})
	}
}

// A coded refusal made by a handler this process hosts, which rpc.CallRaw
// reaches through the node's local dispatcher instead of the mesh, keeps its
// Coded, and the bridge proxying it answers its status with its code.
func TestALocalHostsCodedRefusalKeepsItsStatusAndCode(t *testing.T) {
	tc := codedCases[0]
	op := deniedOp(rpc.ScopeNone, tc.answer)
	source := rpc.NewRegistry()
	if err := source.Register(op); err != nil {
		t.Fatalf("Register: %v", err)
	}
	exported := handlers.NewHandlerRegistry()
	if err := source.ExportToHandlerRegistry(exported); err != nil {
		t.Fatalf("ExportToHandlerRegistry: %v", err)
	}
	adapter := &localDispatcherAdapter{registry: exported}

	_, err := adapter.DispatchBytes(context.Background(), op.FQN(), nil)
	if c, ok := rpc.CodedOf(err); !ok || c.Status != tc.status || c.Code != tc.code {
		t.Fatalf("DispatchBytes returned %v; want the host's Coded %d %s", err, tc.status, tc.code)
	}

	previous := rpc.LocalDispatcherInstance()
	rpc.SetLocalDispatcher(adapter)
	t.Cleanup(func() { rpc.SetLocalDispatcher(previous) })
	if status, body := postThroughBridge(t, proxyFor(t, op), op.FQN()); status != tc.status || !strings.Contains(body, tc.code) {
		t.Fatalf("the bridge proxying a local host answered %d %s; want %d with code %s", status, body, tc.status, tc.code)
	}
	if status, body := postThroughBridge(t, rpchttp.NewBridge(source), op.FQN()); status != tc.status || !strings.Contains(body, tc.code) {
		t.Fatalf("the bridge running the Func answered %d %s; want %d with code %s", status, body, tc.status, tc.code)
	}
}
