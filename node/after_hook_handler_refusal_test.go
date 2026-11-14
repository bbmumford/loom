/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/ORBTR/aether"

	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/pkg/dispatch"
	"github.com/bbmumford/loom/pkg/rpc"
)

// A handler's own conflict, and its own refusal of the caller's authority,
// under an outer middleware whose After answers a failed response with one of
// its own reaches the origin of a call across the mesh with its kind, under
// the outer response's words over the handler's, and the bridge answers the
// conflict 409 and the refusal 403; dispatch.LocalCaller and the node's local
// dispatcher return each with its kind too. A handler's failure of neither
// kind stays a handler failure, which the bridge answers 400.
func TestAHandlersRefusalUnderAnOuterAfterReachesTheOriginWithItsKind(t *testing.T) {
	for _, tc := range []struct {
		name             string
		answer           error
		conflict, denied bool
		status           int
		words            string
	}{
		{"the handler's conflict", errStaleRevision, true, false, http.StatusConflict, "revision 3 is stale"},
		{"the handler's refusal of the caller", errHostRefused, false, true, http.StatusForbidden, "holds no grant"},
		{"the handler's other failure", errors.New("host: the record is not found"), false, false, http.StatusBadRequest, "not found"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op := deniedOp(rpc.ScopeNone, tc.answer)
			reg := guardedRegistry(t, op, &restatingAfter{})
			caller := dispatch.NewHWPCaller(&oneHopFinder{session: &servedSession{
				hopSession: hopSession{remote: "far", proto: aether.ProtoQUIC},
				server:     NewRPCServer(reg),
			}})
			t.Cleanup(caller.Close)

			carries := func(surface string, err error) {
				t.Helper()
				if err == nil {
					t.Fatalf("%s: the call succeeded; the handler refused it", surface)
				}
				if got := errors.Is(err, rpc.ErrConflict); got != tc.conflict {
					t.Errorf("%s: errors.Is(%v, rpc.ErrConflict) = %v, want %v", surface, err, got, tc.conflict)
				}
				if got := errors.Is(err, rpc.ErrScopeDenied); got != tc.denied {
					t.Errorf("%s: errors.Is(%v, rpc.ErrScopeDenied) = %v, want %v", surface, err, got, tc.denied)
				}
				if !strings.Contains(err.Error(), "restated: ") || !strings.Contains(err.Error(), tc.words) {
					t.Errorf("%s: %v; want the outer middleware's words over the handler's %q", surface, err, tc.words)
				}
			}

			_, err := caller.Call(context.Background(), "test", op.FQN(), nil)
			carries("the far caller", err)

			previous := dispatch.GetCaller()
			dispatch.SetCaller(caller)
			t.Cleanup(func() { dispatch.SetCaller(previous) })
			if status, body := postThroughBridge(t, proxyFor(t, op), op.FQN()); status != tc.status {
				t.Errorf("the bridge answered %d %s; want %d", status, body, tc.status)
			}

			_, err = dispatch.NewLocalCaller(reg).Call(context.Background(), "test", op.FQN(), nil)
			carries("dispatch.LocalCaller", err)

			_, err = (&localDispatcherAdapter{registry: reg}).DispatchBytes(context.Background(), op.FQN(), nil)
			carries("the local dispatcher", err)
		})
	}
}

// answeringAfter passes its Before and answers After with what answer returns
// over the response and error it is handed.
type answeringAfter struct {
	answer func(*handlers.RPCResponse, error) (*handlers.RPCResponse, error)
}

func (*answeringAfter) Name() string { return "answering-after" }
func (*answeringAfter) Before(ctx context.Context, _ string, _ *handlers.RPCRequest) (context.Context, error) {
	return ctx, nil
}
func (m *answeringAfter) After(_ context.Context, _ string, resp *handlers.RPCResponse, err error) (*handlers.RPCResponse, error) {
	return m.answer(resp, err)
}

// A handler's conflict the outer middleware's After answered with an error of
// its own naming no kind, or with no response and no error at all, still
// reaches the origin of a call across the mesh, dispatch.LocalCaller and the
// node's local dispatcher as a conflict.
func TestAHandlersConflictUnderAnAfterAnsweringAnErrorOrNothingStaysAConflict(t *testing.T) {
	for _, tc := range []struct {
		name   string
		answer func(*handlers.RPCResponse, error) (*handlers.RPCResponse, error)
		words  string
	}{
		{"an error of its own beside a failed response", func(*handlers.RPCResponse, error) (*handlers.RPCResponse, error) {
			return &handlers.RPCResponse{Success: false, Error: "x"}, errors.New("the outer middleware failed")
		}, "the outer middleware failed"},
		{"an error of its own and no response", func(*handlers.RPCResponse, error) (*handlers.RPCResponse, error) {
			return nil, errors.New("the outer middleware failed")
		}, "the outer middleware failed"},
		{"no response and no error", func(*handlers.RPCResponse, error) (*handlers.RPCResponse, error) {
			return nil, nil
		}, "revision 3 is stale"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op := deniedOp(rpc.ScopeNone, errStaleRevision)
			reg := guardedRegistry(t, op, &answeringAfter{answer: tc.answer})
			caller := dispatch.NewHWPCaller(&oneHopFinder{session: &servedSession{
				hopSession: hopSession{remote: "far", proto: aether.ProtoQUIC},
				server:     NewRPCServer(reg),
			}})
			t.Cleanup(caller.Close)

			_, far := caller.Call(context.Background(), "test", op.FQN(), nil)
			_, local := dispatch.NewLocalCaller(reg).Call(context.Background(), "test", op.FQN(), nil)
			_, own := (&localDispatcherAdapter{registry: reg}).DispatchBytes(context.Background(), op.FQN(), nil)
			for surface, err := range map[string]error{"the far caller": far, "dispatch.LocalCaller": local, "the local dispatcher": own} {
				if !errors.Is(err, rpc.ErrConflict) || errors.Is(err, rpc.ErrScopeDenied) {
					t.Errorf("%s returned %v; want the handler's conflict", surface, err)
					continue
				}
				if !strings.Contains(err.Error(), tc.words) {
					t.Errorf("%s returned %v; want the words %q", surface, err, tc.words)
				}
			}
		})
	}
}
