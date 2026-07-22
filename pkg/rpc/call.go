package rpc

import (
	"context"
	"fmt"
	"reflect"

	"google.golang.org/protobuf/proto"
	sharedDispatch "github.com/bbmumford/loom/pkg/dispatch"
)

// Call sends an RPC over the mesh with type-safe generics — UNLESS the
// handler is hosted locally in this process, in which case it dispatches
// in-process and skips the mesh entirely. Replaces all per-domain Mesh
// structs and dispatch.CallProto.
//
// Usage:
//
//	resp, err := rpc.Call[*pb.CreateUserResponse](ctx, "hstles.identity.CreateUser", req)
func Call[Resp proto.Message](ctx context.Context, handler string, req proto.Message) (Resp, error) {
	var zero Resp

	payload, err := proto.Marshal(req)
	if err != nil {
		return zero, fmt.Errorf("marshal %s: %w", handler, err)
	}

	// Local-handler short-circuit: if this process hosts the handler,
	// dispatch in-process. Avoids serializing twice, opening a stream,
	// and round-tripping a payload that the calling node would have
	// executed in zero hops. Mirrors the check the HTTP /rpc/ bridge
	// already does (rpc/http/bridge.go:85).
	if respBytes, handled, lerr := tryLocalDispatch(ctx, handler, payload); handled {
		if lerr != nil {
			return zero, lerr
		}
		resp := newInstance[Resp]()
		if err := proto.Unmarshal(respBytes, resp); err != nil {
			return zero, fmt.Errorf("unmarshal %s: %w", handler, err)
		}
		return resp, nil
	}

	caller := sharedDispatch.GetCaller()
	if caller == nil {
		return zero, fmt.Errorf("dispatch caller not initialized")
	}

	role := ExtractRole(handler)
	ctx = bridgeWireIdentity(ctx)
	respBytes, err := caller.Call(ctx, role, handler, payload)
	if err != nil {
		return zero, err
	}

	// Instantiate Resp via reflection (generics provide the type)
	resp := newInstance[Resp]()
	if err := proto.Unmarshal(respBytes, resp); err != nil {
		return zero, fmt.Errorf("unmarshal %s: %w", handler, err)
	}
	return resp, nil
}

// CallRaw sends an RPC over the mesh with raw bytes (no type safety) —
// UNLESS the handler is hosted locally, in which case it dispatches
// in-process. Used by the admin domain's ExecuteOperation and other
// dynamic dispatch paths.
func CallRaw(ctx context.Context, handler string, payload []byte) ([]byte, error) {
	// Same local-handler short-circuit as Call. See Call() for rationale.
	if out, handled, lerr := tryLocalDispatch(ctx, handler, payload); handled {
		return out, lerr
	}
	caller := sharedDispatch.GetCaller()
	if caller == nil {
		return nil, fmt.Errorf("dispatch caller not initialized")
	}
	role := ExtractRole(handler)
	ctx = bridgeWireIdentity(ctx)
	return caller.Call(ctx, role, handler, payload)
}

// bridgeWireIdentity copies the caller's authenticated scope-list + userId
// from the rpc context map (stamped by the edge's ContextMirror via
// WithRPCContext — #K-32) onto the dispatch-local keys that buildRPCRequestCtx
// reads when it serializes req.Context for the mesh hop. This is the ONE
// place that bridges the two ctx surfaces: pkg/dispatch cannot import pkg/rpc
// (the rpc→dispatch dependency would cycle), so the rpc layer — which does
// import dispatch — performs the copy on the way out to the wire. Selective
// by design (R-782): only scopes + userId cross, never the whole map. Runs
// only on the mesh path (after the local-dispatch short-circuit), so an
// in-process handler still reads the same values straight from the rpc map.
func bridgeWireIdentity(ctx context.Context) context.Context {
	m := rpcContextMap(ctx)
	if m == nil {
		return ctx
	}
	if sc := ParseScopes(m["scopes"]); len(sc) > 0 {
		ctx = sharedDispatch.WithScopes(ctx, sc)
	}
	if uid := m["userId"]; uid != "" {
		ctx = sharedDispatch.WithUserID(ctx, uid)
	}
	return ctx
}

// newInstance creates a new zero-value instance of a proto.Message pointer type.
func newInstance[T proto.Message]() T {
	var zero T
	t := reflect.TypeOf(zero)
	if t.Kind() == reflect.Ptr {
		return reflect.New(t.Elem()).Interface().(T)
	}
	return zero
}
