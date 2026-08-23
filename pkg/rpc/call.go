package rpc

import (
	"context"
	"fmt"
	"reflect"

	sharedDispatch "github.com/bbmumford/loom/pkg/dispatch"
	"google.golang.org/protobuf/proto"
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

// CallNode sends an RPC to ONE NAMED NODE, or fails. It is the targeted
// counterpart of Call, for callers that have already decided WHICH peer must
// serve the request — a placement, a shard owner, a specific runtime — and for
// whom reaching a different node serving the same role is a wrong answer rather
// than a degraded one.
//
// Differences from Call, all deliberate:
//
//  1. NO LOCAL SHORT-CIRCUIT. Call consults tryLocalDispatch first and, if this
//     process hosts the handler, never touches the mesh. That is correct for
//     role dispatch and unsound here: TryLocalDispatch gates purely on
//     HasHandler(name) and knows nothing about node identity, so it would
//     satisfy "call node X" locally whenever this process happens to host X's
//     role — silently, and regardless of whether this process IS node X.
//     pkg/rpc has no access to the local node's ID (that lives in node/, which
//     cannot be imported here), so "bypass unless the target is self" is not
//     expressible at this layer. The safe reading is therefore the strict one:
//     a targeted call always goes over the mesh. The cost is one unnecessary
//     hop when the target happens to be this node; the alternative is a
//     targeting guarantee that quietly is not one.
//
//  2. REQUIRES A TargetedCaller. If the configured caller cannot target (e.g.
//     LocalCaller, which has no mesh peers), this returns ErrNoRouteToNode
//     rather than falling back to Call. A caller that cannot honour the target
//     must say so, not approximate it.
//
// Errors wrap dispatch.ErrNoRouteToNode when the target could not be reached,
// so callers can distinguish "the named node is unreachable" from "the handler
// returned an error" — a distinction placement logic needs and role dispatch
// does not have to make.
func CallNode[Resp proto.Message](ctx context.Context, nodeID, handler string, req proto.Message) (Resp, error) {
	var zero Resp

	if nodeID == "" {
		return zero, fmt.Errorf("targeted call %s: empty nodeID: %w", handler, sharedDispatch.ErrNoRouteToNode)
	}

	payload, err := proto.Marshal(req)
	if err != nil {
		return zero, fmt.Errorf("marshal %s: %w", handler, err)
	}

	// NOTE: tryLocalDispatch is intentionally NOT consulted here — see (1) above.
	caller := sharedDispatch.GetCaller()
	if caller == nil {
		return zero, fmt.Errorf("dispatch caller not initialized")
	}
	targeted, ok := caller.(sharedDispatch.TargetedCaller)
	if !ok {
		return zero, fmt.Errorf("targeted call %s to node %s: caller %T does not implement TargetedCaller: %w",
			handler, nodeID, caller, sharedDispatch.ErrNoRouteToNode)
	}

	role := ExtractRole(handler)
	ctx = bridgeWireIdentity(ctx)
	respBytes, err := targeted.CallNode(ctx, nodeID, role, handler, payload)
	if err != nil {
		return zero, err
	}

	resp := newInstance[Resp]()
	if err := proto.Unmarshal(respBytes, resp); err != nil {
		return zero, fmt.Errorf("unmarshal %s: %w", handler, err)
	}
	return resp, nil
}

// CallNodeRaw is the raw-bytes counterpart of CallNode, mirroring CallRaw.
// Same contract: no local short-circuit, requires a TargetedCaller, and an
// unreachable target is ErrNoRouteToNode rather than a fallback.
func CallNodeRaw(ctx context.Context, nodeID, handler string, payload []byte) ([]byte, error) {
	if nodeID == "" {
		return nil, fmt.Errorf("targeted call %s: empty nodeID: %w", handler, sharedDispatch.ErrNoRouteToNode)
	}
	caller := sharedDispatch.GetCaller()
	if caller == nil {
		return nil, fmt.Errorf("dispatch caller not initialized")
	}
	targeted, ok := caller.(sharedDispatch.TargetedCaller)
	if !ok {
		return nil, fmt.Errorf("targeted call %s to node %s: caller %T does not implement TargetedCaller: %w",
			handler, nodeID, caller, sharedDispatch.ErrNoRouteToNode)
	}
	role := ExtractRole(handler)
	ctx = bridgeWireIdentity(ctx)
	return targeted.CallNode(ctx, nodeID, role, handler, payload)
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
