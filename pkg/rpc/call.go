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
	return caller.Call(ctx, role, handler, payload)
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
