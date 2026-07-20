/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package dispatch

import (
	"context"
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/proto"
)

// CallJSON sends an RPC over the mesh using JSON serialization.
// Used by ORBTR domains whose handlers accept native Go types.
//
// Usage:
//
//	resp, err := dispatch.CallJSON[handlers.GetBundleRequest, handlers.GetBundleResponse](
//	    ctx, caller, "policy", "policy.get_bundle", req,
//	)
func CallJSON[Req any, Resp any](
	ctx context.Context,
	caller Caller,
	role, method string,
	req *Req,
) (*Resp, error) {
	payload, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal %s request: %w", method, err)
	}

	// Local-handler short-circuit — same logic the rpc package uses,
	// reaching every per-domain dispatch shim that goes through this
	// helper. Without this hook, a same-process caller still
	// round-trips over the mesh. Audit critical fix #2.
	var respPayload []byte
	if out, handled, lerr := TryLocalDispatch(ctx, method, payload); handled {
		if lerr != nil {
			return nil, lerr
		}
		respPayload = out
	} else {
		respPayload, err = caller.Call(ctx, role, method, payload)
		if err != nil {
			return nil, err
		}
	}

	var resp Resp
	if err := json.Unmarshal(respPayload, &resp); err != nil {
		return nil, fmt.Errorf("unmarshal %s response: %w", method, err)
	}
	return &resp, nil
}

// CallProto sends an RPC over the mesh using protobuf serialization.
// Used by HSTLES domains whose handlers accept proto-generated types.
//
// Usage:
//
//	resp, err := dispatch.CallProto(
//	    ctx, caller, "auth", "auth.validate_session",
//	    req, func() *pb.ValidateSessionResponse { return &pb.ValidateSessionResponse{} },
//	)
func CallProto[Req proto.Message, Resp proto.Message](
	ctx context.Context,
	caller Caller,
	role, method string,
	req Req,
	newResp func() Resp,
) (Resp, error) {
	payload, err := proto.Marshal(req)
	if err != nil {
		var zero Resp
		return zero, fmt.Errorf("marshal %s request: %w", method, err)
	}

	// Local-handler short-circuit — see CallJSON for rationale.
	var respPayload []byte
	if out, handled, lerr := TryLocalDispatch(ctx, method, payload); handled {
		if lerr != nil {
			var zero Resp
			return zero, lerr
		}
		respPayload = out
	} else {
		respPayload, err = caller.Call(ctx, role, method, payload)
		if err != nil {
			var zero Resp
			return zero, err
		}
	}

	resp := newResp()
	if err := proto.Unmarshal(respPayload, resp); err != nil {
		var zero Resp
		return zero, fmt.Errorf("unmarshal %s response: %w", method, err)
	}
	return resp, nil
}
