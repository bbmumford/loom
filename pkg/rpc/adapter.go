package rpc

import (
	"context"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/bbmumford/loom/node/handlers"
)

// ExportToHandlerRegistry registers all handlers from the rpc.Registry
// into an existing handlers.HandlerRegistry. This bridges the new
// registration model with the existing RPCServer / task-executor dispatch
// paths so an endpoint that migrates a domain into rpc.Registry (via a
// canonical register.go) keeps working on both wire surfaces.
//
// Each handler is wrapped in a shim that implements handlers.RPCHandler
// AND handlers.TaskHandler — the second interface is required so mesh task
// executors (execution.Executor) can resolve the shim after the mesh
// task path type-asserts GetMeta result to TaskHandler. Domains dispatched
// exclusively via RPC (identity, notify, auth …) don't rely on this branch
// but paying its cost is free because ExecuteTask is a pass-through to
// ExecuteRPC — no allocations beyond the RPCRequest struct.
func (r *Registry) ExportToHandlerRegistry(hr *handlers.HandlerRegistry) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, h := range r.handlers {
		shim := newRegistryShim(h)
		if err := hr.RegisterRPC(shim); err != nil {
			return err
		}
	}
	return nil
}

// registryShim wraps an rpc.Handler to implement handlers.HandlerMeta,
// handlers.RPCHandler and handlers.TaskHandler. The task interface is
// required so mesh execution.Executor (which type-asserts GetMeta result
// to TaskHandler before invoking) can dispatch handlers registered via
// rpc.Registry — e.g. hstles.maintenance.RotateRecord published by the
// rotation scanner as a mesh task.
type registryShim struct {
	handler *Handler
}

func newRegistryShim(h *Handler) *registryShim {
	return &registryShim{handler: h}
}

// HandlerMeta interface
func (s *registryShim) Name() string                      { return s.handler.FQN() }
func (s *registryShim) Role() string                      { return s.handler.Role() }
func (s *registryShim) RequiresAuth() bool                { return !s.handler.IsPublic() }
func (s *registryShim) AllowedAuthTypes() []string        { return nil }
func (s *registryShim) Scopes() []string                  { return nil }
func (s *registryShim) TenantScope() handlers.TenantScope { return s.handler.Scope }
func (s *registryShim) AllowedTenants() []string          { return nil }

// RPCHandler interface — decodes req.Payload into the handler's request
// prototype, calls the handler, and marshals the response. Payload
// encoding is auto-detected: bytes starting with '{' are protojson (the
// mesh task path stuffs Task.Payload as json.RawMessage), any other
// leading byte is treated as protobuf binary (the standard mesh RPC path).
// This lets one shim serve both dispatch surfaces without asking the
// caller which encoding they used.
func (s *registryShim) ExecuteRPC(ctx context.Context, req *handlers.RPCRequest) (*handlers.RPCResponse, error) {
	start := time.Now()

	protoReq := NewInstance(s.handler.Request)
	if len(req.Payload) > 0 {
		if err := unmarshalPayload(req.Payload, protoReq); err != nil {
			return &handlers.RPCResponse{
				Success: false,
				Error:   "unmarshal: " + err.Error(),
				Latency: time.Since(start),
			}, nil
		}
	}

	protoResp, err := s.handler.Func(ctx, protoReq)
	if err != nil {
		return &handlers.RPCResponse{
			Success: false,
			Error:   err.Error(),
			Latency: time.Since(start),
		}, nil
	}

	respBytes, err := proto.Marshal(protoResp)
	if err != nil {
		return &handlers.RPCResponse{
			Success: false,
			Error:   "marshal: " + err.Error(),
			Latency: time.Since(start),
		}, nil
	}

	return &handlers.RPCResponse{
		Success: true,
		Payload: respBytes,
		Latency: time.Since(start),
	}, nil
}

// TaskHandler interface — mesh task executors call ExecuteTask after
// resolving GetMeta and type-asserting to TaskHandler. We reuse the RPC
// path so the wire semantics stay identical (same payload decoding, same
// handler invocation, same response marshaling). The returned TaskResult
// mirrors what BaseHandler.ExecuteTask does when its executeTaskFunc is
// nil — keeps the migration purely cosmetic at the wire level.
func (s *registryShim) ExecuteTask(ctx context.Context, task *handlers.Task) (*handlers.TaskResult, error) {
	resp, err := s.ExecuteRPC(ctx, &handlers.RPCRequest{
		ID:      task.ID,
		Handler: task.Handler,
		Payload: task.Payload,
		Context: task.Metadata,
	})
	if err != nil {
		return &handlers.TaskResult{
			TaskID: task.ID,
			Status: handlers.TaskStatusFailed,
			Error:  err.Error(),
		}, nil
	}
	if resp == nil || !resp.Success {
		errMsg := ""
		if resp != nil {
			errMsg = resp.Error
		}
		return &handlers.TaskResult{
			TaskID: task.ID,
			Status: handlers.TaskStatusFailed,
			Error:  errMsg,
		}, nil
	}
	return &handlers.TaskResult{
		TaskID:  task.ID,
		Status:  handlers.TaskStatusCompleted,
		Payload: resp.Payload,
	}, nil
}

// unmarshalPayload decodes payload into msg, auto-detecting between
// proto binary (mesh RPC path) and protojson (mesh task path — the
// scanner marshals via protojson because Task.Payload is json.RawMessage
// and the outer Task struct is transported as JSON).
//
// Detection: the first non-whitespace byte is inspected. JSON payloads
// always lead with one of {, [, ", -, digit, t, f, n (object / array /
// string / number / true / false / null). Proto binary starts with a
// varint tag whose low 3 bits encode the wire type — the lowest possible
// tag byte is 0x08 (field 1, wire type 0), the highest legal tag byte is
// well below 0x22 for early field numbers. The overlap window with JSON
// leading bytes (0x22 = '"', 0x2d = '-', 0x30-0x39 = digits, 0x5b = '[',
// 0x66/0x6e/0x74 = f/n/t, 0x7b = '{') is small but non-empty, so we
// prefer the proto binary parse when the leading byte is a valid varint
// tag AND the JSON parse would fail. That is:
//
//   - Leading `{` or `[` → JSON object/array → protojson
//   - Otherwise → try proto first (compact & strict), fall back to
//     protojson only on proto failure (handles wrapper/well-known
//     types where protojson emits a bare scalar).
//
// Result: any real proto binary parses cleanly on the first attempt;
// domain messages (always JSON objects on the task wire) hit the first
// branch; well-known-type wrappers exercised in tests / edge domains
// fall through to the retry. Cost is one failed proto.Unmarshal in
// the rare wrapper-scalar case — cheap because proto binary parsing
// fails fast on non-varint leading bytes.
func unmarshalPayload(payload []byte, msg proto.Message) error {
	first := firstNonWhitespace(payload)
	if first == '{' || first == '[' {
		return protojson.Unmarshal(payload, msg)
	}
	if err := proto.Unmarshal(payload, msg); err == nil {
		return nil
	}
	return protojson.Unmarshal(payload, msg)
}

// firstNonWhitespace returns the first byte in b that is not ASCII
// whitespace, or 0 if b is empty / all whitespace. Used by
// unmarshalPayload's encoding heuristic.
func firstNonWhitespace(b []byte) byte {
	for _, c := range b {
		switch c {
		case ' ', '\t', '\n', '\r':
			continue
		}
		return c
	}
	return 0
}
