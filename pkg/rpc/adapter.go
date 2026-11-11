package rpc

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/bbmumford/loom/node/handlers"
	sharedDispatch "github.com/bbmumford/loom/pkg/dispatch"
	"github.com/bbmumford/loom/pkg/rpc/effect"
	rpcscope "github.com/bbmumford/loom/pkg/rpc/scope"
)

// ExportToHandlerRegistry registers every handler in this rpc.Registry that
// carries a Func into a node/handlers.HandlerRegistry, so the RPCServer and
// task-executor dispatch paths reach handlers a domain registers through its
// register.go.
//
// A handler with no Func is left out: it is a RegisterProxy entry, or a
// Register call that supplied none, so this node does not execute it and the
// node holding the Func must. Every reader of hr takes GetMeta finding a name
// as "this node runs it", and localDispatcherAdapter.HasHandler
// (node/rpc_local_dispatch.go) is one of them: it answers true for any
// RPC-shaped entry, so an exported Func-less handler makes Call and CallRaw
// return tryLocalDispatch's refusal instead of falling through to the mesh,
// and makes RPCServer.handleRequest answer in place of forwarding.
//
// Each exported handler is wrapped in a shim that implements
// handlers.RPCHandler AND handlers.TaskHandler — the second interface is
// required so mesh task executors (execution.Executor) can resolve the shim
// after the mesh task path type-asserts GetMeta result to TaskHandler. A
// handler reached only through RPC never takes the task branch; the branch
// costs one RPCRequest per task because ExecuteTask passes through to
// ExecuteRPC.
func (r *Registry) ExportToHandlerRegistry(hr *handlers.HandlerRegistry) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, h := range r.handlers {
		if h.Func == nil {
			continue
		}
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
// rpc.Registry — e.g. an operation a background scanner publishes as a mesh
// task.
type registryShim struct {
	handler *Handler
}

func newRegistryShim(h *Handler) *registryShim {
	return &registryShim{handler: h}
}

// HandlerMeta interface
func (s *registryShim) Name() string                { return s.handler.FQN() }
func (s *registryShim) Role() string                { return s.handler.Role() }
func (s *registryShim) RequiresAuth() bool          { return !s.handler.IsPublic() }
func (s *registryShim) AllowedAuthTypes() []string  { return nil }
func (s *registryShim) Scopes() []string            { return nil }
func (s *registryShim) Scope() handlers.Scope       { return s.handler.Scope }
func (s *registryShim) AllowedPartitions() []string { return nil }

// Requirement forwards the wrapped handler's declared Requirement, so a handler
// registered with one is judged by the same authorization arm on the mesh
// registry as on the HTTP registry. nil when the handler declares none.
func (s *registryShim) Requirement() *rpcscope.Requirement { return s.handler.Requirement }

var _ handlers.RequirementDeclarer = (*registryShim)(nil)

// Effect forwards the wrapped handler's declared effect class, so RPCServer
// judges a local run after an unknown forward outcome by the class the handler
// was registered with.
func (s *registryShim) Effect() effect.Class { return s.handler.Effect }

var _ handlers.EffectDeclarer = (*registryShim)(nil)

// HandleStream serves the wrapped handler's Stream, the one DispatchStream
// reaches after judging its Requirement. An operation with none refuses.
func (s *registryShim) HandleStream(ctx context.Context, stream handlers.MessageStream) error {
	serve := s.handler.Stream
	if serve == nil {
		return handlers.ErrUnsupportedMode
	}
	return serve(ctx, stream)
}

var _ handlers.StreamHandler = (*registryShim)(nil)

// RPCHandler interface — decodes req.Payload into the handler's request
// prototype, calls the handler, and marshals the response. Payload
// encoding is auto-detected: bytes starting with '{' are protojson (the
// mesh task path stuffs Task.Payload as json.RawMessage), any other
// leading byte is treated as protobuf binary (the standard mesh RPC path).
// This lets one shim serve both dispatch surfaces without asking the
// caller which encoding they used.
//
// A handler with no Func is refused with ErrProxyDispatchLocal, the refusal
// Registry.Dispatch gives, before the payload is read. ExportToHandlerRegistry
// exports no such handler, so the refusal covers the window the shared pointer
// opens: the shim holds the registry's stored *Handler, which Get, All and
// ByRole hand out, so a holder can clear Func on it after the export.
//
// Func and WebhookSignature are read once, before the payload, and the call
// below uses those two values. Both run caller code in between — the request
// type's own ProtoReflect under unmarshalPayload, and the handler's
// SecretProvider under verifyWebhookSignature — so re-reading either field
// would let a Func cleared on the stored handler reach the call as nil, and a
// WebhookSignature cleared there drop the check between the test and its use.
//
// A handler that declares WebhookSignature is refused after decoding and
// before Func, through verifyWebhookSignature as in Registry.Dispatch, on a
// missing secret or a signature that does not verify.
//
// A Func error becomes a failed response with the error's text, and with the
// error itself as Denied when it refuses the caller's authority, or as Conflict
// when it is a conflict with the state the handler holds, so the node serving
// the call over the mesh (RPCServer.executeLocal) answers it as that refusal
// and not as a handler failure.
func (s *registryShim) ExecuteRPC(ctx context.Context, req *handlers.RPCRequest) (*handlers.RPCResponse, error) {
	fn, sig := s.handler.Func, s.handler.WebhookSignature
	if fn == nil {
		return nil, fmt.Errorf("%w: %s", ErrProxyDispatchLocal, s.handler.FQN())
	}
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

	// The signature material is the decoded request's payload and signature
	// fields, which a proxying gateway forwards inside the marshalled request,
	// so the check Registry.Dispatch runs applies here unchanged and refuses
	// before Func on a missing secret or a bad signature.
	if sig != nil {
		if err := verifyWebhookSignature(sig, protoReq); err != nil {
			return nil, fmt.Errorf("%s: %w", s.handler.FQN(), err)
		}
	}

	protoResp, err := fn(ctx, protoReq)
	if err != nil {
		return &handlers.RPCResponse{
			Success:  false,
			Error:    err.Error(),
			Latency:  time.Since(start),
			Denied:   handlers.Denial(err),
			Conflict: sharedDispatch.Conflict(err),
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
// nil, so a task executor sees the same result from an exported handler as
// from a BaseHandler.
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
