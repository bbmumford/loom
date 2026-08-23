/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"testing"

	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/pkg/rpc"
)

// COVERAGE of the local-dispatch short circuit: HasHandler (:32),
// DispatchBytes (:59), installLocalDispatcher (:86) — all at 0.0%.
//
// CENSUS: each has 1 non-test caller; installLocalDispatcher <- runtime.go:570,
// the single edge that makes this whole file reachable.
//
// 🔑 THIS IS THE SHORT CIRCUIT THAT SKIPS THE MESH. Once installed, every
// rpc.Call consults the local registry FIRST and never touches the wire when
// this process hosts the handler. Two consequences the tests below pin:
//   - HasHandler is the ONLY gate in front of local execution. A wrong TRUE
//     turns a mesh call into a spurious local failure; a wrong FALSE sends a
//     call over the wire to reach a handler that is already in this process.
//   - DispatchBytes must keep the security semantics its doc claims: "local
//     calls thus have the same security semantics as remote ones" — which is
//     true ONLY because it goes through registry.Dispatch rather than calling
//     the handler directly.

// rpcOnlyHandler is registered as an RPCHandler.
type rpcOnlyHandler struct {
	name   string
	role   string
	scope  handlers.TenantScope
	ran    bool
	result []byte
}

func (h *rpcOnlyHandler) Name() string { return h.name }
func (h *rpcOnlyHandler) Role() string {
	if h.role != "" {
		return h.role
	}
	return "system"
}
func (h *rpcOnlyHandler) RequiresAuth() bool                { return false }
func (h *rpcOnlyHandler) AllowedAuthTypes() []string        { return nil }
func (h *rpcOnlyHandler) Scopes() []string                  { return nil }
func (h *rpcOnlyHandler) TenantScope() handlers.TenantScope { return h.scope }
func (h *rpcOnlyHandler) AllowedTenants() []string          { return nil }

func (h *rpcOnlyHandler) ExecuteRPC(ctx context.Context, req *handlers.RPCRequest) (*handlers.RPCResponse, error) {
	h.ran = true
	return &handlers.RPCResponse{ID: req.ID, Success: true, Payload: h.result}, nil
}

var _ handlers.RPCHandler = (*rpcOnlyHandler)(nil)

// taskOnlyHandler is registered under a name but implements ONLY TaskHandler —
// the exact shape the HasHandler filter exists to reject.
type taskOnlyHandler struct{ name string }

func (h *taskOnlyHandler) Name() string                      { return h.name }
func (h *taskOnlyHandler) Role() string                      { return "system" }
func (h *taskOnlyHandler) RequiresAuth() bool                { return false }
func (h *taskOnlyHandler) AllowedAuthTypes() []string        { return nil }
func (h *taskOnlyHandler) Scopes() []string                  { return nil }
func (h *taskOnlyHandler) TenantScope() handlers.TenantScope { return handlers.TenantScopeNone }
func (h *taskOnlyHandler) AllowedTenants() []string          { return nil }

func (h *taskOnlyHandler) ExecuteTask(ctx context.Context, t *handlers.Task) (*handlers.TaskResult, error) {
	return &handlers.TaskResult{TaskID: t.ID}, nil
}

var _ handlers.TaskHandler = (*taskOnlyHandler)(nil)

// ── HasHandler: the only gate in front of local execution ───────────────────

// 🔴 THE FILE'S STATED REASON TO EXIST (:40-51). HandlerRegistry keeps RPC, Task
// and Stream handlers in ONE map. If HasHandler said true for a Task-shaped
// entry, TryLocalDispatch would return (handled=true, err=ErrUnsupportedMode)
// and the caller would surface a spurious failure for a call that should have
// fallen through to the mesh.
func TestHasHandlerRejectsATaskOnlyHandlerSoTheCallFallsThroughToTheMesh(t *testing.T) {
	reg := handlers.NewHandlerRegistry()
	if err := reg.RegisterTask(&taskOnlyHandler{name: "orbtr.io.x.TaskOnly"}); err != nil {
		t.Fatalf("RegisterTask: %v", err)
	}
	a := &localDispatcherAdapter{registry: reg}

	if a.HasHandler("orbtr.io.x.TaskOnly") {
		t.Fatal("HasHandler returned TRUE for a Task-only handler — TryLocalDispatch " +
			"will claim the call as handled, the registry's Dispatch will return " +
			"ErrUnsupportedMode, and the caller sees a spurious failure instead of " +
			"falling through to mesh dispatch")
	}
}

// …and the admission twin: it must return TRUE for a real RPC handler, or the
// test above passes against a HasHandler that is simply broken shut and every
// local call needlessly crosses the wire.
func TestHasHandlerAcceptsARealRPCHandler(t *testing.T) {
	reg := handlers.NewHandlerRegistry()
	if err := reg.RegisterRPC(&rpcOnlyHandler{name: "orbtr.io.x.Real", scope: handlers.TenantScopeNone}); err != nil {
		t.Fatalf("RegisterRPC: %v", err)
	}
	a := &localDispatcherAdapter{registry: reg}

	if !a.HasHandler("orbtr.io.x.Real") {
		t.Fatal("HasHandler returned FALSE for a registered RPC handler — the local " +
			"short circuit never fires, so every call this process could serve " +
			"itself goes out over the mesh instead")
	}
}

// An unregistered name is not handled locally.
func TestHasHandlerReturnsFalseForAnUnregisteredName(t *testing.T) {
	a := &localDispatcherAdapter{registry: handlers.NewHandlerRegistry()}
	if a.HasHandler("orbtr.io.x.NeverRegistered") {
		t.Fatal("HasHandler claimed an unregistered name")
	}
}

// 🔴 BOTH NIL GUARDS MUST FAIL CLOSED. HasHandler is the only gate in front of
// local execution; if the nil branch ever returned true, every rpc.Call in the
// process would be claimed locally and dispatched against a nil registry.
func TestTheNilGuardsFailClosed(t *testing.T) {
	var typedNil *localDispatcherAdapter
	if typedNil.HasHandler("anything") {
		t.Fatal("a nil adapter claimed a handler — every call would be claimed " +
			"locally and dispatched against nothing")
	}

	noRegistry := &localDispatcherAdapter{} // registry deliberately nil
	if noRegistry.HasHandler("anything") {
		t.Fatal("an adapter with no registry claimed a handler")
	}

	// And DispatchBytes must ERROR rather than panic or return a nil payload
	// that a caller would read as an empty success.
	for name, a := range map[string]*localDispatcherAdapter{
		"typed nil":   typedNil,
		"no registry": noRegistry,
	} {
		out, err := a.DispatchBytes(context.Background(), "x", nil)
		if err == nil {
			t.Errorf("%s: DispatchBytes returned no error — a caller reads (nil, nil) "+
				"as an empty SUCCESS", name)
		}
		if out != nil {
			t.Errorf("%s: DispatchBytes returned a payload alongside the failure", name)
		}
	}
}

// ── DispatchBytes: the security-semantics claim ─────────────────────────────

// 🔴 THE DOC'S CLAIM IS LOAD-BEARING: "local calls thus have the same security
// semantics as remote ones". That holds ONLY because DispatchBytes goes through
// registry.Dispatch, which runs validateTenantScope. Type-asserting to
// RPCHandler and calling ExecuteRPC directly would be faster, identical on the
// happy path, and would silently drop tenant enforcement for every local call.
//
// This is the same asymmetry I found in compose_bridge's task branch,
// which does exactly that — so it is not hypothetical in this codebase.
func TestDispatchBytesEnforcesTenantScopeJustLikeARemoteCall(t *testing.T) {
	reg := handlers.NewHandlerRegistry()
	h := &rpcOnlyHandler{name: "orbtr.io.x.TenantScoped", scope: handlers.TenantScopeTenant}
	if err := reg.RegisterRPC(h); err != nil {
		t.Fatalf("RegisterRPC: %v", err)
	}
	a := &localDispatcherAdapter{registry: reg}

	// No tenant in context ⇒ a tenant-scoped handler must be refused.
	_, err := a.DispatchBytes(context.Background(), "orbtr.io.x.TenantScoped", []byte(`{}`))
	if err == nil {
		t.Fatal("a TENANT-SCOPED handler executed through the local short circuit " +
			"with no tenant in context — DispatchBytes is bypassing " +
			"registry.Dispatch's validateTenantScope, so local calls do NOT have " +
			"the same security semantics as remote ones, contrary to its own doc")
	}
	if h.ran {
		t.Fatal("the handler BODY ran despite the scope check failing — the check " +
			"is happening after execution, which is not a check")
	}
}

// An unscoped handler still executes and its payload comes back — otherwise the
// test above passes against a DispatchBytes that refuses everything.
func TestDispatchBytesReturnsThePayloadOfAnUnscopedHandler(t *testing.T) {
	reg := handlers.NewHandlerRegistry()
	h := &rpcOnlyHandler{name: "orbtr.io.x.Open", scope: handlers.TenantScopeNone, result: []byte("pong")}
	if err := reg.RegisterRPC(h); err != nil {
		t.Fatalf("RegisterRPC: %v", err)
	}
	a := &localDispatcherAdapter{registry: reg}

	out, err := a.DispatchBytes(context.Background(), "orbtr.io.x.Open", []byte(`{}`))
	if err != nil {
		t.Fatalf("DispatchBytes on an unscoped handler: %v — the local short "+
			"circuit refuses everything, so this suite's refusal tests prove nothing", err)
	}
	if string(out) != "pong" {
		t.Fatalf("payload = %q, want %q — the handler's result is not reaching the caller", out, "pong")
	}
	if !h.ran {
		t.Fatal("the handler never ran")
	}
}

// A handler that returns Success=false must surface as an ERROR, mirroring the
// wire format — otherwise a local caller reads a failed call as a success with
// an empty payload.
func TestAnUnsuccessfulHandlerBecomesAnErrorNotAnEmptySuccess(t *testing.T) {
	reg := handlers.NewHandlerRegistry()
	if err := reg.RegisterRPC(&failingHandler{name: "orbtr.io.x.Fails"}); err != nil {
		t.Fatalf("RegisterRPC: %v", err)
	}
	a := &localDispatcherAdapter{registry: reg}

	out, err := a.DispatchBytes(context.Background(), "orbtr.io.x.Fails", nil)
	if err == nil {
		t.Fatal("a handler returning Success=false came back as a SUCCESS through " +
			"the local path — remote callers get an error for the same response, so " +
			"the same call succeeds or fails depending on whether it crossed the wire")
	}
	if out != nil {
		t.Fatal("a failed dispatch returned a payload")
	}
}

type failingHandler struct{ name string }

func (h *failingHandler) Name() string                      { return h.name }
func (h *failingHandler) Role() string                      { return "system" }
func (h *failingHandler) RequiresAuth() bool                { return false }
func (h *failingHandler) AllowedAuthTypes() []string        { return nil }
func (h *failingHandler) Scopes() []string                  { return nil }
func (h *failingHandler) TenantScope() handlers.TenantScope { return handlers.TenantScopeNone }
func (h *failingHandler) AllowedTenants() []string          { return nil }
func (h *failingHandler) ExecuteRPC(ctx context.Context, req *handlers.RPCRequest) (*handlers.RPCResponse, error) {
	return &handlers.RPCResponse{ID: req.ID, Success: false, Error: "handler said no"}, nil
}

var _ handlers.RPCHandler = (*failingHandler)(nil)

// ── installLocalDispatcher ──────────────────────────────────────────────────

// 🔴 runtime.go:570 is the SINGLE edge that makes this file reachable. If the
// struct literal ever loses its `registry: reg` field, every guard above starts
// failing closed and the local short circuit silently stops working — every call
// would go out over the mesh, which is slower but not obviously broken.
func TestInstallLocalDispatcherBindsTheRegistryItWasHanded(t *testing.T) {
	reg := handlers.NewHandlerRegistry()
	if err := reg.RegisterRPC(&rpcOnlyHandler{name: "orbtr.io.x.Installed", scope: handlers.TenantScopeNone}); err != nil {
		t.Fatalf("RegisterRPC: %v", err)
	}

	installLocalDispatcher(reg)

	d := rpc.LocalDispatcherInstance()
	if d == nil {
		t.Fatal("no dispatcher was installed — rpc.Call never consults the local " +
			"registry and the short circuit is dead")
	}
	if !d.HasHandler("orbtr.io.x.Installed") {
		t.Fatal("the installed dispatcher does not see the registry it was handed — " +
			"the binding was lost, so every local call goes out over the mesh")
	}
	if d.HasHandler("orbtr.io.x.NotInThisRegistry") {
		t.Fatal("the installed dispatcher claims a handler no registry holds")
	}
}

// Guard against the suite leaving a process-global dispatcher behind for other
// tests: installing a registry with nothing in it must claim nothing.
func TestInstallingAnEmptyRegistryClaimsNothing(t *testing.T) {
	installLocalDispatcher(handlers.NewHandlerRegistry())
	if d := rpc.LocalDispatcherInstance(); d != nil && d.HasHandler("orbtr.io.x.Installed") {
		t.Fatal("a stale handler survived re-installation — the global dispatcher " +
			"is not last-write-wins, contrary to installLocalDispatcher's doc")
	}
}
