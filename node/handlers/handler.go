/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package handlers

import (
	"context"
	"sync"
	"fmt"
	"time"

	"github.com/bbmumford/loom/pkg/trace"
	"github.com/bbmumford/loom/pkg/rpc/scope"
)

// ---------------------------------------------------------------------------
// Split handler interfaces (preferred — use these for new code)
// ---------------------------------------------------------------------------

// TenantScope defines the tenant isolation level for a handler.
//
// Deprecated: rpc.TenantScope is the canonical caller-facing scope symbol.
// Both rpc.TenantScope and this handlers.TenantScope now alias the SAME
// underlying declaration in rpc/scope — they are the same type identity,
// not merely "structurally identical string aliases". A value assigned in
// one package is directly usable in the other with no cast or conversion.
// New code MUST import "github.com/bbmumford/loom/pkg/rpc" and use
// rpc.TenantScope + rpc.Scope*; this handlers-side alias remains only so
// the ~434 legacy call sites continue to compile during the migration.
//
// Do NOT add new constants HERE — declare them in rpc/scope/tenantscope.go
// and forward them from both re-exports. Finding rev-069.
type TenantScope = scope.TenantScope

// TenantScope* forwards to the canonical constants in rpc/scope. Because
// each constant is a direct forward, drift between handlers.TenantScope*
// and rpc.Scope* is now IMPOSSIBLE at compile time — both re-exports pull
// from the same declaration.
//
// Deprecated: use rpc.Scope{None,Platform,Tenant,Org,User,Profile,Unknown}
// for new code. These symbols remain to avoid a big-bang rename in ~434
// legacy call sites, but will be deleted once migration completes.
const (
	TenantScopeNone     = scope.None     // No tenant restriction (default, backward-compat). Mirrors rpc.ScopeNone.
	TenantScopePlatform = scope.Platform // HSTLES internal only. Mirrors rpc.ScopePlatform.
	TenantScopeTenant   = scope.Tenant   // Requires valid tenant_id. Mirrors rpc.ScopeTenant.
	TenantScopeOrg      = scope.Org      // Requires tenant + org membership. Mirrors rpc.ScopeOrg.
	TenantScopeUser     = scope.User     // Requires tenant + own user data only. Mirrors rpc.ScopeUser.
	TenantScopeProfile  = scope.Profile  // Requires tenant + billing-profile membership. Mirrors rpc.ScopeProfile (per Quorum ADR-0012).
	// TenantScopeUnknown is the fail-closed sentinel, mirroring rpc.ScopeUnknown.
	// validateTenantScope's default arm rejects any handler bound to this scope.
	// Never set it manually — it exists so decay-to-empty-string cannot fail open.
	TenantScopeUnknown = scope.Unknown
)

// HandlerMeta contains metadata common to all handler types.
// Both RPCHandler and TaskHandler embed this interface.
type HandlerMeta interface {
	// Name returns the operation name (e.g., "auth.login", "identity.getUser")
	Name() string

	// Role returns the domain role (e.g., "auth", "identity", "notify")
	Role() string

	// RequiresAuth returns true if operation needs authentication
	RequiresAuth() bool

	// AllowedAuthTypes returns acceptable authentication methods.
	// Values should be the security/core.AuthType* constants — "session",
	// "mtls", "api-key", "bearer", "dev". Empty slice = any auth type
	// accepted (if RequiresAuth is true).
	AllowedAuthTypes() []string

	// Scopes returns required JWT scopes (e.g., ["auth.login"])
	Scopes() []string

	// TenantScope returns the required tenant isolation level.
	// Returns TenantScopeNone (empty) for unrestricted handlers.
	TenantScope() TenantScope

	// AllowedTenants returns an optional tenant whitelist.
	// Empty slice = all tenants allowed (subject to TenantScope).
	AllowedTenants() []string
}

// RPCHandler handles synchronous request/response operations.
type RPCHandler interface {
	HandlerMeta
	ExecuteRPC(ctx context.Context, req *RPCRequest) (*RPCResponse, error)
}

// TaskHandler handles asynchronous/fire-and-forget operations.
type TaskHandler interface {
	HandlerMeta
	ExecuteTask(ctx context.Context, task *Task) (*TaskResult, error)
}

// StreamHandler handles long-lived bidirectional, server-stream, or client-stream
// operations. Used for: RemoteNegotiate, JobExecute (bidi), PolicyWatch (server-stream),
// DeviceControl (bidi).
//
// StreamHandler does NOT use GenericWrapper — streams have no single request/response
// cycle. Middleware fires Before() on stream open and After() on stream close, but
// the stream itself is unmediated.
type StreamHandler interface {
	HandlerMeta
	HandleStream(ctx context.Context, stream MessageStream) error
}

// MessageStream abstracts the underlying transport for streaming handlers.
// Implementations may wrap WebSocket, VL1 session, gRPC stream, etc.
type MessageStream interface {
	// Send writes a payload to the remote end.
	Send(payload []byte) error

	// Recv blocks until a payload is received or the stream closes.
	Recv() ([]byte, error)

	// Context returns the stream's context, which is canceled on close.
	Context() context.Context

	// Close terminates the stream from this end.
	Close() error
}

// DualHandler supports both sync and async dispatch (rare).
type DualHandler interface {
	RPCHandler
	TaskHandler
}

// ---------------------------------------------------------------------------
// Request / Response types
// ---------------------------------------------------------------------------

// RPCRequest represents a direct RPC call
type RPCRequest struct {
	ID        string                 `json:"id"`         // Request ID (idempotency)
	Handler   string                 `json:"handler"`    // "auth.login", "identity.getUser"
	Payload   []byte                 `json:"payload"`    // Proto-encoded operation data
	Context   map[string]interface{} `json:"context"`    // Session, trace IDs
	Timeout   time.Duration          `json:"timeout"`    // Max execution time
	SessionID string                 `json:"sessionId"` // Optional session identifier
	TraceID   string                 `json:"traceId"`   // Optional trace identifier
}

// RPCResponse represents the result of an RPC call
type RPCResponse struct {
	ID       string                 `json:"id"`       // Matches request ID
	Success  bool                   `json:"success"`  // Operation result
	Payload  []byte                 `json:"payload"`  // Proto-encoded response data
	Error    string                 `json:"error"`    // Error message if failed
	Latency  time.Duration          `json:"latency"`  // Execution time
	Metadata map[string]interface{} `json:"metadata"` // Extra context
}

// Task represents an async task execution
type Task struct {
	ID          string                 `json:"id"`          // Task ID
	Handler     string                 `json:"handler"`     // "notify.sendEmail"
	Payload     []byte                 `json:"payload"`     // Proto-encoded operation data
	Priority    int                    `json:"priority"`    // 1-10
	Deadline    time.Time              `json:"deadline"`    // Must complete by
	Idempotency string                 `json:"idempotency"` // Dedup key
	Metadata    map[string]interface{} `json:"metadata"`    // Extra context
	CreatedAt   time.Time              `json:"createdAt"`  // When task was created
	SessionID   string                 `json:"sessionId"`  // Optional session identifier
	TraceID     string                 `json:"traceId"`    // Optional trace identifier
}

// TaskStatus represents task execution state
type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusCompleted TaskStatus = "completed"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusTimeout   TaskStatus = "timeout"
	TaskStatusCanceled  TaskStatus = "canceled"
)

// TaskResult represents the outcome of task execution
type TaskResult struct {
	TaskID      string                 `json:"taskId"`
	Status      TaskStatus             `json:"status"`       // Completed, Failed, Timeout
	Payload     []byte                 `json:"payload"`      // Proto-encoded result data
	Error       string                 `json:"error"`        // Error if failed
	Duration    time.Duration          `json:"duration"`     // Actual execution time
	CompletedAt time.Time              `json:"completedAt"` // When task completed
	NodeID      string                 `json:"nodeId"`      // Which node executed
	Metadata    map[string]interface{} `json:"metadata"`     // Extra context
}

// ---------------------------------------------------------------------------
// Execution path selection
// ---------------------------------------------------------------------------

// ExecutionPath indicates how to execute a handler
type ExecutionPath int

const (
	ExecutionPathAuto ExecutionPath = iota // Auto-select based on context
	ExecutionPathRPC                       // Force direct RPC
	ExecutionPathTask                      // Force async task
)

// ExecutionContext contains routing information for handler execution
type ExecutionContext struct {
	Role       string        // "auth", "identity", "notify"
	Operation  string        // "login", "verify2fa", "createUser", etc.
	Priority   int           // 1-10, higher = more urgent
	Timeout    time.Duration // Max execution time
	Idempotent bool          // Can safely retry
	SessionID  string        // Optional session for auth
	TraceID    string        // Optional trace for observability
}

// Sentinel errors for dispatch
var (
	ErrHandlerNotFound = fmt.Errorf("handler not found")
	ErrUnsupportedMode = fmt.Errorf("handler does not support requested dispatch mode")
)

// ---------------------------------------------------------------------------
// HandlerRegistry
// ---------------------------------------------------------------------------

// HandlerRegistry manages all registered handlers and middleware.
// Accepts RPCHandler, TaskHandler, DualHandler, and the legacy Handler interface.
//
// Concurrency: guarded by mu. Historically the maps were unguarded under a
// "register everything before serving" convention; Phase-1 runtime role
// activation registers and UNREGISTERS handlers while dispatch is live, so
// the registry is now properly synchronized (reads take RLock — negligible
// against the network cost of any dispatch).
type HandlerRegistry struct {
	mu               sync.RWMutex
	handlers         map[string]HandlerMeta
	enabledRoles     map[string]bool
	roleMiddleware   map[string][]Middleware // per-role middleware
	globalMiddleware []Middleware            // runs for all handlers
	platformTenants  []string                // e.g., ["hstles", "orbtr"]

	// regObserver, when non-nil, is invoked (outside the lock) with each
	// successfully registered handler name — the compose scope tracker
	// hooks this so role activation captures exactly the FQNs it
	// registered and teardown can remove precisely those.
	regObserver func(name string)
}

// NewHandlerRegistry creates a new handler registry
func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{
		handlers:       make(map[string]HandlerMeta),
		enabledRoles:   make(map[string]bool),
		roleMiddleware: make(map[string][]Middleware),
	}
}

// UseForRole registers middleware that runs for all handlers of a given role.
func (r *HandlerRegistry) UseForRole(role string, mw Middleware) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.roleMiddleware[role] = append(r.roleMiddleware[role], mw)
}

// Use registers global middleware that runs for all handlers.
func (r *HandlerRegistry) Use(mw Middleware) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.globalMiddleware = append(r.globalMiddleware, mw)
}

// SetPlatformTenants configures which tenant IDs are considered platform tenants.
// Handlers with TenantScopePlatform will only accept requests from these tenants.
// Each endpoint should call this at init, e.g. registry.SetPlatformTenants([]string{"hstles"}).
func (r *HandlerRegistry) SetPlatformTenants(tenants []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.platformTenants = append([]string(nil), tenants...)
}

// isPlatformTenant checks if the given tenant ID is in the configured platform tenants list.
// Returns true if tenantID matches any configured platform tenant, or if tenantID is "hstles"
// (hardcoded fallback for backward compatibility when no platform tenants are configured).
func (r *HandlerRegistry) isPlatformTenant(tenantID string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.platformTenants) > 0 {
		for _, pt := range r.platformTenants {
			if pt == tenantID {
				return true
			}
		}
		return false
	}
	// Fallback: if no platform tenants configured, default to "hstles" for backward compat
	return tenantID == "hstles"
}

// GetMiddleware returns global middleware followed by role-specific middleware.
func (r *HandlerRegistry) GetMiddleware(role string) []Middleware {
	r.mu.RLock()
	defer r.mu.RUnlock()
	total := len(r.globalMiddleware) + len(r.roleMiddleware[role])
	if total == 0 {
		return nil
	}
	result := make([]Middleware, 0, total)
	result = append(result, r.globalMiddleware...)
	result = append(result, r.roleMiddleware[role]...)
	return result
}

// SetEnabledRoles configures which roles this node can execute
func (r *HandlerRegistry) SetEnabledRoles(roles []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.enabledRoles = make(map[string]bool)
	for _, role := range roles {
		r.enabledRoles[role] = true
	}
}

// IsRoleEnabled checks if a role is enabled on this node
func (r *HandlerRegistry) IsRoleEnabled(role string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.enabledRoles[role]
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

// RegisterRPC registers a synchronous RPC handler.
func (r *HandlerRegistry) RegisterRPC(h RPCHandler) error {
	return r.registerMeta(h)
}

// RegisterTask registers an asynchronous task handler.
func (r *HandlerRegistry) RegisterTask(h TaskHandler) error {
	return r.registerMeta(h)
}

// RegisterStream registers a streaming handler.
func (r *HandlerRegistry) RegisterStream(h StreamHandler) error {
	return r.registerMeta(h)
}

// RegisterHandler registers any handler that implements HandlerMeta.
// The handler must also implement RPCHandler, TaskHandler, or both.
func (r *HandlerRegistry) RegisterHandler(h HandlerMeta) error {
	return r.registerMeta(h)
}

func (r *HandlerRegistry) registerMeta(h HandlerMeta) error {
	if h == nil {
		return fmt.Errorf("handler cannot be nil")
	}

	name := h.Name()
	if name == "" {
		return fmt.Errorf("handler name cannot be empty")
	}

	r.mu.Lock()
	if _, exists := r.handlers[name]; exists {
		r.mu.Unlock()
		return fmt.Errorf("handler %s already registered", name)
	}
	r.handlers[name] = h
	observer := r.regObserver
	r.mu.Unlock()
	if observer != nil {
		observer(name)
	}
	return nil
}

// Unregister removes a handler by name, returning whether it existed. The
// Phase-1 role-teardown path: SetEnabledRoles only gates dispatch — this
// actually removes the registration so a re-activation starts clean.
func (r *HandlerRegistry) Unregister(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.handlers[name]; !ok {
		return false
	}
	delete(r.handlers, name)
	return true
}

// SetRegistrationObserver installs (or clears, with nil) the registration
// hook. The observer runs outside the registry lock.
func (r *HandlerRegistry) SetRegistrationObserver(fn func(name string)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.regObserver = fn
}

// ---------------------------------------------------------------------------
// Lookup
// ---------------------------------------------------------------------------

// GetMeta retrieves a handler by name as HandlerMeta.
func (r *HandlerRegistry) GetMeta(name string) (HandlerMeta, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[name]
	return h, ok
}

// GetByRoleMeta retrieves all handlers for a specific role as HandlerMeta.
func (r *HandlerRegistry) GetByRoleMeta(role string) []HandlerMeta {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var result []HandlerMeta
	for _, h := range r.handlers {
		if h.Role() == role {
			result = append(result, h)
		}
	}
	return result
}

// AllHandlers returns all registered handlers as HandlerMeta.
func (r *HandlerRegistry) AllHandlers() []HandlerMeta {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]HandlerMeta, 0, len(r.handlers))
	for _, h := range r.handlers {
		result = append(result, h)
	}
	return result
}

// Handlers is an alias for AllHandlers (used by node runtime).
func (r *HandlerRegistry) Handlers() []HandlerMeta {
	return r.AllHandlers()
}

// List returns all registered handler names
func (r *HandlerRegistry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.handlers))
	for name := range r.handlers {
		names = append(names, name)
	}
	return names
}

// Count returns the number of registered handlers
func (r *HandlerRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.handlers)
}

// ---------------------------------------------------------------------------
// Dispatch — executes a handler with middleware
// ---------------------------------------------------------------------------

// getUserIDFromContext extracts the user ID from context, checking the RPC context map.
// Returns "" if no user ID is found.
func getUserIDFromContext(ctx context.Context) string {
	rpcCtx := extractContextMap(ctx)
	if rpcCtx == nil {
		return ""
	}
	if uid, ok := rpcCtx["userID"].(string); ok {
		return uid
	}
	return ""
}

// validateTenantScope checks that the request context satisfies the handler's tenant scope.
// Returns nil if validation passes. Used internally by Dispatch methods.
func (r *HandlerRegistry) validateTenantScope(ctx context.Context, h HandlerMeta) error {
	scope := h.TenantScope()
	if scope == "" {
		return nil
	}

	tenantID := GetTenantIDFromContext(extractContextMap(ctx))

	switch scope {
	case TenantScopePlatform:
		if !r.isPlatformTenant(tenantID) {
			return fmt.Errorf("handler %s requires platform access, got tenant: %q", h.Name(), tenantID)
		}
	case TenantScopeTenant:
		if tenantID == "" {
			return fmt.Errorf("handler %s requires tenant context", h.Name())
		}
	case TenantScopeOrg:
		if tenantID == "" {
			return fmt.Errorf("handler %s requires org context", h.Name())
		}
	case TenantScopeUser:
		if tenantID == "" {
			return fmt.Errorf("handler %s requires tenant context", h.Name())
		}
		userID := getUserIDFromContext(ctx)
		if userID == "" {
			return fmt.Errorf("handler %s requires authenticated user context", h.Name())
		}
	case TenantScopeProfile:
		// Mirrors rpc.EnforceScope's ScopeProfile arm: profile membership is a
		// scope-string check at the middleware tier (RequireScopesWithParam —
		// Quorum ADR-0012). At the dispatch tier we only enforce that the
		// tenant prerequisite is present; the middleware decides whether the
		// caller belongs to the named billing profile.
		if tenantID == "" {
			return fmt.Errorf("handler %s requires tenant context (profile scope)", h.Name())
		}
	case TenantScopeUnknown:
		// Fail-closed sentinel — a handler bound to TenantScopeUnknown is
		// structurally invalid (e.g. an unrecognised proto enum decayed here).
		// Refuse every dispatch with a clear diagnostic so the misconfiguration
		// surfaces at the first call site rather than silently allowing traffic.
		return fmt.Errorf("handler %s has fail-closed TenantScopeUnknown — add an explicit scope declaration", h.Name())
	default:
		// Any scope value that is not one of the known constants is treated as
		// fail-closed. This prevents a typo (e.g. "tennant") or a partial migration
		// from a future scope tier from silently accepting traffic that would
		// have required tenant/org/user context under the intended scope.
		return fmt.Errorf("handler %s declares unknown tenant scope %q — reject to preserve isolation guarantee", h.Name(), scope)
	}

	allowed := h.AllowedTenants()
	if len(allowed) > 0 {
		found := false
		for _, a := range allowed {
			if a == tenantID {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("tenant %q not authorized for handler %s", tenantID, h.Name())
		}
	}

	// Transport-level verification: if the session arrived on a tenant-specific
	// transport (dedicated or shared preamble), the request's tenant_id must match.
	// "default" transport (single-tenant fallback) allows any request tenant.
	transportTenant := TransportTenantFromContext(ctx)
	if transportTenant != "" && transportTenant != "default" && tenantID != "" {
		if transportTenant != tenantID {
			return fmt.Errorf("transport/request tenant mismatch: transport=%q request=%q for handler %s",
				transportTenant, tenantID, h.Name())
		}
	}

	return nil
}

// extractContextMap extracts the RPCRequest context map from context.Context values.
// This checks the "rpc_context" key set by middleware or transport layers.
func extractContextMap(ctx context.Context) map[string]interface{} {
	if m, ok := ctx.Value("rpc_context").(map[string]interface{}); ok {
		return m
	}
	return nil
}

// injectRPCContext merges the RPCRequest.Context map into the Go context under
// the "rpc_context" key so that extractContextMap, getUserIDFromContext, and
// GetTenantIDFromContext can read values set by the transport layer or HTTP bridge.
// If reqCtx is nil or empty, returns ctx unchanged.
func injectRPCContext(ctx context.Context, reqCtx map[string]interface{}) context.Context {
	if len(reqCtx) == 0 {
		return ctx
	}
	// Merge with any existing rpc_context already set (e.g., by middleware)
	existing := extractContextMap(ctx)
	merged := make(map[string]interface{}, len(reqCtx)+len(existing))
	for k, v := range existing {
		merged[k] = v
	}
	for k, v := range reqCtx {
		merged[k] = v
	}
	return context.WithValue(ctx, "rpc_context", merged)
}

// Dispatch looks up a handler by name and executes it as an RPC, running
// the middleware chain (global + role-scoped) around the handler call.
// Returns ErrHandlerNotFound if no handler is registered for the name.
// Returns ErrUnsupportedMode if the handler doesn't implement RPCHandler.
func (r *HandlerRegistry) Dispatch(ctx context.Context, req *RPCRequest) (*RPCResponse, error) {
	h, ok := r.GetMeta(req.Handler)
	if !ok {
		return nil, ErrHandlerNotFound
	}

	// Inject RPCRequest.Context into Go context so validateTenantScope and
	// getUserIDFromContext can read tenant_id, userID, etc. from the RPC
	// context map. Merges with any existing rpc_context already set.
	ctx = injectRPCContext(ctx, req.Context)

	// Propagate e2e trace ID — the LocalCaller (in-process dispatch) and
	// any other caller that stamps req.TraceID expect the handler chain
	// to expose it via trace.FromContext. The HWP-mesh receiving path
	// does the same lift in mesh/node/rpc.go.
	if req.TraceID != "" {
		ctx = trace.WithID(ctx, req.TraceID)
	}

	// 🔒 TENANT SCOPE ENFORCEMENT
	if err := r.validateTenantScope(ctx, h); err != nil {
		return &RPCResponse{
			Success: false,
			Error:   fmt.Sprintf("tenant restriction: %v", err),
		}, nil
	}

	// Run middleware Before hooks
	middleware := r.GetMiddleware(h.Role())
	var err error
	for idx, mw := range middleware {
		ctx, err = mw.Before(ctx, h.Name(), req)
		if err != nil {
			// MESH-D06: a later Before rejected the request. Run After (reverse
			// order) for the middlewares whose Before already completed so any
			// resource they acquired in Before (span, semaphore, tx, counter) is
			// released — the previous code returned immediately and leaked them.
			rejection := &RPCResponse{Success: false, Error: err.Error()}
			rr, re := rejection, err
			for i := idx - 1; i >= 0; i-- {
				rr, re = middleware[i].After(ctx, h.Name(), rr, re)
			}
			if rr != nil {
				rejection = rr
			}
			return rejection, nil
		}
	}

	// Dispatch based on interface
	var resp *RPCResponse
	switch rpc := h.(type) {
	case RPCHandler:
		resp, err = rpc.ExecuteRPC(ctx, req)
	default:
		return nil, ErrUnsupportedMode
	}

	// Run middleware After hooks (reverse order)
	for i := len(middleware) - 1; i >= 0; i-- {
		resp, err = middleware[i].After(ctx, h.Name(), resp, err)
	}

	return resp, err
}

// DispatchTask looks up a handler by name and executes it as a Task.
// Returns ErrHandlerNotFound if no handler is registered for the name.
// Returns ErrUnsupportedMode if the handler doesn't implement TaskHandler.
func (r *HandlerRegistry) DispatchTask(ctx context.Context, task *Task) (*TaskResult, error) {
	h, ok := r.GetMeta(task.Handler)
	if !ok {
		return nil, ErrHandlerNotFound
	}

	th, ok := h.(TaskHandler)
	if !ok {
		return nil, ErrUnsupportedMode
	}

	// Inject task metadata into Go context for tenant scope validation
	ctx = injectRPCContext(ctx, task.Metadata)

	// 🔒 TENANT SCOPE ENFORCEMENT
	if err := r.validateTenantScope(ctx, h); err != nil {
		return &TaskResult{
			TaskID: task.ID,
			Status: TaskStatusFailed,
			Error:  fmt.Sprintf("tenant restriction: %v", err),
		}, nil
	}

	// Run middleware Before hooks (convert task to RPCRequest for middleware)
	middleware := r.GetMiddleware(h.Role())
	var err error
	rpcReq := &RPCRequest{
		ID:      task.ID,
		Handler: task.Handler,
		Payload: task.Payload,
		Context: task.Metadata,
	}
	for _, mw := range middleware {
		ctx, err = mw.Before(ctx, h.Name(), rpcReq)
		if err != nil {
			return &TaskResult{
				TaskID: task.ID,
				Status: TaskStatusFailed,
				Error:  err.Error(),
			}, nil
		}
	}

	return th.ExecuteTask(ctx, task)
}

// DispatchStream looks up a handler by name and opens a stream, running
// the middleware chain around the stream lifecycle.
// Before() fires on stream open, After() fires on stream close.
// Returns ErrHandlerNotFound if no handler is registered for the name.
// Returns ErrUnsupportedMode if the handler doesn't implement StreamHandler.
func (r *HandlerRegistry) DispatchStream(ctx context.Context, handlerName string, stream MessageStream) error {
	h, ok := r.GetMeta(handlerName)
	if !ok {
		return ErrHandlerNotFound
	}

	sh, ok := h.(StreamHandler)
	if !ok {
		return ErrUnsupportedMode
	}

	// 🔒 TENANT SCOPE ENFORCEMENT
	if err := r.validateTenantScope(ctx, h); err != nil {
		return fmt.Errorf("tenant restriction: %w", err)
	}

	// Run middleware Before hooks
	middleware := r.GetMiddleware(h.Role())
	req := &RPCRequest{Handler: handlerName}
	var err error
	for _, mw := range middleware {
		ctx, err = mw.Before(ctx, h.Name(), req)
		if err != nil {
			return fmt.Errorf("middleware before: %w", err)
		}
	}

	// Execute stream handler
	streamErr := sh.HandleStream(ctx, stream)

	// Run middleware After hooks (reverse order)
	for i := len(middleware) - 1; i >= 0; i-- {
		_, _ = middleware[i].After(ctx, h.Name(), &RPCResponse{
			Success: streamErr == nil,
			Error:   errString(streamErr),
		}, streamErr)
	}

	return streamErr
}

// errString returns the error string or empty if nil.
func errString(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}

// ---------------------------------------------------------------------------
// Execution path selection
// ---------------------------------------------------------------------------

// SelectExecutionPath determines whether to use RPC or Task based on context
func SelectExecutionPath(ctx ExecutionContext) ExecutionPath {
	// Fast Path (RPC) preferred for:
	// - Low-latency requirements (< 100ms)
	// - Synchronous operations (login, verify)
	// - High priority requests (7-10)
	if ctx.Priority >= 7 || ctx.Timeout < 100*time.Millisecond {
		return ExecutionPathRPC
	}

	// Robust Path (Task) preferred for:
	// - Async operations (email sending, batch jobs)
	// - Idempotent operations (can retry)
	// - Long-running workflows (> 5s)
	if ctx.Idempotent || ctx.Timeout > 5*time.Second {
		return ExecutionPathTask
	}

	// Default: Try RPC first, fallback to Task
	return ExecutionPathAuto
}

// ---------------------------------------------------------------------------
// Context helpers
// ---------------------------------------------------------------------------

// GetDomainFromContext extracts the domain from RPCRequest.Context map
// Returns fallback if domain is missing or invalid
// Domain should be set by HTTP layer: auth.hstles.com → hstles.com
func GetDomainFromContext(ctx map[string]interface{}, fallback string) string {
	if ctx == nil {
		return fallback
	}

	domain, ok := ctx["domain"]
	if !ok {
		return fallback
	}

	domainStr, ok := domain.(string)
	if !ok || domainStr == "" {
		return fallback
	}

	return domainStr
}

// GetTenantIDFromContext extracts tenant_id from RPCRequest.Context map
func GetTenantIDFromContext(ctx map[string]interface{}) string {
	if ctx == nil {
		return ""
	}

	tenantID, ok := ctx["tenant_id"]
	if !ok {
		return ""
	}

	tenantIDStr, ok := tenantID.(string)
	if !ok {
		return ""
	}

	return tenantIDStr
}

// ── Transport-level tenant context ──────────────────────────────────────────
// These helpers propagate the tenant ID determined at the transport layer
// (from preamble decode or dedicated-transport mapping) through the Go
// context.Context so that validateTenantScope can verify transport/request
// tenant agreement.

type transportTenantCtxKey struct{}

// WithTransportTenant injects the transport-level tenant ID into context.
// Called by the RPC server when accepting a session from a tenant-aware listener.
func WithTransportTenant(ctx context.Context, tenantID string) context.Context {
	return context.WithValue(ctx, transportTenantCtxKey{}, tenantID)
}

// TransportTenantFromContext extracts the transport-level tenant ID from context.
// Returns "" if no transport tenant was set (e.g., single-tenant fallback).
func TransportTenantFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(transportTenantCtxKey{}).(string); ok {
		return v
	}
	return ""
}

// GetRequestIDFromContext extracts request_id for tracing
func GetRequestIDFromContext(ctx map[string]interface{}) string {
	if ctx == nil {
		return ""
	}

	requestID, ok := ctx["request_id"]
	if !ok {
		return ""
	}

	requestIDStr, ok := requestID.(string)
	if !ok {
		return ""
	}

	return requestIDStr
}
