/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package handlers

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/bbmumford/loom/internal/securityctx"
	"github.com/bbmumford/loom/pkg/rpc/scope"
	"github.com/bbmumford/loom/pkg/trace"
	"github.com/bbmumford/loom/ports"
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
	TenantScopeNone     = scope.None     // Explicitly opts out of tenant restriction. Mirrors rpc.ScopeNone.
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
	// Returns TenantScopeNone for deliberately unrestricted handlers. The zero
	// value is invalid and registration rejects it.
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
	ID        string                 `json:"id"`        // Request ID (idempotency)
	Handler   string                 `json:"handler"`   // "auth.login", "identity.getUser"
	Payload   []byte                 `json:"payload"`   // Proto-encoded operation data
	Context   map[string]interface{} `json:"context"`   // Session, trace IDs
	Timeout   time.Duration          `json:"timeout"`   // Max execution time
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
	CreatedAt   time.Time              `json:"createdAt"`   // When task was created
	SessionID   string                 `json:"sessionId"`   // Optional session identifier
	TraceID     string                 `json:"traceId"`     // Optional trace identifier
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
	Status      TaskStatus             `json:"status"`      // Completed, Failed, Timeout
	Payload     []byte                 `json:"payload"`     // Proto-encoded result data
	Error       string                 `json:"error"`       // Error if failed
	Duration    time.Duration          `json:"duration"`    // Actual execution time
	CompletedAt time.Time              `json:"completedAt"` // When task completed
	NodeID      string                 `json:"nodeId"`      // Which node executed
	Metadata    map[string]interface{} `json:"metadata"`    // Extra context
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
	mu                 sync.RWMutex
	handlers           map[string]*registrationEntry
	enabledRoles       map[string]bool
	roleMiddleware     map[string][]Middleware // per-role middleware
	globalMiddleware   []Middleware            // runs for all handlers
	platformTenants    scope.PlatformTenants   // exact, fail-closed platform allowlist
	registrationScopes map[*registrationScopeToken]*registrationScopeState
	pendingHandlers    map[string]*registrationEntry
	nextScopeID        uint64
	scopeIDsExhausted  bool
}

type registrationEntry struct {
	name      string
	meta      HandlerMeta
	admission TaskAdmission
}

// registrationScopeToken must remain non-zero-sized. Go permits pointers to
// distinct zero-sized allocations to compare equal (and currently lowers
// them to runtime.zerobase), which would collapse simultaneous activation
// scopes into one map key. issuer + id also make issuance identity explicit
// and reject a token presented to a different registry before map lookup.
type registrationScopeToken struct {
	issuer *HandlerRegistry
	id     uint64
}

type registrationScopeState struct {
	role       string
	admission  TaskAdmission
	open       bool
	published  bool
	registered []RegistrationHandle
}

// RegistrationHandle is an immutable identity for one exact successful
// registration. Its fields are deliberately private: only the issuing
// registry can compare and remove the represented entry.
type RegistrationHandle struct {
	registry *HandlerRegistry
	entry    *registrationEntry
}

// Valid reports whether the handle names an issued registration.
func (h RegistrationHandle) Valid() bool {
	return h.registry != nil && h.entry != nil
}

// TaskAdmission is a generation-bound execution lease captured with a
// [ResolvedHandler]. Acquire returns a release function when the exact
// generation is still accepting work. A closed generation returns ok=false,
// even if a replacement handler with the same name has since been registered.
//
// Runtime role activation installs this seam so role resources cannot be
// released while a task admitted against that activation generation is still
// executing. Registries without a capture hook preserve their standalone
// behaviour.
type TaskAdmission interface {
	Acquire() (release func(), ok bool)
}

// NewHandlerRegistry creates a new handler registry
func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{
		handlers:           make(map[string]*registrationEntry),
		enabledRoles:       make(map[string]bool),
		roleMiddleware:     make(map[string][]Middleware),
		registrationScopes: make(map[*registrationScopeToken]*registrationScopeState),
		pendingHandlers:    make(map[string]*registrationEntry),
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
	r.platformTenants = scope.NewPlatformTenants(tenants...)
}

// isPlatformTenant delegates to the same canonical fail-closed evaluator used
// by rpc.EnforceScope. An unset or empty allowlist authorizes nobody.
func (r *HandlerRegistry) isPlatformTenant(tenantID string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.platformTenants.IsPlatform(tenantID)
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
	_, err := r.registerMeta(ports.RegistrationScope{}, h)
	return err
}

// RegisterTask registers an asynchronous task handler.
func (r *HandlerRegistry) RegisterTask(h TaskHandler) error {
	_, err := r.registerMeta(ports.RegistrationScope{}, h)
	return err
}

// RegisterStream registers a streaming handler.
func (r *HandlerRegistry) RegisterStream(h StreamHandler) error {
	_, err := r.registerMeta(ports.RegistrationScope{}, h)
	return err
}

// RegisterHandler registers any handler that implements HandlerMeta.
// The handler must also implement RPCHandler, TaskHandler, or both.
func (r *HandlerRegistry) RegisterHandler(h HandlerMeta) error {
	_, err := r.registerMeta(ports.RegistrationScope{}, h)
	return err
}

// RegisterRPCScoped registers an RPC handler as causally owned by scope.
func (r *HandlerRegistry) RegisterRPCScoped(
	registrationScope ports.RegistrationScope,
	h RPCHandler,
) (RegistrationHandle, error) {
	return r.registerMeta(registrationScope, h)
}

// RegisterTaskScoped registers a task handler as causally owned by scope.
func (r *HandlerRegistry) RegisterTaskScoped(
	registrationScope ports.RegistrationScope,
	h TaskHandler,
) (RegistrationHandle, error) {
	return r.registerMeta(registrationScope, h)
}

// RegisterStreamScoped registers a stream handler as causally owned by scope.
func (r *HandlerRegistry) RegisterStreamScoped(
	registrationScope ports.RegistrationScope,
	h StreamHandler,
) (RegistrationHandle, error) {
	return r.registerMeta(registrationScope, h)
}

// RegisterHandlerScoped registers generic handler metadata as causally owned
// by scope.
func (r *HandlerRegistry) RegisterHandlerScoped(
	registrationScope ports.RegistrationScope,
	h HandlerMeta,
) (RegistrationHandle, error) {
	return r.registerMeta(registrationScope, h)
}

func (r *HandlerRegistry) registerMeta(
	registrationScope ports.RegistrationScope,
	h HandlerMeta,
) (RegistrationHandle, error) {
	if h == nil {
		return RegistrationHandle{}, fmt.Errorf("handler cannot be nil")
	}

	name := h.Name()
	if name == "" {
		return RegistrationHandle{}, fmt.Errorf("handler name cannot be empty")
	}
	if declared := h.TenantScope(); !scope.IsDeclared(declared) {
		return RegistrationHandle{}, fmt.Errorf("handler %s has invalid tenant scope %q: declare one of the TenantScope* tiers explicitly", name, declared)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	var admission TaskAdmission
	var state *registrationScopeState
	if token := registrationScopeTokenOf(registrationScope); token != nil {
		state = r.registrationScopes[token]
		if state == nil || !state.open {
			return RegistrationHandle{}, fmt.Errorf("handler %s: registration scope is closed or invalid", name)
		}
		if h.Role() != state.role {
			return RegistrationHandle{}, fmt.Errorf(
				"handler %s role %q does not match registration scope role %q",
				name,
				h.Role(),
				state.role,
			)
		}
		admission = state.admission
	} else if registrationScope.Token() != nil {
		return RegistrationHandle{}, fmt.Errorf("handler %s: registration scope is invalid", name)
	}
	if _, exists := r.handlers[name]; exists {
		return RegistrationHandle{}, fmt.Errorf("handler %s already registered", name)
	}
	if _, exists := r.pendingHandlers[name]; exists {
		return RegistrationHandle{}, fmt.Errorf(
			"handler %s is reserved by a pending role activation",
			name,
		)
	}

	entry := &registrationEntry{
		name:      name,
		meta:      h,
		admission: admission,
	}
	handle := RegistrationHandle{registry: r, entry: entry}
	if state != nil {
		state.registered = append(state.registered, handle)
	}
	if state != nil && !state.published {
		// Registration performed by an activator is causally owned immediately
		// but is not a local-routing fact until that exact activation succeeds.
		// Reserving the name prevents a concurrent registration from stealing
		// the publication slot without exposing it through Resolve/GetMeta.
		r.pendingHandlers[name] = entry
	} else {
		r.handlers[name] = entry
	}
	return handle, nil
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

// UnregisterExact removes only the exact entry represented by handle. A
// same-name replacement or a handle issued by another registry is untouched.
func (r *HandlerRegistry) UnregisterExact(handle RegistrationHandle) bool {
	if handle.registry != r || handle.entry == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.handlers[handle.entry.name]
	if current == handle.entry {
		delete(r.handlers, handle.entry.name)
		return true
	}
	pending := r.pendingHandlers[handle.entry.name]
	if pending == handle.entry {
		delete(r.pendingHandlers, handle.entry.name)
		return true
	}
	return false
}

// OpenRegistrationScope creates a causal ownership scope for one exact role
// activation generation.
func (r *HandlerRegistry) OpenRegistrationScope(
	role string,
	admission TaskAdmission,
) (ports.RegistrationScope, error) {
	if role == "" || admission == nil {
		return ports.RegistrationScope{}, fmt.Errorf("registration scope requires role and admission")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.scopeIDsExhausted || r.nextScopeID == ^uint64(0) {
		r.scopeIDsExhausted = true
		return ports.RegistrationScope{}, fmt.Errorf("registration scope identity exhausted")
	}
	r.nextScopeID++
	token := &registrationScopeToken{
		issuer: r,
		id:     r.nextScopeID,
	}
	r.registrationScopes[token] = &registrationScopeState{
		role:      role,
		admission: admission,
		open:      true,
	}
	return ports.NewRegistrationScope(token), nil
}

// PublishRegistrationScope atomically makes every still-owned registration in
// a pending activation scope visible to lookup and local dispatch. The scope's
// names are reserved from registration time, so publication cannot overwrite a
// live neighbour. Registrations added after publication are visible
// immediately and remain owned by the same exact scope.
func (r *HandlerRegistry) PublishRegistrationScope(
	registrationScope ports.RegistrationScope,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	token := registrationScopeTokenOf(registrationScope)
	if token == nil || token.issuer != r {
		return fmt.Errorf("registration scope is invalid")
	}
	state := r.registrationScopes[token]
	if state == nil || !state.open {
		return fmt.Errorf("registration scope is closed or invalid")
	}
	if state.published {
		return nil
	}

	for _, handle := range state.registered {
		entry := handle.entry
		if entry == nil || r.pendingHandlers[entry.name] != entry {
			// Exact unregister before publication is a valid withdrawal. Do
			// not resurrect it from the ownership history.
			continue
		}
		if _, exists := r.handlers[entry.name]; exists {
			return fmt.Errorf(
				"handler %s became registered before scope publication",
				entry.name,
			)
		}
	}
	for _, handle := range state.registered {
		entry := handle.entry
		if entry == nil || r.pendingHandlers[entry.name] != entry {
			continue
		}
		delete(r.pendingHandlers, entry.name)
		r.handlers[entry.name] = entry
	}
	state.published = true
	return nil
}

// SealRegistrationScope prevents further causal registrations and returns an
// immutable snapshot of every exact registration already owned by it.
func (r *HandlerRegistry) SealRegistrationScope(
	registrationScope ports.RegistrationScope,
) ([]RegistrationHandle, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	token := registrationScopeTokenOf(registrationScope)
	if token == nil || token.issuer != r {
		return nil, false
	}
	state := r.registrationScopes[token]
	if state == nil {
		return nil, false
	}
	state.open = false
	return append([]RegistrationHandle(nil), state.registered...), true
}

// ReopenRegistrationScope re-enables a previously sealed live scope.
func (r *HandlerRegistry) ReopenRegistrationScope(
	registrationScope ports.RegistrationScope,
) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	token := registrationScopeTokenOf(registrationScope)
	if token == nil || token.issuer != r {
		return false
	}
	state := r.registrationScopes[token]
	if state == nil {
		return false
	}
	state.open = true
	return true
}

// CloseRegistrationScope atomically seals and retires a scope, returning the
// exact registrations it owned. Later scoped registration fails closed.
func (r *HandlerRegistry) CloseRegistrationScope(
	registrationScope ports.RegistrationScope,
) ([]RegistrationHandle, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	token := registrationScopeTokenOf(registrationScope)
	if token == nil || token.issuer != r {
		return nil, false
	}
	state := r.registrationScopes[token]
	if state == nil {
		return nil, false
	}
	state.open = false
	if !state.published {
		// An unpublished scope has no public registrations to preserve. Release
		// all exact reservations atomically with retirement so a failed
		// activation cannot strand names even if its caller ignores the
		// returned ownership snapshot.
		for _, handle := range state.registered {
			entry := handle.entry
			if entry != nil && r.pendingHandlers[entry.name] == entry {
				delete(r.pendingHandlers, entry.name)
			}
		}
	}
	delete(r.registrationScopes, token)
	return append([]RegistrationHandle(nil), state.registered...), true
}

func registrationScopeTokenOf(registrationScope ports.RegistrationScope) *registrationScopeToken {
	token, _ := registrationScope.Token().(*registrationScopeToken)
	return token
}

// ---------------------------------------------------------------------------
// Lookup
// ---------------------------------------------------------------------------

// ResolvedHandler is an immutable handler-registration snapshot. It keeps
// asynchronous dispatch bound to the exact handler that was classified,
// even if role teardown unregisters it and a replacement with the same name
// is installed before the goroutine starts.
//
// The zero value is invalid. Obtain snapshots through
// [HandlerRegistry.Resolve].
type ResolvedHandler struct {
	registry  *HandlerRegistry
	name      string
	meta      HandlerMeta
	admission TaskAdmission
}

// Resolve captures the handler currently registered under name.
func (r *HandlerRegistry) Resolve(name string) (ResolvedHandler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	entry, ok := r.handlers[name]
	if !ok {
		return ResolvedHandler{}, false
	}
	return ResolvedHandler{
		registry:  r,
		name:      name,
		meta:      entry.meta,
		admission: entry.admission,
	}, true
}

// Meta returns the exact handler metadata captured by Resolve.
func (h ResolvedHandler) Meta() HandlerMeta {
	return h.meta
}

func (h ResolvedHandler) acquireAdmission() (func(), bool) {
	if h.admission == nil {
		return func() {}, true
	}
	release, ok := h.admission.Acquire()
	if !ok {
		return nil, false
	}
	if release == nil {
		release = func() {}
	}
	return release, true
}

// GetMeta retrieves a handler by name as HandlerMeta.
func (r *HandlerRegistry) GetMeta(name string) (HandlerMeta, bool) {
	resolved, ok := r.Resolve(name)
	if !ok {
		return nil, false
	}
	return resolved.Meta(), true
}

// GetByRoleMeta retrieves all handlers for a specific role as HandlerMeta.
func (r *HandlerRegistry) GetByRoleMeta(role string) []HandlerMeta {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var result []HandlerMeta
	for _, entry := range r.handlers {
		if entry.meta.Role() == role {
			result = append(result, entry.meta)
		}
	}
	return result
}

// AllHandlers returns all registered handlers as HandlerMeta.
func (r *HandlerRegistry) AllHandlers() []HandlerMeta {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]HandlerMeta, 0, len(r.handlers))
	for _, entry := range r.handlers {
		result = append(result, entry.meta)
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

// validateTenantScope checks that the trusted typed context satisfies the
// handler's tenant scope. RPCRequest.Context and task Metadata are deliberately
// ignored here: both are mutable request data and cannot establish platform,
// organisation, or user identity.
func (r *HandlerRegistry) validateTenantScope(ctx context.Context, h HandlerMeta) error {
	declared := h.TenantScope()
	identity := scope.AuthenticatedIdentityFromContext(ctx)
	tenantID := identity.PlatformTenantID
	switch scope.CheckPresence(declared, identity, r.isPlatformTenant(tenantID)) {
	case scope.PresenceSatisfied:
		// Continue through allowlist and transport reconciliation.
	case scope.PresencePlatformRequired:
		return fmt.Errorf("%w: handler %s requires authenticated platform access, got tenant: %q", scope.ErrDenied, h.Name(), tenantID)
	case scope.PresenceTenantRequired:
		return fmt.Errorf("%w: handler %s requires authenticated platform tenant context", scope.ErrDenied, h.Name())
	case scope.PresenceOrganizationRequired:
		return fmt.Errorf("%w: handler %s requires authenticated organisation context", scope.ErrDenied, h.Name())
	case scope.PresenceUserRequired:
		return fmt.Errorf("%w: handler %s requires authenticated user context", scope.ErrDenied, h.Name())
	case scope.PresenceUnknownScope:
		if declared == TenantScopeUnknown {
			return fmt.Errorf("%w: handler %s has fail-closed TenantScopeUnknown — add an explicit scope declaration", scope.ErrDenied, h.Name())
		}
		return fmt.Errorf("%w: handler %s declares unknown tenant scope %q — reject to preserve isolation guarantee", scope.ErrDenied, h.Name(), declared)
	default:
		return fmt.Errorf("%w: handler %s declares unknown tenant scope %q — reject to preserve isolation guarantee", scope.ErrDenied, h.Name(), declared)
	}

	if declared == TenantScopeNone {
		return nil
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
			return fmt.Errorf("%w: tenant %q not authorized for handler %s", scope.ErrDenied, tenantID, h.Name())
		}
	}

	// Transport-level verification: if the session arrived on a tenant-specific
	// transport (dedicated or shared preamble), the request's tenant_id must match.
	// "default" transport (single-tenant fallback) allows any request tenant.
	transportTenant := TransportTenantFromContext(ctx)
	if transportTenant != "" && transportTenant != "default" && tenantID != "" {
		if transportTenant != tenantID {
			return fmt.Errorf("%w: transport/authenticated tenant mismatch: transport=%q identity=%q for handler %s",
				scope.ErrDenied,
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

// injectRPCContext merges mutable request hints into the Go context for
// middleware and application handlers. The tenant-scope gate deliberately
// ignores this map and consumes only scope.AuthenticatedIdentity.
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
//
// This legacy surface preserves its existing internal-call semantics and
// does not add a second authorization decision. Product runtimes and other
// authority-bearing entry points must use [HandlerRegistry.DispatchRPCWithAuth].
func (r *HandlerRegistry) Dispatch(ctx context.Context, req *RPCRequest) (*RPCResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("RPC request is nil")
	}
	resolved, ok := r.Resolve(req.Handler)
	if !ok {
		return nil, ErrHandlerNotFound
	}
	return resolved.dispatchRPC(ctx, req, nil, false)
}

// DispatchRPCWithAuth resolves and executes an RPC through the authenticated
// pipeline:
//
//	auth → tenant scope → middleware Before → RPCHandler → middleware After
//
// A nil validator uses the fail-closed Loom-local validator. Product runtimes
// must pass the validator injected at composition so private product context
// keys, allowed authentication types, and required scopes are interpreted by
// the owning product rather than by Loom.
func (r *HandlerRegistry) DispatchRPCWithAuth(
	ctx context.Context,
	req *RPCRequest,
	auth ports.AuthValidator,
) (*RPCResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("RPC request is nil")
	}
	resolved, ok := r.Resolve(req.Handler)
	if !ok {
		return nil, ErrHandlerNotFound
	}
	return resolved.DispatchRPCWithAuth(ctx, req, auth)
}

// DispatchRPCWithAuth executes an RPC against the exact registration captured
// by Resolve. A same-name replacement cannot steal an already-classified
// call, and the exact activation generation remains leased across
// authorization, middleware, and handler execution.
func (h ResolvedHandler) DispatchRPCWithAuth(
	ctx context.Context,
	req *RPCRequest,
	auth ports.AuthValidator,
) (*RPCResponse, error) {
	return h.dispatchRPC(ctx, req, auth, true)
}

func (h ResolvedHandler) dispatchRPC(
	ctx context.Context,
	req *RPCRequest,
	auth ports.AuthValidator,
	validateAuth bool,
) (*RPCResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("RPC request is nil")
	}
	if h.registry == nil || h.meta == nil {
		return nil, ErrHandlerNotFound
	}
	if req.Handler != h.name {
		return nil, fmt.Errorf(
			"resolved handler %q does not match RPC handler %q",
			h.name,
			req.Handler,
		)
	}
	rpc, ok := h.meta.(RPCHandler)
	if !ok {
		return nil, ErrUnsupportedMode
	}

	releaseAdmission, admitted := h.acquireAdmission()
	if !admitted {
		return &RPCResponse{
			Success: false,
			Error:   "handler activation generation is not accepting RPC calls",
		}, nil
	}
	defer releaseAdmission()

	// Surface RPCRequest.Context to middleware and application handlers.
	// validateTenantScope does not consume this mutable request map.
	ctx = injectRPCContext(ctx, req.Context)

	// Propagate e2e trace ID — the LocalCaller (in-process dispatch) and
	// any other caller that stamps req.TraceID expect the handler chain
	// to expose it via trace.FromContext. The HWP-mesh receiving path
	// does the same lift in mesh/node/rpc.go.
	if req.TraceID != "" {
		ctx = trace.WithID(ctx, req.TraceID)
	}

	if validateAuth {
		if auth == nil {
			auth = securityctx.Default()
		}
		if err := auth.ValidateExecutionAuth(ctx, h.meta); err != nil {
			return &RPCResponse{
				Success: false,
				Error:   fmt.Sprintf("authorization failed: %v", err),
			}, nil
		}
	}

	// 🔒 TENANT SCOPE ENFORCEMENT
	if err := h.registry.validateTenantScope(ctx, h.meta); err != nil {
		return &RPCResponse{
			Success: false,
			Error:   fmt.Sprintf("tenant restriction: %v", err),
		}, nil
	}

	// Run middleware Before hooks
	middleware := h.registry.GetMiddleware(h.meta.Role())
	var err error
	for idx, mw := range middleware {
		ctx, err = mw.Before(ctx, h.meta.Name(), req)
		if err != nil {
			// A later Before rejected the request. Run After (reverse
			// order) for the middlewares whose Before already completed so any
			// resource they acquired in Before (span, semaphore, tx, counter) is
			// released — the previous code returned immediately and leaked them.
			rejection := &RPCResponse{Success: false, Error: err.Error()}
			rr, re := rejection, err
			for i := idx - 1; i >= 0; i-- {
				rr, re = middleware[i].After(ctx, h.meta.Name(), rr, re)
			}
			if rr != nil {
				rejection = rr
			}
			return rejection, nil
		}
	}

	// Dispatch based on interface
	resp, err := rpc.ExecuteRPC(ctx, req)

	// Run middleware After hooks (reverse order)
	for i := len(middleware) - 1; i >= 0; i-- {
		resp, err = middleware[i].After(ctx, h.meta.Name(), resp, err)
	}

	return resp, err
}

// DispatchTask looks up a handler by name and executes it through the complete
// task pipeline. Pure-Loom callers use the fail-closed local auth validator;
// platform runtimes should call [HandlerRegistry.DispatchTaskWithAuth] with
// their injected validator so it reads the platform's private context keys.
func (r *HandlerRegistry) DispatchTask(ctx context.Context, task *Task) (*TaskResult, error) {
	return r.DispatchTaskWithAuth(ctx, task, securityctx.Default())
}

// DispatchTaskWithAuth is the canonical task execution pipeline shared by
// compose dispatch and the asynchronous task executor:
//
//	auth → tenant scope → middleware Before → TaskHandler → middleware After
//
// After hooks always unwind in reverse order for every Before hook that
// completed, including handler errors and later-Before rejection. A nil auth
// validator falls back to the fail-closed Pure-Loom validator.
func (r *HandlerRegistry) DispatchTaskWithAuth(
	ctx context.Context,
	task *Task,
	auth ports.AuthValidator,
) (*TaskResult, error) {
	if task == nil {
		return nil, fmt.Errorf("task is nil")
	}
	resolved, ok := r.Resolve(task.Handler)
	if !ok {
		return nil, ErrHandlerNotFound
	}
	return resolved.DispatchTaskWithAuth(ctx, task, auth)
}

// DispatchTaskWithAuth executes task through the canonical registry pipeline
// using the exact handler captured by Resolve. A task cannot be relabelled
// after resolution: its handler name must match the captured registration.
func (h ResolvedHandler) DispatchTaskWithAuth(
	ctx context.Context,
	task *Task,
	auth ports.AuthValidator,
) (*TaskResult, error) {
	if task == nil {
		return nil, fmt.Errorf("task is nil")
	}
	if h.registry == nil || h.meta == nil {
		return nil, ErrHandlerNotFound
	}
	if task.Handler != h.name {
		return nil, fmt.Errorf(
			"resolved handler %q does not match task handler %q",
			h.name,
			task.Handler,
		)
	}

	th, ok := h.meta.(TaskHandler)
	if !ok {
		return nil, ErrUnsupportedMode
	}

	releaseAdmission, admitted := h.acquireAdmission()
	if !admitted {
		return &TaskResult{
			TaskID: task.ID,
			Status: TaskStatusFailed,
			Error:  "handler activation generation is not accepting tasks",
		}, nil
	}
	defer releaseAdmission()

	// Surface task metadata to middleware and application handlers.
	// validateTenantScope does not consume this mutable task map.
	ctx = injectRPCContext(ctx, task.Metadata)

	if auth == nil {
		auth = securityctx.Default()
	}
	if err := auth.ValidateExecutionAuth(ctx, h.meta); err != nil {
		return &TaskResult{
			TaskID: task.ID,
			Status: TaskStatusFailed,
			Error:  fmt.Sprintf("authorization failed: %v", err),
		}, nil
	}

	// 🔒 TENANT SCOPE ENFORCEMENT
	if err := h.registry.validateTenantScope(ctx, h.meta); err != nil {
		return &TaskResult{
			TaskID: task.ID,
			Status: TaskStatusFailed,
			Error:  fmt.Sprintf("tenant restriction: %v", err),
		}, nil
	}

	// Run middleware Before hooks (convert task to RPCRequest for middleware)
	middleware := h.registry.GetMiddleware(h.meta.Role())
	var err error
	rpcReq := &RPCRequest{
		ID:      task.ID,
		Handler: task.Handler,
		Payload: task.Payload,
		Context: task.Metadata,
	}
	for idx, mw := range middleware {
		ctx, err = mw.Before(ctx, h.name, rpcReq)
		if err != nil {
			result := &TaskResult{
				TaskID: task.ID,
				Status: TaskStatusFailed,
				Error:  err.Error(),
			}
			result, _ = unwindTaskMiddleware(ctx, h.name, middleware[:idx], result, err)
			return result, nil
		}
	}

	result, err := th.ExecuteTask(ctx, task)
	if result == nil {
		result = &TaskResult{
			TaskID: task.ID,
			Status: TaskStatusFailed,
		}
	} else if result.TaskID == "" {
		result.TaskID = task.ID
	}
	return unwindTaskMiddleware(ctx, h.name, middleware, result, err)
}

func unwindTaskMiddleware(
	ctx context.Context,
	handlerName string,
	middleware []Middleware,
	result *TaskResult,
	handlerErr error,
) (*TaskResult, error) {
	if result == nil {
		result = &TaskResult{Status: TaskStatusFailed}
	}

	resp := &RPCResponse{
		ID:       result.TaskID,
		Success:  handlerErr == nil && result.Status == TaskStatusCompleted,
		Payload:  result.Payload,
		Error:    result.Error,
		Metadata: result.Metadata,
	}
	if handlerErr != nil && resp.Error == "" {
		resp.Error = handlerErr.Error()
	}

	err := handlerErr
	for i := len(middleware) - 1; i >= 0; i-- {
		resp, err = middleware[i].After(ctx, handlerName, resp, err)
	}

	if resp != nil {
		result.Payload = resp.Payload
		result.Metadata = resp.Metadata
		if !resp.Success {
			result.Status = TaskStatusFailed
			result.Error = resp.Error
		}
	}
	if err != nil && result.Error == "" {
		result.Status = TaskStatusFailed
		result.Error = err.Error()
	}
	return result, err
}

// DispatchStream looks up a handler by name and opens a stream, running
// the middleware chain around the stream lifecycle.
// Before() fires on stream open, After() fires on stream close.
// Returns ErrHandlerNotFound if no handler is registered for the name.
// Returns ErrUnsupportedMode if the handler doesn't implement StreamHandler.
func (r *HandlerRegistry) DispatchStream(ctx context.Context, handlerName string, stream MessageStream) error {
	resolved, ok := r.Resolve(handlerName)
	if !ok {
		return ErrHandlerNotFound
	}
	h := resolved.meta

	sh, ok := h.(StreamHandler)
	if !ok {
		return ErrUnsupportedMode
	}
	releaseAdmission, admitted := resolved.acquireAdmission()
	if !admitted {
		return fmt.Errorf("handler activation generation is not accepting streams")
	}
	defer releaseAdmission()

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

// GetTenantIDFromContext is a legacy accessor for the snake_case
// RPCRequest.Context hint. It is retained for application compatibility only;
// validateTenantScope never treats this mutable map as identity authority.
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
// These helpers propagate the server-resolved tenant ID determined at the
// transport layer through context.Context so validateTenantScope can reconcile
// the transport binding with the canonical typed identity.

type transportTenantCtxKey struct{}

// WithTransportTenant injects the transport-level tenant ID into context and
// establishes the platform-tenant component of the canonical typed scope
// identity. Called by the RPC server after accepting a session from a
// tenant-aware listener.
func WithTransportTenant(ctx context.Context, tenantID string) context.Context {
	ctx = context.WithValue(ctx, transportTenantCtxKey{}, tenantID)
	return scope.WithAuthenticatedPlatformTenant(ctx, tenantID)
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
