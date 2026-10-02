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
// Handler interfaces: HandlerMeta plus one interface per dispatch mode
// ---------------------------------------------------------------------------

// Scope is the opaque scope label a handler declares at registration. It is the
// same type the rpc package exposes as rpc.ScopeTag: both name the single
// declaration in pkg/rpc/scope, so a value crosses between them without
// conversion and the two surfaces cannot drift.
//
// loom assigns a label no meaning. It reads only ScopeNone (the enforcement
// opt-out) and the two non-declarable sentinels; every other label is handed
// unread to the consumer's PresencePolicy, which decides what it requires.
type Scope = scope.ScopeTag

// Scope* forward to the canonical constants in pkg/rpc/scope, which is also
// where a new label must be declared — declaring one here instead would make
// this surface and rpc.Scope* disagree.
const (
	ScopeNone = scope.ScopeNone // Opts out of scope enforcement.

	// The declarable tags, each naming what must be present on the identity.
	ScopeAllowlisted     = scope.ScopeAllowlisted     // pinned axis the policy admits
	ScopePartition       = scope.ScopePartition       // pinned axis present
	ScopeWithinPartition = scope.ScopeWithinPartition // pinned + within-transport axes
	ScopeSubject         = scope.ScopeSubject         // pinned axis + authenticated subject

	// ScopeUnknown is the fail-closed sentinel for a scope the scope package does
	// not recognise. validateScope rejects any handler bound to it, so holding it
	// denies instead of reading as "no scope required".
	//
	// No code converts a caller-supplied value into a scope tag, so a handler
	// holds it only by declaring it, and registerMeta refuses that declaration
	// (scope.IsDeclared). It is the value a conversion must return for an input
	// it does not recognise, because that value already denies.
	ScopeUnknown = scope.ScopeUnknown
)

// HandlerMeta contains metadata common to all handler types.
// Both RPCHandler and TaskHandler embed this interface.
type HandlerMeta interface {
	// Name returns the operation name the registry keys on (e.g.
	// "orbtr.<domain>.<Op>")
	Name() string

	// Role returns the domain role (e.g., "auth", "identity", "notify")
	Role() string

	// RequiresAuth returns true if operation needs authentication
	RequiresAuth() bool

	// AllowedAuthTypes returns acceptable authentication methods. The
	// ports.AuthValidator compares each value, as a string, with the
	// authentication type it reads from the context, so values use that
	// validator's spelling (e.g. "session", "mtls", "api-key", "bearer",
	// "dev"). Empty slice = any auth type accepted (if RequiresAuth is true).
	AllowedAuthTypes() []string

	// Scopes returns required JWT scopes (e.g., ["auth.login"])
	Scopes() []string

	// Scope returns the opaque presence tier the handler requires.
	// Returns ScopeNone for deliberately unrestricted handlers. The zero
	// value is invalid and registration rejects it.
	Scope() Scope

	// AllowedPartitions returns the partition values the handler was declared
	// with (the wrappers' WithScope allowed argument). validateScope does not
	// read it, and neither does the loom-local AuthValidator
	// (securityctx.Default): it reaches a composition root's AuthValidator
	// through ports.SecureHandler, and only that validator can act on it.
	AllowedPartitions() []string
}

// RequirementDeclarer is an optional interface a HandlerMeta implements to
// declare an authorization Requirement (a capability or nexus arm) instead of
// relying on its scope tag. validateScope judges a non-nil Requirement only
// through the registry's authorization policy (SetAuthzPolicy), exactly as
// pkg/rpc EnforceScope does for the HTTP registry; a handler that declares
// none, or returns nil, is judged by its scope tag as before.
type RequirementDeclarer interface {
	Requirement() *scope.Requirement
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
// operations, opened through HandlerRegistry.DispatchStream.
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
	Handler   string                 `json:"handler"`   // "orbtr.<domain>.<Op>"
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
	Operation  string        // operation within the role, e.g. "login", "verify2fa"
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
// It accepts any HandlerMeta; each dispatch method type-asserts the entry to
// RPCHandler, TaskHandler or StreamHandler (a DualHandler serves both RPC and
// task dispatch).
//
// Concurrency: guarded by mu. Runtime role activation (node/role_activation.go)
// registers and unregisters handlers through registration scopes while
// dispatch is live, so every map access takes mu; reads take RLock.
type HandlerRegistry struct {
	mu                 sync.RWMutex
	handlers           map[string]*registrationEntry
	enabledRoles       map[string]bool
	roleMiddleware     map[string][]Middleware // per-role middleware
	globalMiddleware   []Middleware            // runs for all handlers
	policy             scope.PresencePolicy    // consumer-supplied scope evaluator (nil fails closed)
	authz              *scope.Policy           // judges a declared Requirement (nil fails such a handler closed)
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

// SetPresencePolicy configures the consumer-supplied evaluator that decides,
// for every declared scope other than ScopeNone, whether the
// server-established identity satisfies it. loom carries the opaque scope tag
// and identity but interprets neither. A nil policy fails closed:
// validateScope denies every non-None handler when no policy is set.
// Each endpoint should install its policy at init, e.g.
// registry.SetPresencePolicy(myPolicy).
func (r *HandlerRegistry) SetPresencePolicy(p scope.PresencePolicy) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.policy = p
}

// presencePolicy returns the configured scope evaluator (or nil). A nil policy
// fails closed at validateScope for every non-None handler.
func (r *HandlerRegistry) presencePolicy() scope.PresencePolicy {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.policy
}

// PresencePolicy returns the configured scope evaluator (or nil), letting an
// out-of-package composition root confirm SetPresencePolicy seeded this mesh
// registry. It reads the same field validateScope consults, so a nil
// return proves that path still fails closed. Mirrors rpc.Registry.PresencePolicy.
func (r *HandlerRegistry) PresencePolicy() scope.PresencePolicy {
	return r.presencePolicy()
}

// SetAuthzPolicy configures the policy that judges a handler's declared
// Requirement (see RequirementDeclarer). It holds the consumer's capability
// authorizer; loom never imports one. With no policy set, every handler that
// declares a Requirement is denied. Mirrors rpc.Registry.SetAuthzPolicy.
func (r *HandlerRegistry) SetAuthzPolicy(p *scope.Policy) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.authz = p
}

// authzPolicy returns the configured Requirement policy (or nil, which fails
// every Requirement-declaring handler closed at validateScope).
func (r *HandlerRegistry) authzPolicy() *scope.Policy {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.authz
}

// AuthzPolicy returns the configured Requirement policy (or nil), letting a
// composition root confirm SetAuthzPolicy seeded this mesh registry.
func (r *HandlerRegistry) AuthzPolicy() *scope.Policy {
	return r.authzPolicy()
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
	if declared := h.Scope(); !scope.IsDeclared(declared) {
		return RegistrationHandle{}, fmt.Errorf("handler %s has a non-declarable scope %q: declare a concrete scope tag (the ScopeUnset zero value and the ScopeUnknown proto-decay sentinel are refused)", name, declared)
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

// Unregister removes the published handler registered under name, whichever
// registration that is, and reports whether one existed; a pending
// registration is untouched. Role teardown (node/role_activation.go) calls
// UnregisterExact instead, which cannot remove a same-name replacement.
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

// OpenRegistrationScope creates a causal attribution scope for one exact role
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
			// not resurrect it from the attribution history.
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
		// returned attribution snapshot.
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

// validateScope checks that the trusted typed context satisfies the
// handler's declared Requirement or, when it declares none, its scope tag.
// RPCRequest.Context and task Metadata are deliberately ignored here: both are
// mutable request data and cannot establish any identity axis or the subject.
func (r *HandlerRegistry) validateScope(ctx context.Context, h HandlerMeta) error {
	identity := scope.AuthenticatedIdentityFromContext(ctx)

	// A declared Requirement is judged only by the registry's authorization
	// policy, through the function pkg/rpc EnforceScope also calls, so the mesh
	// and HTTP registries admit the same principals; the scope tag is not
	// consulted. An open requirement admits without the transport check below.
	if rd, ok := h.(RequirementDeclarer); ok {
		if req := rd.Requirement(); req != nil {
			if ok, reason := scope.AuthorizeRequirement(ctx, *req, r.authzPolicy()); !ok {
				return fmt.Errorf("%w: handler %s %s", scope.ErrDenied, h.Name(), reason)
			}
			if req.Kind == scope.ArmOpen {
				return nil
			}
			return reconcileTransportPartition(ctx, identity, h.Name())
		}
	}

	declared := h.Scope()
	if declared == scope.ScopeNone {
		// Explicit opt-out — the one tag loom admits without a policy.
		return nil
	}
	if declared == scope.ScopeUnset {
		return fmt.Errorf("%w: handler %s has an undeclared scope (ScopeUnset) — declare a scope explicitly", scope.ErrDenied, h.Name())
	}

	// loom delegates ALL scope semantics to the consumer-supplied policy. A
	// nil policy fails CLOSED: without an evaluator loom cannot judge presence
	// for a handler that opted into enforcement, and the failure mode of this
	// core is a silent fail-OPEN. This also rejects ScopeUnknown (not a
	// declarable tag) via the policy, which must report it unsatisfied.
	policy := r.presencePolicy()
	if policy == nil {
		return fmt.Errorf("%w: handler %s requires a presence policy but none is configured", scope.ErrDenied, h.Name())
	}
	if ok, reason := policy.CheckPresence(declared, identity); !ok {
		return fmt.Errorf("%w: handler %s denied by presence policy: %s", scope.ErrDenied, h.Name(), reason)
	}

	return reconcileTransportPartition(ctx, identity, h.Name())
}

// reconcileTransportPartition denies a session that arrived on a
// partition-bound transport (dedicated, or shared via the preamble) carrying an
// identity pinned to a different partition. A session bound to no partition
// ("") has nothing to compare and is not denied here; every other value is a
// partition and is compared, whatever it is called. It does not interpret any
// scope tag or requirement; it compares the transport binding that
// WithTransportPartition established against the identity's pinned axis, both
// resolved server-side from the same value.
func reconcileTransportPartition(ctx context.Context, identity scope.AuthenticatedIdentity, handler string) error {
	partitionID := identity.Scope(scope.AxisTransportPinned)
	transportPartition := TransportPartitionFromContext(ctx)
	if transportPartition != "" && partitionID != "" {
		if transportPartition != partitionID {
			return fmt.Errorf("%w: transport/authenticated partition mismatch: transport=%q identity=%q for handler %s",
				scope.ErrDenied,
				transportPartition, partitionID, handler)
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
// middleware and application handlers. validateScope deliberately ignores
// this map and consumes only scope.AuthenticatedIdentity.
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
// Dispatch runs validateScope, the middleware and the handler, but not the
// AuthValidator's ValidateExecutionAuth check. Its callers are
// RPCServer.executeLocal (reached from RPCServer.handleRequest), the node's
// local dispatch adapter (node/rpc_local_dispatch.go) and
// dispatch.LocalCaller. An entry point that
// must judge the caller's authentication type and required scopes calls
// [HandlerRegistry.DispatchRPCWithAuth], which runs that check first.
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
//	auth → partition scope → middleware Before → RPCHandler → middleware After
//
// A nil validator uses the fail-closed loom-local validator. A composition
// root passes the validator it injects, so its private context keys, allowed
// authentication types and required scopes are interpreted by that validator
// rather than by loom.
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
	// validateScope does not consume this mutable request map.
	ctx = injectRPCContext(ctx, req.Context)

	// Propagate e2e trace ID — the LocalCaller (in-process dispatch) and
	// any other caller that stamps req.TraceID expect the handler chain
	// to expose it via trace.FromContext. RPCServer.handleRequest in
	// node/rpc.go does the same lift for a request received over the mesh.
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

	// Partition scope enforcement: validateScope judges the declared
	// Requirement or presence tier against the authenticated identity.
	if err := h.registry.validateScope(ctx, h.meta); err != nil {
		return &RPCResponse{
			Success: false,
			Error:   fmt.Sprintf("scope denied: %v", err),
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
			// released; returning straight away would leak them.
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
// task pipeline with the fail-closed loom-local auth validator
// (securityctx.Default). A composition root that injects its own validator
// calls [HandlerRegistry.DispatchTaskWithAuth] with it, so the check reads that
// validator's private context keys.
func (r *HandlerRegistry) DispatchTask(ctx context.Context, task *Task) (*TaskResult, error) {
	return r.DispatchTaskWithAuth(ctx, task, securityctx.Default())
}

// DispatchTaskWithAuth is the canonical task execution pipeline shared by
// compose dispatch and the asynchronous task executor:
//
//	auth → partition scope → middleware Before → TaskHandler → middleware After
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
	// validateScope does not consume this mutable task map.
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

	// Partition scope enforcement: validateScope judges the declared
	// Requirement or presence tier against the authenticated identity.
	if err := h.registry.validateScope(ctx, h.meta); err != nil {
		return &TaskResult{
			TaskID: task.ID,
			Status: TaskStatusFailed,
			Error:  fmt.Sprintf("scope denied: %v", err),
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

	// Partition scope enforcement: validateScope judges the declared
	// Requirement or presence tier against the authenticated identity.
	if err := r.validateScope(ctx, h); err != nil {
		return fmt.Errorf("scope denied: %w", err)
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

// ── Transport-bound partition context ───────────────────────────────────────
// These helpers propagate the transport-resolved partition — the consumer-pinned
// opaque axis value established at the transport layer — through context.Context
// so validateScope reconciles the transport binding against the canonical
// typed identity. The value is an opaque partition identifier; loom attaches no
// class or level to it.

type transportPartitionCtxKey struct{}

// WithTransportPartition binds the transport-resolved partition onto ctx and sets
// the pinned-axis component of the canonical typed scope identity
// (scope.AxisTransportPinned). RPCServer.HandleIncomingSessionsForPartition
// (node/rpc.go) and the runtime's listener accept loop (node/runtime.go) call it
// for every accepted session, with the partition the session carries or, when
// it carries none, the listener's fallback: a dedicated listener's own
// partition, or "" on the shared preamble listener and the unpartitioned one.
//
// "" binds no partition and stamps no axis: scope.WithAuthenticatedScope
// refuses an empty value, so the identity is left with no axis and any identity
// or principal already on ctx is cleared rather than carried onto the session.
func WithTransportPartition(ctx context.Context, partition string) context.Context {
	ctx = context.WithValue(ctx, transportPartitionCtxKey{}, partition)
	return scope.WithAuthenticatedScope(ctx, scope.AxisTransportPinned, partition)
}

// TransportPartitionFromContext returns the partition WithTransportPartition
// bound onto ctx, or "" when nothing bound one (an in-process dispatch) or the
// session is bound to no partition. loom's readers treat "" alone as unbound:
// reconcileTransportPartition skips its comparison,
// RPCServer.handleRequest skips WithPartitionID and
// RPCServer.bindAuthenticatedPrincipal refuses a mesh principal. Any other
// value is a partition, whatever its name.
func TransportPartitionFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(transportPartitionCtxKey{}).(string); ok {
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
