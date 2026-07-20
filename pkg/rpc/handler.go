package rpc

import (
	"context"
	"errors"
	"net/http"
	"reflect"

	"google.golang.org/protobuf/proto"

	"github.com/bbmumford/loom/pkg/rpc/scope"
)

// ErrGradeTooLow is returned when the best available connection grade is below
// the minimum required for the operation class. Callers can use errors.Is to
// distinguish grade failures from transport failures.
var ErrGradeTooLow = errors.New("connection grade too low for operation")

// OperationClass categorizes RPC operations by their transport requirements.
// Defined here (not in mesh/node) to avoid import cycles.
type OperationClass int

const (
	OpBulk     OperationClass = iota // Telemetry, inventory — any connection works
	OpStandard                       // CRUD, queries — reliable transport minimum
	OpRealtime                       // Remote access, streaming — fast transport minimum
	OpCritical                       // Key rotation, auth — fast transport + retry
)

// Middleware is a standard HTTP middleware function.
// Handlers declare their own middleware chain — no fixed security tiers.
// This allows each app to compose its own auth/rate-limit/scope stack.
type Middleware = func(http.Handler) http.Handler

// TenantScope controls tenant isolation enforcement at the handler dispatch
// level. It aliases the canonical declaration in the rpc/scope subpackage —
// the handlers package (mesh/node/handlers) aliases the SAME underlying
// type, so a scope value round-trips between the two surfaces with no
// possibility of drift. See rpc/scope/tenantscope.go for the semantics
// of each tier and finding rev-069 for the collapse-to-one-source rationale.
type TenantScope = scope.TenantScope

// Scope* forwards to the canonical constants in rpc/scope. Callers should
// import "github.com/bbmumford/loom/pkg/rpc" and use rpc.Scope* directly — the
// scope subpackage is an implementation detail. New tiers MUST be added
// in rpc/scope/tenantscope.go so both re-exports pick them up together.
const (
	ScopeNone     = scope.None     // No tenant restriction
	ScopePlatform = scope.Platform // HSTLES internal only
	ScopeTenant   = scope.Tenant   // Requires valid tenantId
	ScopeOrg      = scope.Org      // Requires tenant + org membership
	ScopeUser     = scope.User     // Requires tenant + own user data only
	ScopeProfile  = scope.Profile  // Requires tenant + billing-profile membership (per Quorum ADR-0012)
	// ScopeUnknown is the fail-closed sentinel installed by mapProtoScope
	// when it sees a proto enum it does not recognise (including an explicit
	// TENANT_SCOPE_UNSPECIFIED in a .proto file). EnforceScope's default
	// branch rejects any handler bound to this scope. NEVER set this
	// scope manually — it exists only to make decay-to-ScopeNone fail closed.
	ScopeUnknown = scope.Unknown
)

// HandlerFunc is the generic handler function signature.
// All domain handlers are adapted to this signature at registration time.
type HandlerFunc func(ctx context.Context, req proto.Message) (proto.Message, error)

// Handler defines a single RPC operation with all metadata needed
// for dispatch, security, HTTP routing, and service discovery.
//
// Security is defined by the Middleware chain — not a fixed tier enum.
// Each handler composes its own auth/rate-limit/scope stack:
//
//	rpc.Handler{
//	    Middleware: []rpc.Middleware{
//	        hstles.RequireAuth("session", "bearer"),
//	        hstles.RequireScopes("identity.write"),
//	    },
//	}
type Handler struct {
	Namespace        string                  // "platform", "app", "custom"
	Domain           string                  // "identity", "device", "policy"
	Operation        string                  // "CreateUser", "ListDevices"
	Func             HandlerFunc             // The handler function (nil for proxy — dispatched over mesh)
	Request          proto.Message           // Nil pointer type hint: (*pb.CreateUserRequest)(nil)
	Response         proto.Message           // Nil pointer type hint: (*pb.CreateUserResponse)(nil)
	Middleware       []Middleware            // HTTP middleware chain applied before handler (auth, rate limit, etc.)
	Scope            TenantScope             // Tenant scope enforcement at mesh dispatch level
	Tags             []string                // Classification: "users", "write", "feature:policy.full"
	OpClass          OperationClass          // minimum grade requirement for mesh forwarding (default: OpStandard)
	WebhookSignature *WebhookSignatureConfig // optional: declare this handler is a webhook-class receiver; the registry runs signature verification pre-dispatch
}

// FQN returns the fully-qualified handler name: "namespace.domain.Operation"
func (h *Handler) FQN() string {
	return h.Namespace + "." + h.Domain + "." + h.Operation
}

// Role returns the namespace-qualified domain: "namespace.domain"
// Used for LAD publishing and mesh routing.
func (h *Handler) Role() string {
	return h.Namespace + "." + h.Domain
}

// IsPublic returns true if the handler has no middleware (no auth required).
func (h *Handler) IsPublic() bool {
	return len(h.Middleware) == 0
}

// Wrap adapts a typed handler function to the generic HandlerFunc signature.
// This is the bridge between domain-specific typed handlers and the generic registry.
//
// Usage:
//
//	rpc.Handler{
//	    Func: rpc.Wrap(func(ctx context.Context, req *pb.CreateUserRequest) (*pb.CreateUserResponse, error) {
//	        return handlers.CreateUser(ctx, req)
//	    }),
//	    Request:  (*pb.CreateUserRequest)(nil),
//	    Response: (*pb.CreateUserResponse)(nil),
//	}
func Wrap[Req, Resp proto.Message](fn func(ctx context.Context, req Req) (Resp, error)) HandlerFunc {
	return func(ctx context.Context, req proto.Message) (proto.Message, error) {
		return fn(ctx, req.(Req))
	}
}

// NewInstance creates a new zero-value instance from a proto.Message type hint.
// Handles typed nil pointers (e.g., (*pb.FooRequest)(nil)) — proto.Clone returns
// a typed nil which passes interface nil checks but panics on field access.
func NewInstance(hint proto.Message) proto.Message {
	// Always use reflect to create a genuine zero-value instance.
	// proto.Clone of typed nil returns typed nil (not safe to use).
	t := reflect.TypeOf(hint)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return reflect.New(t).Interface().(proto.Message)
}
