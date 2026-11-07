package rpc

import (
	"context"
	"net/http"
	"reflect"

	"google.golang.org/protobuf/proto"

	"github.com/bbmumford/loom/node/handlers"
	sharedDispatch "github.com/bbmumford/loom/pkg/dispatch"
	"github.com/bbmumford/loom/pkg/rpc/effect"
	"github.com/bbmumford/loom/pkg/rpc/scope"
)

// ErrGradeTooLow is returned when the best available connection grade is below
// the minimum required for the operation class, and when the request names an
// operation class the grade gate does not know. Callers can use errors.Is to
// distinguish grade failures from transport failures. The dispatcher behind
// Call, CallRaw and CallNode (dispatch.HWPCaller) returns it for the first
// hop, and node's forwarder for every later one, each wrapped in a
// *dispatch.GradeTooLowError (node.GradeTooLowError is the same type) carrying
// the current and required grade. A refusal made on a later hop keeps its
// type on the way back: the refusing node stamps it on its response
// (dispatch.StampRefusal) and HWPCaller rebuilds it, so Call returns it
// rather than a handler error. It is dispatch's sentinel under this
// package's name, because pkg/dispatch cannot import pkg/rpc.
var ErrGradeTooLow = sharedDispatch.ErrGradeTooLow

// ErrRouteDeadline is returned when the caller's remaining deadline cannot
// cover the next hop, or has already passed, before anything is sent. node's
// forwarder returns it wrapped in a *dispatch.RouteDeadlineError
// (node.RouteDeadlineError is the same type) carrying the remaining budget
// and the hop's need, because a forwarded call keeps the caller's deadline
// and never gains time. Like ErrGradeTooLow it is dispatch's sentinel under
// this package's name, and it keeps its type across hops the same way.
var ErrRouteDeadline = sharedDispatch.ErrRouteDeadline

// ErrAmbiguousEffect is returned when a call to an operation that is not
// effect.Class.Repeatable was sent and its outcome is unknown: the handler may
// or may not have run. The dispatcher neither resends it nor sends it on
// another route, and the HTTP bridge answers it with a 502 it marks not
// retryable. It is dispatch's sentinel under this package's name.
var ErrAmbiguousEffect = sharedDispatch.ErrAmbiguousEffect

// ErrAnswerersDisagree is returned when the nodes advertising an operation do
// not name one requirement for it, before anything is sent: such a call would be
// judged by whichever node took it. The HTTP bridge answers it with a 503 it marks
// not retryable. It is dispatch's sentinel under this package's name.
var ErrAnswerersDisagree = sharedDispatch.ErrAnswerersDisagree

// ErrNoAnswerer is returned when a relay finds no node advertising the operation
// and the request names no target, before anything is sent: node's forwarder
// sends only to the operation's answerers. It reaches the origin wrapped in a
// *dispatch.NoAnswererError naming the operation, rebuilt from the relay's
// response, and the origin tries its next route on it. It is dispatch's
// sentinel under this package's name.
var ErrNoAnswerer = sharedDispatch.ErrNoAnswerer

// Effect is the effect class a Handler declares in its Effect field: package
// effect's Class under this package's name, so a declaration names the type and
// its values without a second import.
type Effect = effect.Class

// The effect classes a Handler declares in its Effect field: package effect's
// values under this package's names.
const (
	EffectUnsafe     = effect.Unsafe
	EffectIdempotent = effect.Idempotent
	EffectReadOnly   = effect.ReadOnly
)

// OperationClass is the class a Handler declares for its transport
// requirements. node's grade gate uses grade.OperationClass
// (node/grade_alias.go) and reads the "opClass" context hint set by
// dispatch.WithOpClass, not this value; nothing converts between the two.
//
// The zero value is OpStandard, the class a Handler that declares none
// carries, matching the gate, which holds a request naming no class to the
// Standard floor.
type OperationClass int

const (
	OpStandard OperationClass = iota // CRUD, queries — reliable transport minimum; the default
	OpBulk                           // Telemetry, inventory — any connection works
	OpRealtime                       // Remote access, streaming — fast transport minimum
	OpCritical                       // Key rotation, auth — fast transport + retry
)

// Middleware is a standard HTTP middleware function.
// Handlers declare their own middleware chain — no fixed security tiers.
// This allows each app to compose its own auth/rate-limit/scope stack.
type Middleware = func(http.Handler) http.Handler

// ScopeTag controls scope enforcement at the handler dispatch level: an opaque
// label the handlers package (node/handlers) aliases to the SAME underlying
// rpc/scope type, so a scope value round-trips between the two surfaces without
// drift. loom carries the tag opaquely; a consumer's PresencePolicy decides what
// each tag requires. See rpc/scope/scopetag.go.
type ScopeTag = scope.ScopeTag

// Scope* forwards to the canonical opaque tags in rpc/scope. Callers should
// import "github.com/bbmumford/loom/pkg/rpc" and use rpc.Scope* directly — the
// scope subpackage is an implementation detail. New tags MUST be added
// in rpc/scope/scopetag.go so both re-exports pick them up together.
const (
	ScopeNone = scope.ScopeNone // Opts out of scope enforcement

	// The declarable tags, each naming what must be present on the identity.
	ScopeAllowlisted     = scope.ScopeAllowlisted     // partition the policy admits
	ScopePartition       = scope.ScopePartition       // partition present
	ScopeWithinPartition = scope.ScopeWithinPartition // partition present (evaluated as ScopePartition)
	ScopeSubject         = scope.ScopeSubject         // partition + authenticated subject

	// ScopeUnknown is the fail-closed sentinel for a scope that arrived as
	// something the scope package does not recognise. It is not declarable:
	// registration refuses it, and EnforceScope hands it to the PresencePolicy,
	// which denies it when the policy is nil or the reference policy.
	//
	// No code converts a caller-supplied value into a scope tag, so the only
	// way to hold it is to set it, which the refusals exist to catch.
	ScopeUnknown = scope.ScopeUnknown
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
//	        authmw.RequireAuth("session", "bearer"), // a composition root's middleware
//	        authmw.RequireScopes("identity.write"),
//	    },
//	}
type Handler struct {
	Namespace        string                  // FQN prefix before Domain (one or more dot-separated segments), e.g. "example"
	Domain           string                  // "identity", "device", "policy"
	Operation        string                  // "GetStatus", "ListDevices"
	Func             HandlerFunc             // The handler function (nil for proxy — dispatched over mesh)
	Request          proto.Message           // Nil pointer type hint: (*pb.GetStatusRequest)(nil)
	Response         proto.Message           // Nil pointer type hint: (*pb.GetStatusResponse)(nil)
	Middleware       []Middleware            // HTTP middleware chain applied before handler (auth, rate limit, etc.)
	Scope            ScopeTag                // opaque scope label a PresencePolicy evaluates at mesh dispatch
	Requirement      *scope.Requirement      // two-arm authorization; when set it supersedes Scope at the enforcement seam. nil = fall back to Scope/PresencePolicy.
	Tags             []string                // Classification: "status", "write", "feature:policy.full"
	OpClass          OperationClass          // declared operation class (zero value: OpStandard); loom does not read it, a caller forwards a class as the opClass hint via dispatch.WithOpClass
	Effect           effect.Class            // what a second execution does (zero value: effect.Unsafe); dispatch sends only an effect.Class.Repeatable operation down more than one route or again after an unknown outcome
	WebhookSignature *WebhookSignatureConfig // optional: declare this handler is a webhook-class receiver; the registry runs signature verification pre-dispatch

	// Stream, when set, serves this operation as a handler stream as well
	// (node.serveHandlerStream, reached with OpenNodeStream): frames both ways
	// under the same Requirement the operation's Func is judged by. An
	// operation that is only a stream keeps a Func refusing the single-shot
	// call, because a Func is what exports it to the mesh registry.
	Stream func(ctx context.Context, stream handlers.MessageStream) error
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
//	    Func: rpc.Wrap(func(ctx context.Context, req *pb.GetStatusRequest) (*pb.GetStatusResponse, error) {
//	        return handlers.GetStatus(ctx, req)
//	    }),
//	    Request:  (*pb.GetStatusRequest)(nil),
//	    Response: (*pb.GetStatusResponse)(nil),
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
