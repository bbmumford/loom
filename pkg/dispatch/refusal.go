/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */
package dispatch

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/rpc/pb"
	"github.com/bbmumford/loom/grade"
	"github.com/bbmumford/loom/node/handlers"
	rpcscope "github.com/bbmumford/loom/pkg/rpc/scope"
)

// The typed route refusals live here because every dispatcher returns them:
// HWPCaller in this package, and node's forwarder and session finder, which
// import this package. pkg/rpc re-exports the sentinels and node aliases the
// grade and deadline types, so callers test either spelling with errors.Is and
// errors.As.

// ErrGradeTooLow is what every grade refusal unwraps to: the best route on
// offer grades below the floor of the request's operation class, or the
// request names a class grade.ParseOperationClass does not declare.
var ErrGradeTooLow = errors.New("connection grade too low for operation")

// ErrRouteDeadline is what every deadline refusal unwraps to: the caller's
// remaining time does not cover the hop the request would have been sent to,
// so it was not sent on, or the request reached a server after its deadline
// had passed, so it was not run.
var ErrRouteDeadline = errors.New("remaining deadline does not cover the next hop")

// GradeTooLowError is the typed grade refusal. Current is the best grade among
// the candidate routes and Required the floor of the request's class; for an
// undeclared class both are zero, since it has no floor.
type GradeTooLowError struct {
	Handler      string
	Class        string // the "opClass" hint as the request carried it; "" is Standard
	Current      grade.Grade
	Required     grade.Grade
	UnknownClass bool
}

func (e *GradeTooLowError) Error() string {
	if e.UnknownClass {
		return fmt.Sprintf("%s: operation class %q is not declared: %v",
			e.Handler, e.Class, ErrGradeTooLow)
	}
	class := e.Class
	if class == "" {
		class = grade.OpClassStandard.String()
	}
	return fmt.Sprintf("%s: best path grade %s is below the %s floor of operation class %q: %v",
		e.Handler, e.Current, e.Required, class, ErrGradeTooLow)
}

func (e *GradeTooLowError) Unwrap() error { return ErrGradeTooLow }

// RouteDeadlineError is the typed deadline refusal. Need is the smallest need
// among the candidate hops, so Remaining < Need always holds. Need is zero
// when the request's absolute deadline had passed before a hop was chosen;
// Remaining is then negative, by how long.
type RouteDeadlineError struct {
	Handler   string
	Remaining time.Duration
	Need      time.Duration
}

func (e *RouteDeadlineError) Error() string {
	if e.Need <= 0 {
		return fmt.Sprintf("%s: the deadline passed %v before a hop was chosen: %v",
			e.Handler, -e.Remaining, ErrRouteDeadline)
	}
	return fmt.Sprintf("%s: %v left, the next hop needs %v: %v",
		e.Handler, e.Remaining, e.Need, ErrRouteDeadline)
}

func (e *RouteDeadlineError) Unwrap() error { return ErrRouteDeadline }

// ErrAnswerersDisagree is what every AnswerersDisagreeError unwraps to: the nodes
// advertising an operation do not name one requirement for it, so no caller can
// know what it would be judged by. pkg/rpc re-exports it, and the HTTP bridge
// (pkg/rpc/http classifyMeshError) answers it with a 503 it marks not retryable.
var ErrAnswerersDisagree = errors.New("the nodes advertising this operation name different requirements")

// AnswerersDisagreeError is the typed refusal HWPCaller and node's forwarder give
// before sending when ports.JudgeHandlerAdverts finds the adverts of Handler in
// disagreement, or when a call names one node whose advert names no requirement
// beside adverts that do. Requirements holds one "<requirement>: <node>,<node>"
// entry per group, for the log and the response. StampRefusal joins the entries
// with "; " into the failed response a later hop returns, and responseError
// splits that field back into them at the origin, so the origin's refusal names
// the groups the refusing node named; no caller reads an entry to decide
// anything.
type AnswerersDisagreeError struct {
	Handler      string
	Requirements []string
}

func (e *AnswerersDisagreeError) Error() string {
	return fmt.Sprintf("%s: %v: %s", e.Handler, ErrAnswerersDisagree, strings.Join(e.Requirements, "; "))
}

func (e *AnswerersDisagreeError) Unwrap() error { return ErrAnswerersDisagree }

// ErrNoAnswerer is what every NoAnswererError unwraps to: no node this one knows
// of advertises the operation, or the one node a call names does not, so there
// is no node it may send the call to. pkg/rpc re-exports it.
var ErrNoAnswerer = errors.New("no node advertises this operation")

// ErrStarting is what a call unwraps to when the node it reached is still
// starting: its handler registry is held until ready (node/handlers'
// HoldUntilReady), so it ran nothing and forwarded nothing. node's RPCServer
// answers every inbound request with it while held and stamps it on the
// response (StampRefusal); responseError rebuilds it here, and isRouteRefusal
// holds it because nothing ran, so HWPCaller and node's forwarder move to the
// next route and the last error stays typed for the HTTP bridge's 503 with
// Retry-After. It is scope.ErrStarting, this exact value, because
// node/handlers refuses with it and cannot import this package.
var ErrStarting = rpcscope.ErrStarting

// NoAnswererError is the typed refusal of a call before it is sent to a node
// that does not advertise Handler. node's forwarder gives it when the role table
// lists no answerer for Handler and the request names no target, because the
// forwarder sends only to a node whose record lists Handler. node's
// ConnectionManager.CheckAnswerers gives it, with Node set, when a call
// HWPCaller.CallNode or OpenNodeStream would send to Node finds no record of
// Node listing Handler, whatever role the call names. Sent, the request CallNode
// builds names no target node, so RPCServer.handleRequest on Node would forward
// it to another node that lists Handler, against TargetedCaller's contract
// ("Reaching nodeID, or an error. Never another node."), and a stream open would
// be refused by Node's handler registry after the open, untyped; so nothing is
// sent. StampRefusal records it on the failed response a relay returns and
// responseError rebuilds it at the origin, which tries its next route on it
// (Unsent), as it does on the other route refusals; the forwarder's refusal is
// the only one that crosses a hop, and it names no node, so the stamp carries
// the kind alone.
type NoAnswererError struct {
	Handler string
	Node    string // the node a targeted call names; "" for a routed call
}

func (e *NoAnswererError) Error() string {
	if e.Node != "" {
		return fmt.Sprintf("%s: node %s does not advertise this operation", e.Handler, e.Node)
	}
	return fmt.Sprintf("%s: %v", e.Handler, ErrNoAnswerer)
}

func (e *NoAnswererError) Unwrap() error { return ErrNoAnswerer }

// AnswererChecker is the optional SessionFinder method HWPCaller asks before it
// reads the result cache, chooses a route or opens a stream. node's
// ConnectionManager implements it from the requirements and handler lists the
// holders' peer records advertise. nodeID names the one node a targeted call must
// reach, and is "" for a routed call. A finder that does not implement it holds no
// adverts to compare.
type AnswererChecker interface {
	CheckAnswerers(role, handler, nodeID string) error
}

// checkAnswerers returns the finder's refusal of a call to handler, when the
// finder judges adverts at all.
func (c *HWPCaller) checkAnswerers(role, handler, nodeID string) error {
	if ac, ok := c.finder.(AnswererChecker); ok {
		return ac.CheckAnswerers(role, handler, nodeID)
	}
	return nil
}

// classFloor returns the lowest route grade a request for handler may leave
// on, from the "opClass" hint WithOpClass set; a request that names no class
// is Standard. A hint grade.ParseOperationClass does not declare has no floor
// and returns the typed refusal instead, so a misspelt or invented class is
// never held to some other class's floor.
func classFloor(handler, hint string) (grade.Grade, error) {
	class, known := grade.ParseOperationClass(hint)
	if !known {
		return 0, &GradeTooLowError{Handler: handler, Class: hint, UnknownClass: true}
	}
	return class.MinGrade(), nil
}

// CheckRouteGrade is the grade gate for one selected route: it returns nil
// when session grades at or above the floor of the class hint names, and the
// typed refusal otherwise. HWPCaller.callOnce runs it on the session it is
// about to send on, and node's ConnectionManager.CallViaBidi on the session
// whose bidi channel it picked, which HWPCaller never sees. A nil session
// carries nothing and grades F.
func CheckRouteGrade(handler, hint string, session aether.Session) error {
	floor, err := classFloor(handler, hint)
	if err != nil {
		return err
	}
	current := grade.GradeF
	if session != nil {
		current = grade.SessionGrade(session)
	}
	if current < floor {
		return &GradeTooLowError{Handler: handler, Class: hint, Current: current, Required: floor}
	}
	return nil
}

// routesAtOrAbove returns the routes whose first-hop session grades at or
// above floor, and the best grade among all of them for the refusal to
// report. It copies only when it drops a route, because Call hands it the
// slice findRoutesCached memoizes and filtering that in place would shrink
// the cached route set for every later call.
func routesAtOrAbove(routes []aether.ProbeRoute, floor grade.Grade) ([]aether.ProbeRoute, grade.Grade) {
	best := grade.GradeF
	kept := 0
	for _, r := range routes {
		g := routeGrade(r)
		if g > best {
			best = g
		}
		if g >= floor {
			kept++
		}
	}
	if kept == len(routes) {
		return routes, best
	}
	out := make([]aether.ProbeRoute, 0, kept)
	for _, r := range routes {
		if routeGrade(r) >= floor {
			out = append(out, r)
		}
	}
	return out, best
}

// routeGrade is the grade of a route's first-hop session; a route without a
// session has no transport to grade and is F.
func routeGrade(r aether.ProbeRoute) grade.Grade {
	if r.Session == nil {
		return grade.GradeF
	}
	return grade.SessionGrade(r.Session)
}

// isRouteRefusal reports whether err is one of the typed route refusals, which
// Call returns wrapped so the caller can still test them with errors.Is. The
// outbound admission's refusal (ErrNotAdmitted) is one: nothing left the node.
// A starting node's refusal (ErrStarting) is one: the node that gave it ran
// nothing.
func isRouteRefusal(err error) bool {
	return errors.Is(err, ErrGradeTooLow) || errors.Is(err, ErrRouteDeadline) ||
		errors.Is(err, ErrAnswerersDisagree) || errors.Is(err, ErrNoAnswerer) ||
		errors.Is(err, ErrNotAdmitted) || errors.Is(err, ErrStarting)
}

// A refusal made by a later hop reaches the origin as a failed RPCResponse.
// Its Error text alone would arrive as a handler rejection, so the refusing
// node also writes the refusal's kind and fields into the response's Metadata
// (StampRefusal) and HWPCaller rebuilds the typed error from them
// (responseError). The denied kind is written through StampDenial alone, for a
// refusal of the caller's authority: by RPCServer.executeLocal from the refusal
// its registry reports (handlers.RPCResponse.Denied), and by
// RPCServer.handleRequest from its refusal of the principal the caller sent; a
// handler chooses that kind by returning an error wrapping scope.ErrDenied.
// The conflict kind is written through StampConflict alone, for a handler's
// conflict with the state it holds: by RPCServer.executeLocal from an error its
// registry returns or the conflict its response reports
// (handlers.RPCResponse.Conflict); a handler chooses it by returning an error
// wrapping ErrConflict. A handler can set no other key. A kind is a string
// value in the response's Metadata map, which StampRefusal, StampDenial and
// StampConflict write and responseError reads, and no field of the wire
// message, so a kind the reading node does not know reads as a handler
// rejection (responseError's default).
const (
	refusalKindKey       = "refusal"
	refusalKindGrade     = "grade"
	refusalKindDeadline  = "deadline"
	refusalKindAmbiguous = "ambiguous"
	refusalKindAdverts   = "adverts"
	refusalKindNoAnswer  = "no-answerer"
	refusalKindDenied    = "denied"
	refusalKindConflict  = "conflict"
	refusalKindStarting  = "starting"

	refusalClassKey        = "refusal.class"
	refusalUnknownKey      = "refusal.unknown-class"
	refusalCurrentKey      = "refusal.current-grade"
	refusalRequiredKey     = "refusal.required-grade"
	refusalRemainingKey    = "refusal.remaining-ns"
	refusalNeedKey         = "refusal.need-ns"
	refusalRequirementsKey = "refusal.requirements"

	// The status and code of a handler's coded refusal (scope.Coded), which
	// StampCoded writes beside the kind (the denial or conflict the refusal
	// also is, or none) and responseError reads back.
	refusalStatusKey = "refusal.status"
	refusalCodeKey   = "refusal.code"
)

// StampRefusal records err on resp when err is a typed route refusal or an
// ambiguous outcome (ErrAmbiguousEffect), so the node that receives resp can
// return the same typed error to its caller. Any other error leaves resp as it
// is. node's RPCServer calls it on the failed response it builds for a refused
// or ambiguous forward, for a request naming an undeclared class, for a
// request that arrived after its deadline and for every request it answers
// while its registry is held until ready (ErrStarting). An ambiguous outcome
// travels typed because HWPCaller must not send an Unsafe call on another
// route after one; a starting refusal travels typed because it may.
func StampRefusal(resp *pb.RPCResponse, err error) {
	if resp == nil {
		return
	}
	var gte *GradeTooLowError
	var rde *RouteDeadlineError
	var ade *AnswerersDisagreeError
	var nae *NoAnswererError
	switch {
	case errors.Is(err, ErrAmbiguousEffect):
		resp.Metadata = map[string]string{refusalKindKey: refusalKindAmbiguous}
	case errors.Is(err, ErrStarting):
		resp.Metadata = map[string]string{refusalKindKey: refusalKindStarting}
	case errors.As(err, &gte):
		resp.Metadata = map[string]string{
			refusalKindKey:     refusalKindGrade,
			refusalClassKey:    gte.Class,
			refusalUnknownKey:  strconv.FormatBool(gte.UnknownClass),
			refusalCurrentKey:  strconv.Itoa(int(gte.Current)),
			refusalRequiredKey: strconv.Itoa(int(gte.Required)),
		}
	case errors.As(err, &rde):
		resp.Metadata = map[string]string{
			refusalKindKey:      refusalKindDeadline,
			refusalRemainingKey: strconv.FormatInt(int64(rde.Remaining), 10),
			refusalNeedKey:      strconv.FormatInt(int64(rde.Need), 10),
		}
	case errors.As(err, &ade):
		resp.Metadata = map[string]string{
			refusalKindKey:         refusalKindAdverts,
			refusalRequirementsKey: strings.Join(ade.Requirements, "; "),
		}
	case errors.As(err, &nae):
		resp.Metadata = map[string]string{refusalKindKey: refusalKindNoAnswer}
	}
}

// StampDenial records err on resp when err refuses the caller's authority (it
// wraps scope.ErrDenied), so the origin's HWPCaller returns an error that
// errors.Is matches to scope.ErrDenied (responseError) and the HTTP bridge
// proxying the call (pkg/rpc/http classifyMeshError) answers 403, as it
// answers the same refusal made in its own process. Any other error leaves
// resp as it is. RPCServer.executeLocal calls it on every response its
// registry gives, and RPCServer.handleRequest on its refusal of the caller's
// principal; StampRefusal never stamps this kind, so a forwarder's error
// that happens to wrap a denial is still read as the forward's outcome.
func StampDenial(resp *pb.RPCResponse, err error) {
	if resp == nil || resp.Success || !errors.Is(err, rpcscope.ErrDenied) {
		return
	}
	resp.Metadata = map[string]string{refusalKindKey: refusalKindDenied}
}

// StampCoded records the status and code of the handler's coded refusal err
// carries (a scope.Coded), so the origin's HWPCaller returns an error carrying
// the same Coded (responseError) and the HTTP bridge proxying the call answers
// the handler's own status with the code beside its words, as it answers the
// same refusal made in its own process. It writes them beside the kind
// StampDenial or StampConflict stamped, which is still what the reading node
// tests with errors.Is, and leaves resp as it is for a response that succeeded,
// for a route refusal's kind (StampRefusal), which is the hop's outcome and
// not the handler's, and for an error carrying no Coded. RPCServer.executeLocal
// calls it after StampDenial and StampConflict on every failed response its
// registry gives, and StampFailed on a refused stream's.
func StampCoded(resp *pb.RPCResponse, err error) {
	if resp == nil || resp.Success {
		return
	}
	c, ok := rpcscope.CodedOf(err)
	if !ok || c.Status < 400 || c.Status > 499 || c.Code == "" {
		return
	}
	switch resp.Metadata[refusalKindKey] {
	case "", refusalKindDenied, refusalKindConflict:
	default:
		return
	}
	if resp.Metadata == nil {
		resp.Metadata = map[string]string{}
	}
	resp.Metadata[refusalStatusKey] = strconv.Itoa(c.Status)
	resp.Metadata[refusalCodeKey] = c.Code
}

// codedAnswer rebuilds the coded refusal StampCoded recorded on resp around
// answer, the handler's answer responseError read from resp, or returns answer
// unchanged when resp records none or a status outside 4xx. text is resp's
// Error as answer carries it: the rebuilt error reads as that text, the far
// handler's own words (its Coded's code among them once), and unwraps to a
// Coded whose Cause is answer, so errors.As finds the status and code and
// errors.Is what answer unwraps to (scope.ErrDenied or ErrConflict, and
// ErrHandlerError).
func codedAnswer(resp *pb.RPCResponse, answer error, text string) error {
	code := resp.Metadata[refusalCodeKey]
	status, err := strconv.Atoi(resp.Metadata[refusalStatusKey])
	if code == "" || err != nil || status < 400 || status > 499 {
		return answer
	}
	return &codedHop{text: text, coded: &rpcscope.Coded{Status: status, Code: code, Cause: answer}}
}

// codedHop is a coded refusal a handler on another node answered with: its
// text is that node's, and it unwraps to the Coded rebuilt from the response.
type codedHop struct {
	text  string
	coded *rpcscope.Coded
}

func (e *codedHop) Error() string { return e.text }
func (e *codedHop) Unwrap() error { return e.coded }

// deniedAnswer is a refusal of the caller's authority a handler on another node
// answered with. Its text is that node's. It unwraps to scope.ErrDenied, which
// the HTTP bridge answers 403, and to ErrHandlerError because it is the
// handler's answer: when every probe of HWPCaller.Call fails, its last error is
// returned wrapped only if it is an ErrHandlerError or a route refusal, and is
// flattened to text otherwise, which would drop scope.ErrDenied.
type deniedAnswer struct{ text string }

func (e *deniedAnswer) Error() string   { return e.text }
func (e *deniedAnswer) Unwrap() []error { return []error{rpcscope.ErrDenied, ErrHandlerError} }

// ErrConflict is what a handler's conflict with the state it holds unwraps to
// (scope.ErrConflict, this exact value): a stale revision, a duplicate, a limit
// reached or a change already in flight, distinct from scope.ErrDenied. A
// handler returns an error wrapping it, StampConflict carries it on the
// response a node answers over the mesh, and responseError hands HWPCaller's
// caller an error that errors.Is matches to it, which a caller answering HTTP
// maps to 409 where it maps scope.ErrDenied to 403.
var ErrConflict = rpcscope.ErrConflict

// Conflict returns err when it is a handler's conflict (it wraps ErrConflict)
// and not also a refusal of the caller's authority (scope.ErrDenied), and nil
// otherwise: the one error StampConflict stamps. A response carries one kind,
// which responseError reads, and an error wrapping both is a denial, which
// StampDenial stamps, so a caller refused authority over a record is not told
// how the record's stored state stands. It is node/handlers' Conflict, the
// predicate RPCResponse.Conflict is read by, so a Func's error (pkg/rpc's
// registryShim), a middleware's refusal (dispatchRPC) and an error this
// package stamps are judged by one body.
func Conflict(err error) error {
	return handlers.Conflict(err)
}

// StampConflict records err on resp when err is a handler's conflict
// (Conflict), so the origin's HWPCaller returns an error that errors.Is
// matches to ErrConflict (responseError) and not a handler rejection. A
// response that succeeded, or already carries a kind (StampDenial or
// StampRefusal stamped it first), is left as it is, as is one for any other
// error.
func StampConflict(resp *pb.RPCResponse, err error) {
	if resp == nil || resp.Success || Conflict(err) == nil || resp.Metadata[refusalKindKey] != "" {
		return
	}
	resp.Metadata = map[string]string{refusalKindKey: refusalKindConflict}
}

// conflictAnswer is a conflict a handler on another node answered with. Its
// text is that node's. It unwraps to ErrConflict and, for the reason
// deniedAnswer does, to ErrHandlerError: HWPCaller.Call keeps its last probe's
// error wrapped only when it is an ErrHandlerError or a route refusal, and a
// flattened conflict would drop ErrConflict.
type conflictAnswer struct{ text string }

func (e *conflictAnswer) Error() string   { return e.text }
func (e *conflictAnswer) Unwrap() []error { return []error{ErrConflict, ErrHandlerError} }

// hopRefusal is a route refusal another node made. Its text is that node's
// own, which names the hop, and it unwraps to the typed refusal rebuilt from
// the response, so errors.Is and errors.As see what the refusing node saw.
type hopRefusal struct {
	text    string
	refusal error
}

func (e *hopRefusal) Error() string { return e.text }
func (e *hopRefusal) Unwrap() error { return e.refusal }

// responseError is the error for a response whose Success is false: the typed
// refusal StampRefusal recorded on it, the handler's refusal of its caller
// StampDenial recorded (a *deniedAnswer), the handler's conflict StampConflict
// recorded (a *conflictAnswer), or else a handler rejection; each of the last
// three inside the scope.Coded StampCoded recorded beside it, when it recorded
// one. Every place HWPCaller reads a failed response goes through it, so a
// route refusal is never reported as ErrHandlerError, which the HTTP bridge
// answers as a caller's mistake. A field that does not parse reads as zero;
// the kind alone decides the type.
func responseError(handler string, resp *pb.RPCResponse) error {
	var refusal error
	switch resp.Metadata[refusalKindKey] {
	case refusalKindGrade:
		current, _ := strconv.Atoi(resp.Metadata[refusalCurrentKey])
		required, _ := strconv.Atoi(resp.Metadata[refusalRequiredKey])
		unknown, _ := strconv.ParseBool(resp.Metadata[refusalUnknownKey])
		refusal = &GradeTooLowError{
			Handler:      handler,
			Class:        resp.Metadata[refusalClassKey],
			Current:      grade.Grade(current),
			Required:     grade.Grade(required),
			UnknownClass: unknown,
		}
	case refusalKindAmbiguous:
		return &AmbiguousEffectError{Handler: handler, Cause: errors.New(resp.Error)}
	case refusalKindDeadline:
		remaining, _ := strconv.ParseInt(resp.Metadata[refusalRemainingKey], 10, 64)
		need, _ := strconv.ParseInt(resp.Metadata[refusalNeedKey], 10, 64)
		refusal = &RouteDeadlineError{
			Handler:   handler,
			Remaining: time.Duration(remaining),
			Need:      time.Duration(need),
		}
	case refusalKindAdverts:
		refusal = &AnswerersDisagreeError{
			Handler:      handler,
			Requirements: strings.Split(resp.Metadata[refusalRequirementsKey], "; "),
		}
	case refusalKindNoAnswer:
		refusal = &NoAnswererError{Handler: handler}
	case refusalKindStarting:
		refusal = ErrStarting
	case refusalKindDenied:
		text := resp.Error
		if text == "" {
			text = rpcscope.ErrDenied.Error()
		}
		return codedAnswer(resp, &deniedAnswer{text: text}, text)
	case refusalKindConflict:
		text := resp.Error
		if text == "" {
			text = ErrConflict.Error()
		}
		return codedAnswer(resp, &conflictAnswer{text: text}, text)
	default:
		return codedAnswer(resp, NewHandlerError(resp.Error), resp.Error)
	}
	if resp.Error == "" {
		return refusal
	}
	return &hopRefusal{text: resp.Error, refusal: refusal}
}
