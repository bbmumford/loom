/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
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
// entry per group, for the log and the response; nothing parses it.
type AnswerersDisagreeError struct {
	Handler      string
	Requirements []string
}

func (e *AnswerersDisagreeError) Error() string {
	return fmt.Sprintf("%s: %v: %s", e.Handler, ErrAnswerersDisagree, strings.Join(e.Requirements, "; "))
}

func (e *AnswerersDisagreeError) Unwrap() error { return ErrAnswerersDisagree }

// AnswererChecker is the optional SessionFinder method HWPCaller asks before it
// reads the result cache, chooses a route or opens a stream. node's
// ConnectionManager implements it from the requirements the holders' peer records
// advertise. nodeID names the one node a targeted call must reach, and is "" for a
// routed call. A finder that does not implement it holds no adverts to compare.
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
// Call returns wrapped so the caller can still test them with errors.Is.
func isRouteRefusal(err error) bool {
	return errors.Is(err, ErrGradeTooLow) || errors.Is(err, ErrRouteDeadline) ||
		errors.Is(err, ErrAnswerersDisagree)
}

// A refusal made by a later hop reaches the origin as a failed RPCResponse.
// Its Error text alone would arrive as a handler rejection, so the refusing
// node also writes the refusal's kind and fields into the response's Metadata
// (StampRefusal) and HWPCaller rebuilds the typed error from them
// (responseError). RPCServer.executeLocal builds its response without
// Metadata, so no handler can set these keys.
const (
	refusalKindKey       = "refusal"
	refusalKindGrade     = "grade"
	refusalKindDeadline  = "deadline"
	refusalKindAmbiguous = "ambiguous"
	refusalKindAdverts   = "adverts"

	refusalClassKey        = "refusal.class"
	refusalUnknownKey      = "refusal.unknown-class"
	refusalCurrentKey      = "refusal.current-grade"
	refusalRequiredKey     = "refusal.required-grade"
	refusalRemainingKey    = "refusal.remaining-ns"
	refusalNeedKey         = "refusal.need-ns"
	refusalRequirementsKey = "refusal.requirements"
)

// StampRefusal records err on resp when err is a typed route refusal or an
// ambiguous outcome (ErrAmbiguousEffect), so the node that receives resp can
// return the same typed error to its caller. Any other error leaves resp as it
// is. node's RPCServer calls it on the failed response it builds for a refused
// or ambiguous forward, for a request naming an undeclared class, and for a
// request that arrived after its deadline. An ambiguous outcome travels typed
// because HWPCaller must not send an Unsafe call on another route after one.
func StampRefusal(resp *pb.RPCResponse, err error) {
	if resp == nil {
		return
	}
	var gte *GradeTooLowError
	var rde *RouteDeadlineError
	var ade *AnswerersDisagreeError
	switch {
	case errors.Is(err, ErrAmbiguousEffect):
		resp.Metadata = map[string]string{refusalKindKey: refusalKindAmbiguous}
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
	}
}

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
// refusal StampRefusal recorded on it, or else a handler rejection. Every
// place HWPCaller reads a failed response goes through it, so a refusal is
// never reported as ErrHandlerError, which the HTTP bridge answers as a
// caller's mistake. A field that does not parse reads as zero; the kind alone
// decides the type.
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
	default:
		return NewHandlerError(resp.Error)
	}
	if resp.Error == "" {
		return refusal
	}
	return &hopRefusal{text: resp.Error, refusal: refusal}
}
