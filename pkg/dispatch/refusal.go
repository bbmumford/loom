/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package dispatch

import (
	"errors"
	"fmt"
	"time"

	"github.com/ORBTR/aether"
	"github.com/bbmumford/loom/grade"
)

// The typed route refusals live here because every dispatcher returns them:
// HWPCaller in this package, and node's forwarder and session finder, which
// import this package. pkg/rpc re-exports the two sentinels and node aliases
// the two types, so callers test either spelling with errors.Is and errors.As.

// ErrGradeTooLow is what every grade refusal unwraps to: the best route on
// offer grades below the floor of the request's operation class, or the
// request names a class grade.ParseOperationClass does not declare.
var ErrGradeTooLow = errors.New("connection grade too low for operation")

// ErrRouteDeadline is what every deadline refusal unwraps to: the caller's
// remaining time does not cover the hop the request would have been sent to,
// so nothing was sent.
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
// among the candidate hops, so Remaining < Need always holds.
type RouteDeadlineError struct {
	Handler   string
	Remaining time.Duration
	Need      time.Duration
}

func (e *RouteDeadlineError) Error() string {
	return fmt.Sprintf("%s: %v left, the next hop needs %v: %v",
		e.Handler, e.Remaining, e.Need, ErrRouteDeadline)
}

func (e *RouteDeadlineError) Unwrap() error { return ErrRouteDeadline }

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
	return errors.Is(err, ErrGradeTooLow) || errors.Is(err, ErrRouteDeadline)
}
