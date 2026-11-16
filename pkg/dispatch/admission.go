/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package dispatch

import (
	"context"
	"errors"
	"fmt"

	"github.com/bbmumford/loom/grade"
)

// Admission is the process's own check of a call before it leaves over the mesh.
// role and method name the operation the call reaches (method is the full name
// Caller.Call takes), payload is the request's bytes as they are sent, and ctx is the
// sender's context. A non-nil answer refuses the call: nothing is sent, and the send
// entry returns the answer wrapped in ErrNotAdmitted. This package does not read what
// the function decides on: the endpoint spine installs the one its Spec declares
// (library endpoint Run), and the endpoint states its own policy in the body.
type Admission func(ctx context.Context, role, method string, payload []byte) error

// ErrNotAdmitted is what every refusal of the installed Admission unwraps to, beside
// the function's own error. Unsent reports it, because nothing left this node; pkg/rpc
// re-exports it and the HTTP bridge answers it 403, not retryable.
var ErrNotAdmitted = errors.New("the process's outbound admission refused the call")

// ErrAdmissionInstalled refuses a second InstallAdmission: the process has one
// outbound admission, and replacing it would let a later caller of InstallAdmission
// widen what the spine installed.
var ErrAdmissionInstalled = errors.New("dispatch: an outbound admission is already installed")

// ErrCallerSealed refuses a SetCaller that would put a caller in force which does not
// run the installed Admission, or replace the caller the spine set after it. Every
// GetCaller reader (pkg/rpc's Call, CallRaw, CallNode, CallNodeRaw and OpenNodeStream)
// sends through whatever caller is in force, so a replacement would carry those calls
// past the admission.
var ErrCallerSealed = errors.New("dispatch: the process's caller is sealed by its outbound admission")

// admission is the installed Admission, read under globalMu with the caller it seals.
var admission Admission

// InstallAdmission makes a the process's outbound admission. It is refused for a nil
// function and when one is already installed; from then on every send entry of this
// package (HWPCaller's Call, CallCacheable, CallNode and OpenNodeStream, and
// OpenHandlerStream) and node's ConnectionManager.CallViaBidi ask a before anything is
// sent, and SetCaller accepts one *HWPCaller and no replacement (ErrCallerSealed).
func InstallAdmission(a Admission) error {
	if a == nil {
		return errors.New("dispatch: InstallAdmission needs a function")
	}
	globalMu.Lock()
	defer globalMu.Unlock()
	if admission != nil {
		return ErrAdmissionInstalled
	}
	admission = a
	return nil
}

// preSend is the one check HWPCaller's send entries (Call, CallCacheable, CallNode and
// OpenNodeStream) make before they read the result cache, choose a route or open a
// stream: the process's admission of the call (Admit), the grade floor its class names
// (classFloor), and the adverts of the operation (checkAnswerers, nodeID naming the one
// node a targeted call or stream must reach, "" for a routed call). It returns the
// floor, which Call filters its routes by. The admission runs first so a call the
// process's own policy refuses is refused whatever the mesh offers; every refusal is
// returned before anything is sent, typed so Unsent reports it.
func (c *HWPCaller) preSend(ctx context.Context, role, method, nodeID string, payload []byte) (grade.Grade, error) {
	if err := Admit(ctx, role, method, payload); err != nil {
		return 0, err
	}
	floor, err := classFloor(method, opClassFromContext(ctx))
	if err != nil {
		return 0, err
	}
	if err := c.checkAnswerers(role, method, nodeID); err != nil {
		return 0, err
	}
	return floor, nil
}

// refusedBeforeSend reports whether err is a refusal the finder's CallViaBidi made of
// a request it never sent (the grade gate, or the admission node's ConnectionManager
// runs for a caller outside this package): the route's circuit gets no verdict and no
// other path is tried for it.
func refusedBeforeSend(err error) bool {
	return errors.Is(err, ErrGradeTooLow) || errors.Is(err, ErrNotAdmitted)
}

// Admit asks the installed Admission about one call and returns its refusal wrapped in
// ErrNotAdmitted; with none installed every call is admitted, as it was before an
// endpoint declared one. The send entries named at InstallAdmission call it, and a
// sender that writes requests onto a session itself calls it first with the same
// role, method and payload.
func Admit(ctx context.Context, role, method string, payload []byte) error {
	globalMu.RLock()
	a := admission
	globalMu.RUnlock()
	if a == nil {
		return nil
	}
	if err := a(ctx, role, method, payload); err != nil {
		return fmt.Errorf("%w: %w", ErrNotAdmitted, err)
	}
	return nil
}
