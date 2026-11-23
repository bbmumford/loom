/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */
package rpc

import (
	"errors"
	"net/http"

	"github.com/bbmumford/loom/pkg/dispatch"
)

// HandlerStatus is the HTTP status a handler's error answers a unary call with.
// A handler stream's failure that crossed the mesh (dispatch.StreamFailureOf)
// answers the status its far side classified it with; a Coded refusal its own
// Status; known sentinels (ErrScopeDenied, ErrHandlerNotFound, webhook
// signature failures) their specific codes; anything else 500. A call that
// reached a node still starting (ErrStarting, from a handler's own downstream
// call) is 503. The HTTP bridge (rpc/http.classifyHandlerStatus) answers with
// it, and node's handler-stream end frame carries it, so a failure reports one
// status over HTTP and over the mesh.
//
// It does not sanitize the message: the HTTP bridge's writer decides whether a
// cause is shown or replaced by the status text.
func HandlerStatus(err error) int {
	if f, ok := dispatch.StreamFailureOf(err); ok && f.Status != 0 {
		return f.Status
	}
	if c, ok := CodedOf(err); ok {
		return c.Status
	}
	switch {
	case errors.Is(err, ErrStarting):
		return http.StatusServiceUnavailable
	case errors.Is(err, ErrScopeDenied):
		// A refusal its recorder could not record (joined with
		// ErrRefusalUnrecorded) is answered as the refusal it is.
		return http.StatusForbidden
	case errors.Is(err, ErrConflict):
		// After the denial: an error wrapping both refuses the caller.
		return http.StatusConflict
	case errors.Is(err, ErrHandlerNotFound):
		return http.StatusNotFound
	case errors.Is(err, ErrProxyDispatchLocal):
		return http.StatusBadGateway
	case errors.Is(err, ErrWebhookSignatureInvalid),
		errors.Is(err, ErrWebhookSecretUnset):
		// Webhook signature failure (bad/missing/expired signature) and
		// unset signing secret both map to 401 Unauthorized — the request
		// did not present credentials this gateway can accept. Distinct
		// from 403 (scope denied) so dashboards and rate-limiters can
		// alarm on each cause separately.
		return http.StatusUnauthorized
	}
	return http.StatusInternalServerError
}

// StreamFailureOf is what a handler stream reports for err once its first frame
// has gone: Error and Code as the HTTP bridge's body for a unary call failing
// with err (the error's text for a 4xx, its status text otherwise; a Coded
// refusal's wire code), Status as HandlerStatus, Kind as dispatch.RefusalKind.
// A failure that crossed the mesh reports its far side's fields unchanged. The
// HTTP stream's error event (rpc/http) writes these fields and node's
// handler-stream end frame carries them, so a stream's failure reads the same
// either way.
func StreamFailureOf(err error) dispatch.StreamFailure {
	if f, ok := dispatch.StreamFailureOf(err); ok {
		return f
	}
	status := HandlerStatus(err)
	f := dispatch.StreamFailure{Error: http.StatusText(status), Status: status, Kind: dispatch.RefusalKind(err)}
	if status < 500 {
		f.Error = err.Error()
		if c, ok := CodedOf(err); ok {
			f.Code = c.Code
		}
	}
	return f
}
