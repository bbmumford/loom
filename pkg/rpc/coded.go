/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package rpc

import (
	"errors"
	"fmt"
)

// Coded is a handler's refusal that names its caller-facing status and a
// stable wire code: the HTTP bridge (rpc/http.classifyHandlerStatus) answers
// it with Status and the code beside the error text, so a client switches on
// the code and never on the sentence. A handler error that is not Coded and
// matches no other sentinel answers 500 with no detail.
//
// TODO: the mesh hop (dispatch.StampRefusal) carries no Coded, so a refusal a
// handler on another node returns reaches the bridge as text and answers 500;
// it holds for a handler the bridge's own process serves.
type Coded struct {
	Status int
	Code   string
	Cause  error
}

// Refuse wraps cause as a Coded refusal. Status must be a 4xx (a refusal of
// the request, never a server fault) and code non-empty; anything else
// answers cause unchanged, so a mistaken call never turns a fault into a
// client error.
func Refuse(status int, code string, cause error) error {
	if status < 400 || status > 499 || code == "" || cause == nil {
		return cause
	}
	return &Coded{Status: status, Code: code, Cause: cause}
}

func (e *Coded) Error() string { return fmt.Sprintf("%s: %v", e.Code, e.Cause) }

func (e *Coded) Unwrap() error { return e.Cause }

// CodedOf answers the Coded refusal err carries, if any.
func CodedOf(err error) (*Coded, bool) {
	var c *Coded
	if errors.As(err, &c) {
		return c, true
	}
	return nil, false
}
