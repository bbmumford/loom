/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

package scope

import (
	"errors"
	"fmt"
)

// Coded is a handler's refusal that names its caller-facing status and a
// stable wire code: the HTTP bridge (rpc/http.classifyHandlerStatus and
// classifyMeshError) answers it with Status and the code beside the error
// text, so a client switches on the code and never on the sentence. A handler
// error that is not Coded and matches no other sentinel answers 500 with no
// detail.
//
// It crosses the mesh: node/handlers' CodedRefusal reads it into
// RPCResponse.Coded, Outcome hands it on, pkg/dispatch's StampCoded writes
// Status and Code on the wire response, and the origin's HWPCaller rebuilds a
// Coded around the far handler's answer (responseError), so a bridge proxying
// the call answers the far handler's status and code. It is declared here,
// beside ErrDenied, for the reason ErrConflict is: node/handlers must read it
// and cannot import pkg/dispatch; pkg/rpc names this exact type as rpc.Coded.
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
