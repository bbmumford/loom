/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package scope

import (
	"context"
	"errors"
	"fmt"
	"reflect"
)

// Refusal is one call loom's check point refused for want of a permission: the
// operation, the caller the server established for the call, and the permission
// the operation asks, which is its Requirement when it declares one and its
// scope tag otherwise (Requirement is then nil).
type Refusal struct {
	Operation   string
	Caller      AuthenticatedIdentity
	Requirement *Requirement
	Scope       ScopeTag
}

// RefusalRecorder records one refusal before the check point returns it. The
// server registers it on each registry that judges calls (pkg/rpc Registry for
// the HTTP bridge and in-process Dispatch, node/handlers HandlerRegistry for the
// mesh), and loom reads nothing of what it does. ctx is the refused call's own
// context. A non-nil error leaves the refusal standing, joined with
// ErrRefusalUnrecorded, so the caller is answered as refused and the server
// that registered the recorder counts and alerts the record it could not make.
type RefusalRecorder interface {
	RecordRefusal(ctx context.Context, refusal Refusal) error
}

// ErrRefusalUnrecorded is joined to a refusal its recorder could not record.
// The joined error still unwraps to the refusal's ErrDenied, so the HTTP bridge
// answers it 403 and the mesh answers it as a denial: a failed record never
// changes what the refused caller is told.
var ErrRefusalUnrecorded = errors.New("rpc: the refusal could not be recorded")

// RecordRefusal is the check point's one call to rec for denied, the refusal it
// is about to return. It returns denied as is when denied does not wrap
// ErrDenied or rec is nil (a registry no server registered a recorder on), and
// when rec records it. Otherwise it returns denied joined with
// ErrRefusalUnrecorded and rec's error as text: the result unwraps to both
// sentinels and to nothing of rec's error, so a sentinel rec's error carries
// cannot change the answer.
func RecordRefusal(ctx context.Context, rec RefusalRecorder, refusal Refusal, denied error) error {
	if !errors.Is(denied, ErrDenied) || rec == nil {
		return denied
	}
	if v := reflect.ValueOf(rec); v.Kind() == reflect.Pointer && v.IsNil() {
		return fmt.Errorf("%w (%w: nil recorder)", denied, ErrRefusalUnrecorded)
	}
	if err := rec.RecordRefusal(ctx, refusal); err != nil {
		return fmt.Errorf("%w (%w: %s)", denied, ErrRefusalUnrecorded, err)
	}
	return denied
}
