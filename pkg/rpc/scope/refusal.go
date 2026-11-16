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
// context. A non-nil error fails the call with ErrRefusalUnrecorded in place of
// the refusal, so no caller is refused without the record.
type RefusalRecorder interface {
	RecordRefusal(ctx context.Context, refusal Refusal) error
}

// ErrRefusalUnrecorded is what a call fails with when its refusal could not be
// recorded. It does not unwrap to ErrDenied: the HTTP bridge answers it 503 and
// the mesh answers it as a failed call, not a denial, so the caller can tell a
// refusal that stands on the record from one that does not.
var ErrRefusalUnrecorded = errors.New("rpc: the refusal could not be recorded")

// RecordRefusal is the check point's one call to rec for denied, the refusal it
// is about to return. It returns denied as is when denied does not wrap
// ErrDenied or rec is nil (a registry no server registered a recorder on), and
// when rec records it. Otherwise it returns ErrRefusalUnrecorded carrying the
// refusal's text and rec's error; neither unwraps from it, so a sentinel in
// either cannot turn the failure back into a denial.
func RecordRefusal(ctx context.Context, rec RefusalRecorder, refusal Refusal, denied error) error {
	if !errors.Is(denied, ErrDenied) || rec == nil {
		return denied
	}
	if v := reflect.ValueOf(rec); v.Kind() == reflect.Pointer && v.IsNil() {
		return fmt.Errorf("%w: %s: nil recorder", ErrRefusalUnrecorded, denied)
	}
	if err := rec.RecordRefusal(ctx, refusal); err != nil {
		return fmt.Errorf("%w: %s: %s", ErrRefusalUnrecorded, denied, err)
	}
	return denied
}
