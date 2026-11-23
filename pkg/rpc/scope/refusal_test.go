/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

package scope

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

type oneRefusal struct {
	calls int
	fail  error
}

func (o *oneRefusal) RecordRefusal(context.Context, Refusal) error {
	o.calls++
	return o.fail
}

// RecordRefusal calls the recorder only for a denial, returns the denial when
// it records or when no recorder is registered, and otherwise returns the
// denial joined with ErrRefusalUnrecorded, from which the recorder's error does
// not unwrap.
func TestRecordRefusal(t *testing.T) {
	ctx := context.Background()
	denied := fmt.Errorf("%w: handler a.b.C", ErrDenied)

	if got := RecordRefusal(ctx, nil, Refusal{}, denied); got != denied {
		t.Fatalf("nil recorder: %v, want the denial", got)
	}
	rec := &oneRefusal{}
	other := errors.New("not a refusal")
	if got := RecordRefusal(ctx, rec, Refusal{}, other); got != other || rec.calls != 0 {
		t.Fatalf("non-denial: %v (calls %d), want it as is and no record", got, rec.calls)
	}
	if got := RecordRefusal(ctx, rec, Refusal{Operation: "a.b.C"}, denied); got != denied || rec.calls != 1 {
		t.Fatalf("recorded: %v (calls %d), want the denial after one record", got, rec.calls)
	}

	sentinel := errors.New("chain closed")
	rec.fail = fmt.Errorf("%w and %w", sentinel, ErrDenied)
	got := RecordRefusal(ctx, rec, Refusal{}, denied)
	if !errors.Is(got, ErrRefusalUnrecorded) || !errors.Is(got, ErrDenied) || errors.Is(got, sentinel) {
		t.Fatalf("unrecorded: %v; want the denial and ErrRefusalUnrecorded, not the recorder's error", got)
	}

	var typedNil *oneRefusal
	if got := RecordRefusal(ctx, typedNil, Refusal{}, denied); !errors.Is(got, ErrRefusalUnrecorded) || !errors.Is(got, ErrDenied) {
		t.Fatalf("typed-nil recorder: %v, want the denial and ErrRefusalUnrecorded", got)
	}
}
