/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package ports

import "testing"

func TestExecutionOwnerKeyIsOpaqueStableAndNonzero(t *testing.T) {
	first, err := NewExecutionOwnerKey("realm=hstles|tenant=orbtr|org=acme|kind=service|id=worker-7|space=ops")
	if err != nil {
		t.Fatalf("NewExecutionOwnerKey(first): %v", err)
	}
	same, err := NewExecutionOwnerKey("realm=hstles|tenant=orbtr|org=acme|kind=service|id=worker-7|space=ops")
	if err != nil {
		t.Fatalf("NewExecutionOwnerKey(same): %v", err)
	}
	other, err := NewExecutionOwnerKey("realm=hstles|tenant=orbtr|org=other|kind=service|id=worker-7|space=ops")
	if err != nil {
		t.Fatalf("NewExecutionOwnerKey(other): %v", err)
	}

	if !first.Valid() || first.Fingerprint() == "" {
		t.Fatal("constructed owner key is invalid")
	}
	if first != same {
		t.Fatal("same canonical owner produced different keys")
	}
	if first == other {
		t.Fatal("different organisation collapsed to the same owner key")
	}
	if _, err := NewExecutionOwnerKey(" \t "); err == nil {
		t.Fatal("blank canonical owner was accepted")
	}
	if (ExecutionOwnerKey{}).Valid() || (ExecutionOwnerKey{}).Fingerprint() != "" {
		t.Fatal("zero owner key is valid")
	}
}
