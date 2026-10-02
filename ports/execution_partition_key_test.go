/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package ports

import "testing"

func TestExecutionPartitionKeyIsOpaqueStableAndNonzero(t *testing.T) {
	first, err := NewExecutionPartitionKey("realm=hstles|tenant=orbtr|org=acme|kind=service|id=worker-7|space=ops")
	if err != nil {
		t.Fatalf("NewExecutionPartitionKey(first): %v", err)
	}
	same, err := NewExecutionPartitionKey("realm=hstles|tenant=orbtr|org=acme|kind=service|id=worker-7|space=ops")
	if err != nil {
		t.Fatalf("NewExecutionPartitionKey(same): %v", err)
	}
	other, err := NewExecutionPartitionKey("realm=hstles|tenant=orbtr|org=other|kind=service|id=worker-7|space=ops")
	if err != nil {
		t.Fatalf("NewExecutionPartitionKey(other): %v", err)
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
	if _, err := NewExecutionPartitionKey(" \t "); err == nil {
		t.Fatal("blank canonical owner was accepted")
	}
	if (ExecutionPartitionKey{}).Valid() || (ExecutionPartitionKey{}).Fingerprint() != "" {
		t.Fatal("zero owner key is valid")
	}
}
