/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package ports

import "testing"

func TestExecutionPartitionKeyIsOpaqueStableAndNonzero(t *testing.T) {
	first, err := NewExecutionPartitionKey("realm=example|partition=p-1|group=acme|kind=service|id=worker-7|space=ops")
	if err != nil {
		t.Fatalf("NewExecutionPartitionKey(first): %v", err)
	}
	same, err := NewExecutionPartitionKey("realm=example|partition=p-1|group=acme|kind=service|id=worker-7|space=ops")
	if err != nil {
		t.Fatalf("NewExecutionPartitionKey(same): %v", err)
	}
	other, err := NewExecutionPartitionKey("realm=example|partition=p-1|group=other|kind=service|id=worker-7|space=ops")
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
		t.Fatal("a different group collapsed to the same owner key")
	}
	if _, err := NewExecutionPartitionKey(" \t "); err == nil {
		t.Fatal("blank canonical owner was accepted")
	}
	if (ExecutionPartitionKey{}).Valid() || (ExecutionPartitionKey{}).Fingerprint() != "" {
		t.Fatal("zero owner key is valid")
	}
}
