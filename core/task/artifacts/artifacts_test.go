/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package artifacts

import (
	"encoding/json"
	"testing"

	storage "github.com/bbmumford/loom/node/storage"
)

func TestManifestRefJSON(t *testing.T) {
	id := storage.ManifestID(storage.Digest([]byte("manifest")))
	ref := NewManifestRef(id)

	raw, err := json.Marshal(ref)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var encoded string
	if err := json.Unmarshal(raw, &encoded); err != nil {
		t.Fatalf("Unmarshal string: %v", err)
	}

	if encoded != storage.Hash(id).Hex() {
		t.Fatalf("expected hex %s, got %s", storage.Hash(id).Hex(), encoded)
	}

	var parsed ManifestRef
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatalf("Unmarshal ref: %v", err)
	}

	if parsed.Hex() != ref.Hex() {
		t.Fatalf("round-trip mismatch: got %s want %s", parsed.Hex(), ref.Hex())
	}
}

// Note: TestCloneForRetryArtifacts was removed to avoid import cycle.
// The Task.CloneForRetry() method should be tested in the parent task package
// where it can properly test the full Task type including artifacts behavior.
