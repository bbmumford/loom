/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

package debug

import "testing"

// SetTags(nil) clears the override: TagEnabled and every cached Logger read the DEBUG variable
// again. An empty, non-nil list is still an override, under which no tag is on.
func TestSetTagsNilFallsBackToDEBUG(t *testing.T) {
	const tag = "loom.test.override"
	t.Setenv("DEBUG", tag)
	t.Cleanup(func() { SetTags(nil); SetEnabled(false) })
	SetEnabled(false)
	logger := New(tag)

	SetTags([]string{"loom.test.other"})
	if TagEnabled(tag) || logger.isEnabled() {
		t.Fatalf("with an override naming another tag, %q is on", tag)
	}
	SetTags(nil)
	if !TagEnabled(tag) || !logger.isEnabled() {
		t.Fatalf("after SetTags(nil) with DEBUG=%q: TagEnabled=%v logger=%v, want both true",
			tag, TagEnabled(tag), logger.isEnabled())
	}
	SetTags([]string{})
	if TagEnabled(tag) || logger.isEnabled() {
		t.Fatalf("under an empty override, %q is on", tag)
	}
}
