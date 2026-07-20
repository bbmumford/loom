/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package debug

import (
	"os"
	"strings"
	"sync/atomic"
)

// forceAllEnabled is set by SetEnabled(true) / Configure(true, …) so
// the legacy "turn every namespace on at runtime" toggle keeps working
// after the tag-list rewrite. Logically equivalent to setting DEBUG=*.
var forceAllEnabled atomic.Bool

// overrideTags holds a runtime-supplied tag list that takes precedence
// over the DEBUG env var. Set by SetTags; intended for test fixtures
// and operator commands ("flip mesh.lad on without restarting").
var overrideTags atomic.Value // []string

// SetTags overrides the effective DEBUG tag list for the current
// process, ignoring whatever was in os.Getenv("DEBUG") at startup.
// Pass nil to clear the override and fall back to the env var. Active
// namespaces of existing Loggers are also refreshed.
func SetTags(tags []string) {
	if tags == nil {
		overrideTags.Store([]string(nil))
	} else {
		// Copy to avoid sharing the caller's slice.
		c := append([]string(nil), tags...)
		overrideTags.Store(c)
	}
	refreshLoggers()
}

// envVar names the single shared activation env var. The Library uses
// one env (DEBUG) with a tag list instead of N dedicated DEBUG_xxx
// variables — operators flip features on by listing tags.
const envVar = "DEBUG"

// wildcardTag enables every registered feature.
const wildcardTag = "*"

// TagEnabled reports whether the DEBUG env var (or a runtime override
// set via SetEnabled/Configure) activates the supplied tag.
//
// Matching rules — in order, first match wins:
//  1. DEBUG list contains "*"                  → all tags enabled
//  2. DEBUG list contains exactly `tag`         → enabled
//  3. DEBUG list contains a prefix-wildcard
//     entry "x.*" where `tag` is "x.…"          → enabled
//     (e.g. DEBUG=mesh.* enables mesh.vl1, mesh.lad, mesh.rpc, …)
//  4. Otherwise                                 → disabled
//
// Tags are separated by any combination of commas and whitespace:
//
//	DEBUG="pprof, mesh.* support"  → ["pprof", "mesh.*", "support"]
//
// Matching is case-sensitive. Substrings like "pprof_extra" do NOT
// enable "pprof"; only exact or prefix-wildcard matches count.
//
// SetEnabled(true) makes TagEnabled return true for everything; useful
// for tests and the legacy "global on" toggle.
func TagEnabled(tag string) bool {
	if tag == "" {
		return false
	}
	if forceAllEnabled.Load() {
		return true
	}
	for _, t := range parsedTags() {
		if t == wildcardTag {
			return true
		}
		if t == tag {
			return true
		}
		// Prefix wildcard: "mesh.*" matches "mesh.vl1", "mesh.lad", etc.
		if strings.HasSuffix(t, ".*") {
			prefix := strings.TrimSuffix(t, "*")
			if strings.HasPrefix(tag, prefix) {
				return true
			}
		}
	}
	return false
}

// parsedTags returns the current effective DEBUG tag list. If
// SetEnabled / Configure overrode the environment at runtime, that
// override takes precedence; otherwise the value reflects the DEBUG
// env var at process start.
func parsedTags() []string {
	if v := overrideTags.Load(); v != nil {
		return v.([]string)
	}
	return splitTags(os.Getenv(envVar))
}

// splitTags parses a raw DEBUG-style value into individual tags.
func splitTags(raw string) []string {
	if raw == "" {
		return nil
	}
	return strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t' || r == '\n'
	})
}
