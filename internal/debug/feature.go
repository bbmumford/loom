/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package debug

import (
	"log"
	"sync"
)

// Feature is a self-contained debug feature that activates when its tag
// is present in the DEBUG environment variable. Each feature lives in
// its own file in this package (e.g. pprof.go) and registers itself via
// init() so endpoint main.go code can call a single debug.Start() and
// pick up every enabled feature without touching imports for each one.
type Feature struct {
	// Tag is the DEBUG token that activates this feature. Lowercase,
	// no spaces. Example: "pprof". The reserved tag "*" enables every
	// registered feature.
	Tag string

	// Desc is a one-line human-readable description, surfaced in logs
	// when the feature activates so operators can confirm what's running.
	Desc string

	// Start activates the feature. Called at most once per process from
	// debug.Start() when TagEnabled(Tag) is true. Implementations should
	// be non-blocking — long-running work (HTTP servers, samplers) must
	// run in their own goroutine.
	Start func()
}

var (
	featuresMu sync.Mutex
	features   []Feature
	started    bool
)

// Register adds a debug feature to the registry. Called from feature
// files' init() so registration happens before main(). Idempotent on
// the same Tag: a second registration replaces the first, which keeps
// tests that override features predictable.
func Register(f Feature) {
	if f.Tag == "" || f.Start == nil {
		return
	}
	featuresMu.Lock()
	defer featuresMu.Unlock()
	for i, existing := range features {
		if existing.Tag == f.Tag {
			features[i] = f
			return
		}
	}
	features = append(features, f)
}

// Start activates every registered debug feature whose tag is present
// in the DEBUG environment variable (or the wildcard "*"). Idempotent
// — subsequent calls are no-ops. Endpoints call this once from main()
// after build-info logging and before heavy initialization. Adding a
// new debug feature requires zero changes here or in any endpoint;
// just drop a file in this package with an init() that calls
// debug.Register.
//
// Activation rules (see TagEnabled):
//
//	DEBUG=pprof              → pprof only
//	DEBUG=pprof,gossip       → pprof + gossip diagnostics
//	DEBUG=*                  → every registered feature
//	DEBUG=                   → nothing (production default)
func Start() {
	featuresMu.Lock()
	if started {
		featuresMu.Unlock()
		return
	}
	started = true
	// Snapshot the slice under the lock so feature.Start() callbacks
	// can call back into Register without deadlocking.
	snap := append([]Feature(nil), features...)
	featuresMu.Unlock()

	for _, f := range snap {
		if !TagEnabled(f.Tag) {
			continue
		}
		log.Printf("[DEBUG] activating feature %q — %s", f.Tag, f.Desc)
		f.Start()
	}
}

// Features returns a copy of the currently registered feature list for
// diagnostics / "what would DEBUG=* enable?" introspection.
func Features() []Feature {
	featuresMu.Lock()
	defer featuresMu.Unlock()
	return append([]Feature(nil), features...)
}
