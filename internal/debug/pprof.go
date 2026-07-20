//go:build !js

/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package debug

import (
	"log"
	"net/http"

	// Import for side-effect: registers /debug/pprof/* on http.DefaultServeMux.
	// We never serve DefaultServeMux on a publicly-reachable address — the
	// pprof goroutine below binds to a Fly-internal port (:6060) that's
	// deliberately omitted from each endpoint's fly.toml [[services]] map,
	// so Fly's edge proxy never routes to it. Access is via
	// `flyctl proxy 6060:6060 -a <app>` from a developer machine, which
	// rides the Fly wireguard mesh.
	_ "net/http/pprof"
)

// pprofAddr is the bind address for the pprof HTTP server. :6060 is the
// Go community convention. Bound to all interfaces so flyctl proxy /
// the Fly wireguard mesh can reach it; never declared in fly.toml
// services, so the public Fly edge proxy ignores it.
const pprofAddr = ":6060"

func init() {
	Register(Feature{
		Tag:   "pprof",
		Desc:  "Go pprof HTTP server on " + pprofAddr + " (Fly-internal; access via `flyctl proxy 6060:6060 -a <app>`)",
		Start: startPprof,
	})
}

// startPprof launches the standard Go pprof HTTP server on a private
// port. Wired through the Feature registry — invoked by debug.Start()
// when the DEBUG env var contains "pprof" or "*". Idempotent because
// net/http/pprof's init registers handlers on http.DefaultServeMux
// exactly once; calling debug.Start() twice would call ListenAndServe
// twice, but the second call returns "address already in use" and
// exits cleanly with the existing server still up.
//
// What pprof exposes (when enabled):
//   - /debug/pprof/goroutine?debug=2 — full goroutine dump
//   - /debug/pprof/heap            — heap allocation profile
//   - /debug/pprof/profile         — 30 s CPU profile
//   - /debug/pprof/mutex           — contended mutexes
//   - /debug/pprof/threadcreate    — OS thread creation profile
//   - /debug/pprof/cmdline         — process argv (build args land here;
//     treat as semi-sensitive)
//
// Usage from a developer machine when an endpoint is wedged:
//
//	flyctl proxy 6060:6060 -a bootstrap-hstles-com
//	curl http://localhost:6060/debug/pprof/goroutine?debug=2 > goroutines.txt
//
// The proxy only opens for the duration of the flyctl process, so the
// endpoint isn't exposed even within the Fly org while pprof is enabled.
func startPprof() {
	go func() {
		log.Printf("[PPROF] starting on %s (Fly-internal only — flyctl proxy %s:6060 to access)",
			pprofAddr, pprofAddr[1:])
		// Pass http.DefaultServeMux explicitly so it's clear the registered
		// handlers are the pprof package's, not anything from the rest of
		// the program.
		if err := http.ListenAndServe(pprofAddr, http.DefaultServeMux); err != nil {
			log.Printf("[PPROF] server exited: %v", err)
		}
	}()
}
