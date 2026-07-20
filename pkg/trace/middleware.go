/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package trace

import (
	"log"
	"net/http"
)

// Middleware reads the X-E2E-Trace-ID inbound header, attaches the ID
// to the request context, and echoes the same header back on the
// response.
//
// When a trace is present, BEGIN and END log lines are emitted directly
// to the standard logger (NOT the debug logger) so the bracketing
// markers always reach fly logs — they are the entire point of the
// middleware and gating them behind a DEBUG namespace would defeat it.
// Production traffic never includes the header, so the markers stay
// silent off-path.
//
// Composition note: place this BEFORE any middleware that calls into
// the mesh (auth, dispatch), so dispatched RPCs can pull the trace
// from ctx and propagate it through the request envelope.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(HeaderName)
		if id == "" {
			next.ServeHTTP(w, r)
			return
		}
		if len(id) > MaxLen {
			id = id[:MaxLen]
		}

		ctx := WithID(r.Context(), id)
		w.Header().Set(HeaderName, id)

		log.Printf("[trace:%s] BEGIN %s %s", id, r.Method, r.URL.Path)
		defer log.Printf("[trace:%s] END   %s %s", id, r.Method, r.URL.Path)

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
