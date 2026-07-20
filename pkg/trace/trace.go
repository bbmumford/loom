/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

// Package trace carries a per-request trace ID across the HSTLES platform.
//
// The trace ID is sourced from the inbound HTTP `X-E2E-Trace-ID` header
// (set by the e2e test framework) and lives on the request context so
// downstream code — mesh-RPC dispatch, handlers, log helpers — can pull
// it back out without threading it through every signature.
//
// Outside of e2e tests the ID is empty; this package is a zero-cost
// no-op in production traffic. Tests that paste a trace ID get exact
// correlation across the gateway, mesh dispatch, and target node logs.
//
// Header format: `e2e-<unix-nano>-<6-hex>` (set by the framework's
// `framework/observe/trace` package). The Library treats the value as
// opaque — any string up to 128 chars passes through.
package trace

import (
	"context"
	"net/http"
)

// HeaderName is the inbound HTTP header tests inject.
const HeaderName = "X-E2E-Trace-ID"

// MaxLen caps inbound trace IDs to bound log-line growth and reject
// pathological inputs. The framework's format fits in ~40 chars.
const MaxLen = 128

// ctxKey is unexported to prevent accidental collisions.
type ctxKey struct{}

// FromContext returns the trace ID on ctx, or "" if none.
func FromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	v, _ := ctx.Value(ctxKey{}).(string)
	return v
}

// WithID returns a derived context carrying id. Empty id returns ctx unchanged.
func WithID(ctx context.Context, id string) context.Context {
	if ctx == nil || id == "" {
		return ctx
	}
	if len(id) > MaxLen {
		id = id[:MaxLen]
	}
	return context.WithValue(ctx, ctxKey{}, id)
}

// FromRequest reads the trace ID from the request header, falling back
// to the request context. Empty if neither carries one.
func FromRequest(r *http.Request) string {
	if r == nil {
		return ""
	}
	if id := r.Header.Get(HeaderName); id != "" {
		if len(id) > MaxLen {
			id = id[:MaxLen]
		}
		return id
	}
	return FromContext(r.Context())
}

// Tag returns "[trace:<id>] " when ctx carries a trace ID, "" otherwise.
// Use as a log prefix so grep-on-trace finds every line a request emitted.
//
//	dbg.Printf("%sresolving %s", trace.Tag(ctx), name)
//
// The trailing space makes "Tag()+rest" concatenation read naturally
// regardless of whether a trace is present.
func Tag(ctx context.Context) string {
	id := FromContext(ctx)
	if id == "" {
		return ""
	}
	return "[trace:" + id + "] "
}
