/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

// Package slog is a subsystem-scoped structured-log adapter over log/slog. It
// exists so authorization decisions, mesh state transitions, and audit events
// emit machine-parsable key/value records under one fixed field vocabulary, so a
// log aggregator can correlate events across subsystems by key.
//
// It interprets no scope: subsystem/reason/actor/decision are opaque strings the
// caller supplies. It auto-injects the per-request trace ID from
// loom/pkg/trace.FromContext (the shared ctx key), so a handler threads no trace
// plumbing by hand; when no trace is set the attribute is dropped.
//
// Field vocabulary (canonical keys — do NOT invent new names):
//
//	subsystem  — dotted package path, e.g. "security.middleware.auth"
//	reason     — snake_case machine token, e.g. "scope_denied"
//	actor      — the principal being judged (opaque id)
//	peer       — mesh peer node id (mesh subsystems)
//	decision   — "allow" | "deny" | "challenge"
//	err        — the underlying error (nil-safe)
//	trace      — per-request trace id (auto-injected from ctx)
//
// The security helpers (SecurityDeny/SecurityAudit/SecurityChallenge) force the
// (subsystem, reason, decision) triple dashboards alert on; every fail-closed
// branch calls SecurityDeny so a deny is visible even when the caller does not
// surface it. This package installs no slog handler — each endpoint main
// installs a JSON handler at boot and these helpers read whatever is installed.
package slog

import (
	"context"
	"log/slog"

	"github.com/bbmumford/loom/pkg/trace"
)

// Canonical field keys. Call sites SHOULD use these so a dashboard search for
// "reason=scope_denied" hits every emitter.
const (
	KeySubsystem = "subsystem"
	KeyReason    = "reason"
	KeyActor     = "actor"
	KeyPeer      = "peer"
	KeyDecision  = "decision"
	KeyErr       = "err"
	KeyTrace     = "trace"
)

// Canonical decision values.
const (
	DecisionAllow     = "allow"
	DecisionDeny      = "deny"
	DecisionChallenge = "challenge"
)

// Logger is a subsystem-scoped structured logger. It carries the subsystem name
// so every record includes it without the call site repeating it. Zero value is
// NOT usable — use New.
type Logger struct {
	subsystem string
}

// New returns a Logger bound to subsystem. Use the dotted package path (e.g.
// "security.middleware.auth") so aggregation by prefix works.
func New(subsystem string) *Logger {
	return &Logger{subsystem: subsystem}
}

// Subsystem returns the bound subsystem string.
func (l *Logger) Subsystem() string {
	if l == nil {
		return ""
	}
	return l.subsystem
}

// attrs prepends the subsystem attribute and (if present) the trace attribute
// onto caller-supplied attrs. Nil-safe on ctx.
func (l *Logger) attrs(ctx context.Context, extra []any) []any {
	sub := ""
	if l != nil {
		sub = l.subsystem
	}
	out := make([]any, 0, 2+len(extra))
	if sub != "" {
		out = append(out, slog.String(KeySubsystem, sub))
	}
	if id := trace.FromContext(ctx); id != "" {
		out = append(out, slog.String(KeyTrace, id))
	}
	out = append(out, extra...)
	return out
}

// Info emits an informational record. Use for non-security state transitions
// (mesh promotion, cache warmup, etc.); prefer the security helpers on the
// authorization path.
func (l *Logger) Info(ctx context.Context, msg string, attrs ...any) {
	slog.LogAttrs(ctx, slog.LevelInfo, msg, toAttrSlice(l.attrs(ctx, attrs))...)
}

// Warn emits a warning record (recoverable degradation).
func (l *Logger) Warn(ctx context.Context, msg string, attrs ...any) {
	slog.LogAttrs(ctx, slog.LevelWarn, msg, toAttrSlice(l.attrs(ctx, attrs))...)
}

// Error emits an error record for an unrecoverable non-security failure.
// Security paths use SecurityDeny so dashboards correlate on decision=deny.
func (l *Logger) Error(ctx context.Context, msg string, err error, attrs ...any) {
	all := l.attrs(ctx, attrs)
	if err != nil {
		all = append(all, slog.Any(KeyErr, err))
	}
	slog.LogAttrs(ctx, slog.LevelError, msg, toAttrSlice(all)...)
}

// SecurityDeny emits the canonical security-deny event: the mandatory entry
// point for every fail-closed authorization/authentication branch. The
// (subsystem, reason, decision=deny) triple is what alerting keys off. err is
// optional: nil for policy denies, the underlying error for infra-driven denies.
func (l *Logger) SecurityDeny(ctx context.Context, msg string, reason string, actor string, err error, attrs ...any) {
	extras := []any{
		slog.String(KeyReason, reason),
		slog.String(KeyDecision, DecisionDeny),
	}
	if actor != "" {
		extras = append(extras, slog.String(KeyActor, actor))
	}
	if err != nil {
		extras = append(extras, slog.Any(KeyErr, err))
	}
	extras = append(extras, attrs...)
	slog.LogAttrs(ctx, slog.LevelError, msg, toAttrSlice(l.attrs(ctx, extras))...)
}

// SecurityAudit emits a canonical audit-grade record (successful handoff, session
// mint, MFA verify) at Info with decision=allow, so one decision facet spans
// allow and deny.
func (l *Logger) SecurityAudit(ctx context.Context, msg string, reason string, actor string, attrs ...any) {
	extras := []any{
		slog.String(KeyReason, reason),
		slog.String(KeyDecision, DecisionAllow),
	}
	if actor != "" {
		extras = append(extras, slog.String(KeyActor, actor))
	}
	extras = append(extras, attrs...)
	slog.LogAttrs(ctx, slog.LevelInfo, msg, toAttrSlice(l.attrs(ctx, extras))...)
}

// SecurityChallenge emits the canonical challenge record (2FA prompt, MFA
// redirect) at Info with decision=challenge, separating "escalated to user"
// from allow/deny.
func (l *Logger) SecurityChallenge(ctx context.Context, msg string, reason string, actor string, attrs ...any) {
	extras := []any{
		slog.String(KeyReason, reason),
		slog.String(KeyDecision, DecisionChallenge),
	}
	if actor != "" {
		extras = append(extras, slog.String(KeyActor, actor))
	}
	extras = append(extras, attrs...)
	slog.LogAttrs(ctx, slog.LevelInfo, msg, toAttrSlice(l.attrs(ctx, extras))...)
}

// toAttrSlice normalises variadic attrs to []slog.Attr so LogAttrs never
// receives a bare (key, value) pair. A leftover string is treated as a key and
// paired with the next value; anything else falls back to slog.Any.
func toAttrSlice(in []any) []slog.Attr {
	out := make([]slog.Attr, 0, len(in))
	i := 0
	for i < len(in) {
		v := in[i]
		if a, ok := v.(slog.Attr); ok {
			out = append(out, a)
			i++
			continue
		}
		if key, ok := v.(string); ok && i+1 < len(in) {
			out = append(out, slog.Any(key, in[i+1]))
			i += 2
			continue
		}
		out = append(out, slog.Any("value", v))
		i++
	}
	return out
}
