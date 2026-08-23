/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package ports

import (
	"context"
	"errors"
)

// ActivationEnv is what the runtime hands a RoleActivator. It exists so role
// service code never captures a request-scoped context or spawns bare
// goroutines (the H7 closure-capture hazard): everything long-running binds
// Context() and launches through Go, so Shutdown's wg.Wait drains it and a
// hot role teardown cannot corrupt the next activation.
type ActivationEnv interface {
	// Context is the runtime lifetime context (rt.Context()) — the ONLY
	// context long-running role goroutines may bind.
	Context() context.Context

	// Go runs fn on the runtime's tracked, panic-recovered goroutine pool
	// (rt.Go). name labels it for diagnostics.
	Go(name string, fn func(ctx context.Context))

	// RegisterConfig registers a decrypted config payload under its cfgload
	// name so cfgload.Exists(name) becomes true and capability detection
	// (strict AND over the role's RoleRequirement.Configs) passes.
	RegisterConfig(name string, payload []byte) error

	// RegistrationScope is the opaque capability for registrations causally
	// owned by this exact activation generation. Role code must pass it to the
	// handler registry scoped registration methods; global registration is
	// intentionally not inferred from timing or a declared role name.
	RegistrationScope() RegistrationScope
}

// RegistrationScope is an opaque, possession-based capability issued by the
// handler registry for one exact role activation generation. The token is
// validated against live registry-owned state, so a zero or fabricated scope
// has no authority.
type RegistrationScope struct {
	token any
}

// NewRegistrationScope binds an infrastructure-owned token into an opaque
// capability. Application code should receive scopes from ActivationEnv.
func NewRegistrationScope(token any) RegistrationScope {
	return RegistrationScope{token: token}
}

// Token returns the opaque identity for validation by the issuing registry.
// It conveys no authority unless that registry still recognizes it.
func (s RegistrationScope) Token() any {
	return s.token
}

// RoleActivator is the Phase-1 runtime role-init path — the single largest
// net-new build (plan §1.4): today roles are fixed at boot; Runtime.SetRoles
// only re-advertises the label. Consumers lift their per-role
// roleinit.Initialize<Role> bodies behind this interface and register them
// with the runtime, which invokes Activate AFTER boot when a takeover claim
// is won.
//
// The runtime — not the activator — owns the post-activation sequence, in
// order: handlerRegistry.SetEnabledRoles(+role) → rpcRegistry.
// ExportToHandlerRegistry → PublishRPCHandlersToLAD → PeerPublisher.SetRoles
// re-advertise. ActivationEnv supplies a causal RegistrationScope; successful
// scoped registrations receive immutable registry identities so Deactivate
// can remove precisely that generation without deleting a same-name
// replacement (SetEnabledRoles alone only gates dispatch — it does not
// unregister).
//
// Prototype RoleActivator on "auth" end-to-end before generalizing
// (plan §Risks 6).
type RoleActivator interface {
	// Role is the service role this activator can bring up ("auth", …).
	Role() string

	// Activate starts the role's services. ctx is scoped to this activation
	// attempt (deadline/abort); long-running work must use env.Context()/
	// env.Go, never ctx. Idempotent: activating an already-active role is a
	// no-op returning nil.
	Activate(ctx context.Context, env ActivationEnv) error

	// Deactivate stops the role's services and releases their resources.
	// Called on claim loss, role handback, or shutdown. Must drain within
	// ctx or return an error — never leave orphaned goroutines. An ordinary
	// error has unknown/possibly-partial teardown state and leaves admission
	// closed. Only an error wrapped by
	// RoleStillActiveAfterDeactivationError certifies transactional rollback
	// and permits admission to reopen.
	Deactivate(ctx context.Context) error
}

// RoleDeactivationDisposition describes what remains usable when Deactivate
// returns an error. The zero/unknown disposition is deliberately fail-closed:
// teardown may have released only some resources, so task admission must stay
// closed until a later successful teardown or explicit recovery.
type RoleDeactivationDisposition uint8

const (
	RoleDeactivationUnknown RoleDeactivationDisposition = iota
	// RoleDeactivationStillActive is an activator's transactional guarantee
	// that the failed attempt released nothing and the role remains fully
	// usable. Only this disposition permits the runtime to reopen admission.
	RoleDeactivationStillActive
)

type roleDeactivationError struct {
	err         error
	disposition RoleDeactivationDisposition
}

func (e *roleDeactivationError) Error() string { return e.err.Error() }
func (e *roleDeactivationError) Unwrap() error { return e.err }

// RoleStillActiveAfterDeactivationError marks err with the transactional
// guarantee that a failed Deactivate attempt left all role resources active.
// Activators must not use this wrapper after a partial teardown.
func RoleStillActiveAfterDeactivationError(err error) error {
	if err == nil {
		return nil
	}
	return &roleDeactivationError{
		err:         err,
		disposition: RoleDeactivationStillActive,
	}
}

// RoleDeactivationDispositionOf returns the strongest disposition explicitly
// carried by err. Ordinary errors return RoleDeactivationUnknown.
func RoleDeactivationDispositionOf(err error) RoleDeactivationDisposition {
	var deactivationErr *roleDeactivationError
	if errors.As(err, &deactivationErr) {
		return deactivationErr.disposition
	}
	return RoleDeactivationUnknown
}
