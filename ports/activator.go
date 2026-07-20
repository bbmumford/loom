/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package ports

import "context"

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
// re-advertise. The compose scope tracker captures exactly the function IDs
// registered during Activate so Deactivate can unregister precisely that
// role's handlers (SetEnabledRoles alone only gates dispatch — it does not
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
	// ctx or return an error — never leave orphaned goroutines.
	Deactivate(ctx context.Context) error
}
