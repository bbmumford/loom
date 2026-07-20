/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/bbmumford/cfgload"

	"github.com/bbmumford/loom/compose"
	"github.com/bbmumford/loom/ports"
	"github.com/bbmumford/loom/secrets"
)

// roleActivationManager is the Phase-1 runtime role-init path (plan §1.4):
// today roles are fixed at boot and SetRoles only re-advertises the label;
// this manager can bring a role's services up (and down) AFTER boot, on a
// won takeover claim or an operator action.
//
// Activation sequence (the ordering is the contract — see ActivateRole):
// configs → Activate → SetEnabledRoles(+role) → PublishRPCHandlersToLAD →
// SetRoles re-advertise. Teardown reverses it, unregistering EXACTLY the
// handler names the activation registered (captured via the compose scope
// tracker hooked into the handler registry's registration observer).
type roleActivationManager struct {
	rt *Runtime

	// mu serializes activations/deactivations. Role transitions are rare;
	// serializing keeps the registration-observer attribution trivially
	// correct (one owner at a time) and makes the ordered side effects
	// atomic with respect to each other.
	mu         sync.Mutex
	activators map[string]ports.RoleActivator
	active     map[string]bool
	tracker    *compose.Tracker
}

func newRoleActivationManager(rt *Runtime) *roleActivationManager {
	return &roleActivationManager{
		rt:         rt,
		activators: map[string]ports.RoleActivator{},
		active:     map[string]bool{},
		tracker:    compose.NewTracker(),
	}
}

// activationEnv implements ports.ActivationEnv over the runtime: role
// goroutines bind rt.Context() through rt.Go (wg-tracked, panic-recovered)
// so Shutdown drains them — the H7 closure-capture rule enforced by
// construction.
type activationEnv struct{ rt *Runtime }

func (e activationEnv) Context() context.Context { return e.rt.ctx }

func (e activationEnv) Go(name string, fn func(ctx context.Context)) {
	rt := e.rt
	rt.Go(name, func() { fn(rt.ctx) })
}

func (e activationEnv) RegisterConfig(name string, payload []byte) error {
	if cfgload.Exists(name) {
		// A live config wins over a gossiped bundle — never clobber what
		// the operator loaded at boot.
		return nil
	}
	return cfgload.Register(name, json.RawMessage(payload))
}

// RegisterRoleActivator registers the runtime role-init implementation for
// a service role. Call before or after boot; last registration per role
// wins (config-reload friendly).
func (rt *Runtime) RegisterRoleActivator(a ports.RoleActivator) error {
	if a == nil || a.Role() == "" {
		return fmt.Errorf("role activation: nil activator or empty role")
	}
	m := rt.roleActivation
	m.mu.Lock()
	defer m.mu.Unlock()
	m.activators[a.Role()] = a
	return nil
}

// HasRoleActivator reports whether a role can be activated at runtime.
func (rt *Runtime) HasRoleActivator(role string) bool {
	m := rt.roleActivation
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.activators[role] != nil
}

// ActiveTakeoverRoles lists roles brought up via runtime activation.
func (rt *Runtime) ActiveTakeoverRoles() []string {
	m := rt.roleActivation
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.active))
	for role := range m.active {
		out = append(out, role)
	}
	return out
}

// ActivateRole runs the full role bring-up sequence with a decrypted config
// bundle. Idempotent per role. On any failure the sequence unwinds: handlers
// registered so far are unregistered and the role is not advertised.
func (rt *Runtime) ActivateRole(ctx context.Context, role string, bundle secrets.ConfigBundle) error {
	m := rt.roleActivation
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.active[role] {
		return nil
	}
	activator, ok := m.activators[role]
	if !ok {
		return fmt.Errorf("role activation: no activator registered for %q", role)
	}

	// 1. Make the role's configs visible (cfgload.Exists → capability
	//    detection passes; the activator decodes its own payloads).
	env := activationEnv{rt: rt}
	for name, payload := range bundle {
		if err := env.RegisterConfig(name, payload); err != nil {
			return fmt.Errorf("role activation %q: config %q: %w", role, name, err)
		}
	}

	// 2. Run the activator inside a scope bracket: every handler it
	//    registers (through any path that lands in the handler registry)
	//    is attributed to this role for precise teardown.
	owner := "role:" + role
	m.tracker.Begin(owner)
	rt.Registry().SetRegistrationObserver(func(name string) {
		m.tracker.Note(owner, compose.FunctionID(name))
	})
	err := activator.Activate(ctx, env)
	rt.Registry().SetRegistrationObserver(nil)
	registered := m.tracker.End(owner)

	if err != nil {
		// Unwind partial registrations — a half-activated role must not
		// leave dispatchable handlers behind.
		for _, fqn := range registered {
			rt.Registry().Unregister(string(fqn))
		}
		m.tracker.Release(owner)
		return fmt.Errorf("role activation %q: %w", role, err)
	}
	for _, fqn := range registered {
		m.tracker.Note(owner, fqn) // persist into Owned for teardown
	}

	// 3. Enable dispatch for the role.
	roles := appendUniqueString(rt.cfg.Roles, role)
	rt.Registry().SetEnabledRoles(roles)

	// 4. Re-export + re-publish handler advertisements (needs the
	//    reflection registry captured by InitSwarm; standalone runtimes
	//    without swarm skip the LAD publish — fleet.peer re-advertise in
	//    step 5 still carries the role).
	if rt.rpcRegistry != nil {
		if err := rt.PublishRPCHandlersToLAD(ctx, rt.rpcRegistry); err != nil {
			log.Printf("[ROLE-ACTIVATE] %s: handler publish failed (will refresh on next cycle): %v", role, err)
		}
	}

	// 5. Re-advertise the role set on fleet.peer.
	rt.SetRoles(roles)

	m.active[role] = true
	log.Printf("[ROLE-ACTIVATE] role %q active: %d handlers registered", role, len(registered))
	return nil
}

// DeactivateRole tears a runtime-activated role down: stop services, remove
// exactly the handlers the activation registered, disable dispatch, and
// re-advertise the reduced role set.
func (rt *Runtime) DeactivateRole(ctx context.Context, role string) error {
	m := rt.roleActivation
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.active[role] {
		return nil
	}
	activator, ok := m.activators[role]
	if !ok {
		return fmt.Errorf("role deactivation: no activator for %q", role)
	}
	if err := activator.Deactivate(ctx); err != nil {
		return fmt.Errorf("role deactivation %q: %w", role, err)
	}

	owner := "role:" + role
	for _, fqn := range m.tracker.Release(owner) {
		rt.Registry().Unregister(string(fqn))
	}

	roles := removeString(rt.cfg.Roles, role)
	rt.Registry().SetEnabledRoles(roles)
	if rt.rpcRegistry != nil {
		if err := rt.PublishRPCHandlersToLAD(ctx, rt.rpcRegistry); err != nil {
			log.Printf("[ROLE-DEACTIVATE] %s: handler publish failed: %v", role, err)
		}
	}
	rt.SetRoles(roles)

	delete(m.active, role)
	log.Printf("[ROLE-DEACTIVATE] role %q inactive", role)
	return nil
}

func appendUniqueString(list []string, s string) []string {
	for _, v := range list {
		if v == s {
			return append([]string(nil), list...)
		}
	}
	return append(append([]string(nil), list...), s)
}

func removeString(list []string, s string) []string {
	out := make([]string, 0, len(list))
	for _, v := range list {
		if v != s {
			out = append(out, v)
		}
	}
	return out
}
