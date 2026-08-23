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

	"github.com/bbmumford/loom/ports"
	"github.com/bbmumford/loom/secrets"
)

// roleActivationManager is the Phase-1 runtime role-init path (plan §1.4):
// today roles are fixed at boot and SetRoles only re-advertises the label;
// this manager can bring a role's services up (and down) AFTER boot, on a
// won takeover claim or an operator action.
//
// Activation sequence (the ordering is the contract — see ActivateRole):
// configs → Activate → open exact-generation admission → atomically publish
// its staged registrations → SetEnabledRoles(+role) →
// PublishRPCHandlersToLAD → SetRoles re-advertise. Teardown reverses it,
// unregistering EXACTLY the exact handler registrations the activation owns,
// identified by immutable registry handles rather than names or a temporal
// observer window.
type roleActivationManager struct {
	rt *Runtime

	// transitionMu serializes the rare multi-step activation/deactivation
	// transitions. mu remains available to task admission while deactivation
	// waits for an already-admitted generation to drain.
	transitionMu sync.Mutex
	mu           sync.Mutex
	activators   map[string]ports.RoleActivator
	active       map[string]bool

	// generations own exact registry scopes and the exact activator instance
	// that brought their resources up.
	generations map[string]*roleGeneration
}

func newRoleActivationManager(rt *Runtime) *roleActivationManager {
	m := &roleActivationManager{
		rt:          rt,
		activators:  map[string]ports.RoleActivator{},
		active:      map[string]bool{},
		generations: map[string]*roleGeneration{},
	}
	return m
}

// roleGeneration is the admission/drain state for one activation. It
// implements the handler registry TaskAdmission contract and is deliberately
// pointer-bound: a
// replacement generation cannot reopen a snapshot captured from its
// predecessor.
type roleGeneration struct {
	manager   *roleActivationManager
	role      string
	activator ports.RoleActivator
	scope     ports.RegistrationScope

	accepting   bool
	inFlight    int
	drained     chan struct{}
	drainClosed bool
}

func newRoleGeneration(
	manager *roleActivationManager,
	role string,
	activator ports.RoleActivator,
) *roleGeneration {
	return &roleGeneration{
		manager:   manager,
		role:      role,
		activator: activator,
		drained:   make(chan struct{}),
	}
}

func (g *roleGeneration) Acquire() (func(), bool) {
	if g == nil || g.manager == nil {
		return nil, false
	}
	m := g.manager
	m.mu.Lock()
	if !g.accepting {
		m.mu.Unlock()
		return nil, false
	}
	g.inFlight++
	m.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			m.mu.Lock()
			defer m.mu.Unlock()
			if g.inFlight <= 0 {
				panic("role task admission counter underflow")
			}
			g.inFlight--
			if !g.accepting && g.inFlight == 0 && !g.drainClosed {
				close(g.drained)
				g.drainClosed = true
			}
		})
	}, true
}

func (g *roleGeneration) closeAdmissionLocked() <-chan struct{} {
	g.accepting = false
	if g.inFlight == 0 && !g.drainClosed {
		close(g.drained)
		g.drainClosed = true
	}
	return g.drained
}

func (g *roleGeneration) reopenAdmissionLocked() {
	if g.drainClosed {
		g.drained = make(chan struct{})
		g.drainClosed = false
	}
	g.accepting = true
}

// activationEnv implements ports.ActivationEnv over the runtime: role
// goroutines bind rt.Context() through rt.Go (wg-tracked, panic-recovered)
// so Shutdown drains them — the H7 closure-capture rule enforced by
// construction.
type activationEnv struct {
	rt    *Runtime
	scope ports.RegistrationScope
}

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

func (e activationEnv) RegistrationScope() ports.RegistrationScope {
	return e.scope
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
	m.transitionMu.Lock()
	defer m.transitionMu.Unlock()

	m.mu.Lock()
	if m.active[role] {
		m.mu.Unlock()
		return nil
	}
	activator, ok := m.activators[role]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("role activation: no activator registered for %q", role)
	}

	// 1. Make the role's configs visible (cfgload.Exists → capability
	//    detection passes; the activator decodes its own payloads).
	generation := newRoleGeneration(m, role, activator)
	registrationScope, err := rt.Registry().OpenRegistrationScope(role, generation)
	if err != nil {
		return fmt.Errorf("role activation %q: open registration scope: %w", role, err)
	}
	generation.scope = registrationScope
	env := activationEnv{rt: rt, scope: registrationScope}

	m.mu.Lock()
	m.generations[role] = generation
	m.mu.Unlock()

	for name, payload := range bundle {
		if err := env.RegisterConfig(name, payload); err != nil {
			rt.Registry().CloseRegistrationScope(registrationScope)
			m.mu.Lock()
			if m.generations[role] == generation {
				delete(m.generations, role)
			}
			m.mu.Unlock()
			return fmt.Errorf("role activation %q: config %q: %w", role, name, err)
		}
	}

	// 2. Run the exact activator with a causal registry capability. Global or
	// unrelated registrations during this call are not inferred as ownership.
	err = activator.Activate(ctx, env)
	if err != nil {
		// Closing the scope is atomic with registration: a racing scoped
		// registration is either included in handles or rejected.
		registered, _ := rt.Registry().CloseRegistrationScope(registrationScope)
		for _, handle := range registered {
			rt.Registry().UnregisterExact(handle)
		}
		m.mu.Lock()
		if m.generations[role] == generation {
			delete(m.generations, role)
		}
		m.mu.Unlock()
		return fmt.Errorf("role activation %q: %w", role, err)
	}

	// 3. The activator succeeded. Make its generation execution-ready before
	// atomically publishing its reserved registrations. During Activate the
	// names were absent from Resolve/GetMeta/HasHandler, so they could neither
	// execute locally nor suppress a real remote provider.
	m.mu.Lock()
	generation.reopenAdmissionLocked()
	m.mu.Unlock()
	if err := rt.Registry().PublishRegistrationScope(registrationScope); err != nil {
		m.mu.Lock()
		generation.closeAdmissionLocked()
		m.mu.Unlock()
		registered, _ := rt.Registry().CloseRegistrationScope(registrationScope)
		for _, handle := range registered {
			rt.Registry().UnregisterExact(handle)
		}
		deactivateErr := activator.Deactivate(ctx)
		m.mu.Lock()
		if m.generations[role] == generation {
			delete(m.generations, role)
		}
		m.mu.Unlock()
		if deactivateErr != nil {
			return fmt.Errorf(
				"role activation %q: publish registrations: %w (activation cleanup: %v)",
				role,
				err,
				deactivateErr,
			)
		}
		return fmt.Errorf("role activation %q: publish registrations: %w", role, err)
	}

	// 4. Enable dispatch for the role.
	roles := appendUniqueString(rt.cfg.Roles, role)
	rt.Registry().SetEnabledRoles(roles)

	// 5. Re-export + re-publish handler advertisements (needs the
	//    reflection registry captured by InitSwarm; standalone runtimes
	//    without swarm skip the LAD publish — fleet.peer re-advertise in
	//    step 6 still carries the role).
	if rt.rpcRegistry != nil {
		if err := rt.PublishRPCHandlersToLAD(ctx, rt.rpcRegistry); err != nil {
			log.Printf("[ROLE-ACTIVATE] %s: handler publish failed (will refresh on next cycle): %v", role, err)
		}
	}

	// 6. Re-advertise the role set on fleet.peer.
	rt.SetRoles(roles)

	m.mu.Lock()
	m.active[role] = true
	m.mu.Unlock()
	log.Printf("[ROLE-ACTIVATE] role %q active", role)
	return nil
}

// DeactivateRole tears a runtime-activated role down in resource-safe order:
// close the exact generation to new task admission, drain every task already
// admitted against it, stop services, remove exactly the handlers the
// activation registered, disable dispatch, and re-advertise the reduced role
// set. An unknown/partial teardown failure leaves admission closed; only an
// explicit transactional still-active disposition reopens it for retry.
func (rt *Runtime) DeactivateRole(ctx context.Context, role string) error {
	m := rt.roleActivation
	m.transitionMu.Lock()
	defer m.transitionMu.Unlock()

	m.mu.Lock()
	if !m.active[role] {
		m.mu.Unlock()
		return nil
	}
	generation := m.generations[role]
	if generation == nil {
		m.mu.Unlock()
		return fmt.Errorf("role deactivation %q: active role has no admission generation", role)
	}
	activator := generation.activator
	if activator == nil {
		m.mu.Unlock()
		return fmt.Errorf("role deactivation %q: generation has no activator", role)
	}
	m.mu.Unlock()

	if _, ok := rt.Registry().SealRegistrationScope(generation.scope); !ok {
		return fmt.Errorf("role deactivation %q: registration scope is missing", role)
	}

	m.mu.Lock()
	drained := generation.closeAdmissionLocked()
	m.mu.Unlock()

	select {
	case <-drained:
	case <-ctx.Done():
		m.mu.Lock()
		if m.active[role] && m.generations[role] == generation {
			generation.reopenAdmissionLocked()
		}
		m.mu.Unlock()
		rt.Registry().ReopenRegistrationScope(generation.scope)
		return fmt.Errorf("role deactivation %q: wait for admitted tasks: %w", role, ctx.Err())
	}

	if err := activator.Deactivate(ctx); err != nil {
		if ports.RoleDeactivationDispositionOf(err) == ports.RoleDeactivationStillActive {
			m.mu.Lock()
			if m.active[role] && m.generations[role] == generation {
				generation.reopenAdmissionLocked()
			}
			m.mu.Unlock()
			rt.Registry().ReopenRegistrationScope(generation.scope)
		}
		return fmt.Errorf("role deactivation %q: %w", role, err)
	}

	registered, _ := rt.Registry().CloseRegistrationScope(generation.scope)
	for _, handle := range registered {
		rt.Registry().UnregisterExact(handle)
	}

	roles := removeString(rt.cfg.Roles, role)
	rt.Registry().SetEnabledRoles(roles)
	if rt.rpcRegistry != nil {
		if err := rt.PublishRPCHandlersToLAD(ctx, rt.rpcRegistry); err != nil {
			log.Printf("[ROLE-DEACTIVATE] %s: handler publish failed: %v", role, err)
		}
	}
	rt.SetRoles(roles)

	m.mu.Lock()
	if m.generations[role] == generation {
		delete(m.generations, role)
	}
	delete(m.active, role)
	m.mu.Unlock()
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
