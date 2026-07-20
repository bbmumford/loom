/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package compose

import "context"

// TriggerKind is the two-level trigger taxonomy's first level.
type TriggerKind string

const (
	TriggerHTTP      TriggerKind = "http"      // chi-router attach of Runtime HTTP handlers
	TriggerCron      TriggerKind = "cron"      // scheduled; fan-in via claim-set, never a central lock
	TriggerState     TriggerKind = "state"     // directory/CRDT state change (e.g. RoleMissing → activation)
	TriggerSubscribe TriggerKind = "subscribe" // swarm topic subscription
	TriggerStream    TriggerKind = "stream"    // long-lived stream attach
)

// Trigger is one declarative "when X on the mesh, invoke Y" instance.
type Trigger struct {
	ID       string
	Kind     TriggerKind
	Function FunctionID
	// Spec is the kind-specific configuration (route, cron expr, topic,
	// state selector …), opaque to the registry.
	Spec []byte
}

// KindHandler implements one TriggerKind: it arms instances and invokes
// their Function when the condition fires.
type KindHandler interface {
	// Arm activates one trigger instance. Long-lived watching binds ctx
	// (the runtime lifetime context); return tears the instance down.
	Arm(ctx context.Context, t Trigger, invoke func(ctx context.Context, fn FunctionID, event []byte) FunctionResult) error
}

// TriggerRegistry stores trigger instances with LATE BINDING: registering a
// kind replays stored instances of that kind; registering an instance
// before its kind stores it pending — order-independent composition.
type TriggerRegistry interface {
	// RegisterKind installs the handler for a kind and replays pending
	// instances of that kind against it.
	RegisterKind(kind TriggerKind, h KindHandler) error

	// RegisterTrigger stores + arms an instance (pending if its kind is not
	// yet registered).
	RegisterTrigger(t Trigger) error

	// RemoveTrigger disarms and forgets an instance.
	RemoveTrigger(id string) error

	// Triggers enumerates registered instances (armed and pending).
	Triggers() []Trigger
}
