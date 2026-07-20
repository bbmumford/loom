/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	swarm "github.com/bbmumford/swarm"

	"github.com/bbmumford/loom/compose"
	"github.com/bbmumford/loom/node/handlers"
)

// This file is the Phase-2 composition bridge: it maps the iii-derived
// compose surfaces onto what the mesh already has.
//
//   REGISTERED — Registry().List()/AllHandlers() enumerate every function.
//   CALLABLE   — ComposeInvoke dispatches a FunctionID locally with the
//                4-way FunctionResult semantics; cross-mesh calls keep
//                using rpc.Call (forwarding fragilities untouched).
//   TRIGGERABLE— TriggerRegistry() arms compose.Trigger instances;
//                "subscribe" (swarm topic) and "cron" (interval) kinds are
//                built in; consumers late-bind "http"/"state" kinds (chi
//                attach, RoleMissing) via RegisterKind.
//   OBSERVABLE — the existing Prometheus export + /mesh/status wire
//                contracts are untouched; the scope tracker inside role
//                activation gives per-role function ownership.

// ComposeInvoke dispatches one FunctionID with an opaque event payload and
// maps the outcome onto compose.FunctionResult:
//
//	RPC handler   → Success/Failure (payload/error)
//	Task handler  → Deferred (accepted for async execution)
//	no handler    → Failure (ErrHandlerNotFound text)
func (rt *Runtime) ComposeInvoke(ctx context.Context, fn compose.FunctionID, event []byte) compose.FunctionResult {
	reg := rt.Registry()
	meta, ok := reg.GetMeta(string(fn))
	if !ok {
		return compose.FunctionResult{Kind: compose.ResultFailure, Err: handlers.ErrHandlerNotFound.Error()}
	}

	if _, isRPC := meta.(handlers.RPCHandler); isRPC {
		resp, err := reg.Dispatch(ctx, &handlers.RPCRequest{
			ID:      fmt.Sprintf("compose-%d", time.Now().UnixNano()),
			Handler: string(fn),
			Payload: event,
			Context: map[string]interface{}{},
		})
		switch {
		case err != nil:
			return compose.FunctionResult{Kind: compose.ResultFailure, Err: err.Error()}
		case resp == nil:
			return compose.FunctionResult{Kind: compose.ResultNoResult}
		case !resp.Success:
			return compose.FunctionResult{Kind: compose.ResultFailure, Err: resp.Error}
		default:
			return compose.FunctionResult{Kind: compose.ResultSuccess, Payload: resp.Payload}
		}
	}

	if th, isTask := meta.(handlers.TaskHandler); isTask {
		task := &handlers.Task{
			ID:        fmt.Sprintf("compose-%d", time.Now().UnixNano()),
			Handler:   string(fn),
			Payload:   event,
			CreatedAt: time.Now(),
			Metadata:  map[string]interface{}{},
		}
		// Fire-and-track: the task runs on a runtime goroutine; the
		// trigger receives Deferred immediately (iii semantics).
		rt.Go("compose.task."+string(fn), func() {
			if _, err := th.ExecuteTask(rt.ctx, task); err != nil {
				dbgNode.Printf("compose deferred task %s failed: %v", fn, err)
			}
		})
		return compose.FunctionResult{Kind: compose.ResultDeferred, Payload: []byte(task.ID)}
	}

	return compose.FunctionResult{Kind: compose.ResultFailure, Err: handlers.ErrUnsupportedMode.Error()}
}

var composeRegMu sync.Mutex

// TriggerRegistry returns the runtime's compose trigger registry, creating
// it on first use with the built-in kinds armed. All trigger watch
// goroutines bind rt.Context() through rt.Go (H7 rule).
func (rt *Runtime) TriggerRegistry() *compose.Registry {
	composeRegMu.Lock()
	defer composeRegMu.Unlock()
	if rt.composeReg != nil {
		return rt.composeReg
	}
	spawn := func(name string, fn func(ctx context.Context)) {
		rt.Go(name, func() { fn(rt.ctx) })
	}
	reg := compose.NewRegistry(rt.ctx, spawn, rt.ComposeInvoke)
	_ = reg.RegisterKind(compose.TriggerSubscribe, &swarmSubscribeKind{rt: rt})
	_ = reg.RegisterKind(compose.TriggerCron, &intervalCronKind{})
	rt.composeReg = reg
	return reg
}

// swarmSubscribeKind arms "subscribe" triggers: every record accepted on
// the spec'd swarm topic invokes the function with the record body.
// Spec JSON: {"topic": "<swarm topic>"}.
type swarmSubscribeKind struct{ rt *Runtime }

func (k *swarmSubscribeKind) Arm(ctx context.Context, t compose.Trigger, invoke func(ctx context.Context, fn compose.FunctionID, event []byte) compose.FunctionResult) error {
	var spec struct {
		Topic string `json:"topic"`
	}
	if err := json.Unmarshal(t.Spec, &spec); err != nil || spec.Topic == "" {
		return fmt.Errorf("subscribe trigger %s: invalid spec (want {\"topic\":…}): %v", t.ID, err)
	}
	if k.rt.swarm == nil || k.rt.swarm.Node == nil {
		return fmt.Errorf("subscribe trigger %s: swarm not initialized", t.ID)
	}
	unsub, err := k.rt.swarm.Node.Subscribe(swarm.Topic(spec.Topic), func(r swarm.Record) error {
		if ctx.Err() != nil {
			return nil
		}
		invoke(ctx, t.Function, r.Body)
		return nil
	})
	if err != nil {
		return err
	}
	<-ctx.Done()
	unsub()
	return nil
}

// intervalCronKind arms "cron" triggers on a fixed interval.
// Spec JSON: {"everyMs": N}. Fan-in coordination across nodes belongs to
// the claim-set (plan §2.5 REJECTS a central cron lock) — a cron trigger
// fires on every node that arms it; the invoked function decides whether
// this node acts (e.g. by claim ranking).
type intervalCronKind struct{}

func (k *intervalCronKind) Arm(ctx context.Context, t compose.Trigger, invoke func(ctx context.Context, fn compose.FunctionID, event []byte) compose.FunctionResult) error {
	var spec struct {
		EveryMs int64 `json:"everyMs"`
	}
	if err := json.Unmarshal(t.Spec, &spec); err != nil || spec.EveryMs <= 0 {
		return fmt.Errorf("cron trigger %s: invalid spec (want {\"everyMs\":N>0}): %v", t.ID, err)
	}
	ticker := time.NewTicker(time.Duration(spec.EveryMs) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-ticker.C:
			event, _ := json.Marshal(map[string]int64{"firedAtUnixMs": now.UnixMilli()})
			invoke(ctx, t.Function, event)
		}
	}
}
