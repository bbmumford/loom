/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package task

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// DomainHandler resolves a task into a domain-specific execution.
type DomainHandler interface {
	TaskType() string
	Handle(ctx context.Context, task *Task) ([]byte, error)
}

// FuncHandler adapts a simple function with a declared task type.
type FuncHandler struct {
	Type string
	Fn   func(ctx context.Context, task *Task) ([]byte, error)
}

// TaskType implements DomainHandler.
func (f FuncHandler) TaskType() string { return f.Type }

// Handle implements DomainHandler.
func (f FuncHandler) Handle(ctx context.Context, task *Task) ([]byte, error) {
	if f.Fn == nil {
		return nil, errors.New("task: func handler has no function")
	}
	return f.Fn(ctx, task)
}

// HandlerRegistration couples a task type with the handler implementation.
type HandlerRegistration struct {
	TaskType string
	Handler  DomainHandler
}

// Registry manages domain handlers available to executors.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]DomainHandler
}

// NewRegistry returns an empty handler registry.
func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]DomainHandler)}
}

// Register inserts a handler for one or more task types.
func (r *Registry) Register(reg HandlerRegistration) error {
	if reg.TaskType == "" {
		return errors.New("task: registration requires task type")
	}
	if reg.Handler == nil {
		return errors.New("task: registration handler is nil")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[reg.TaskType] = reg.Handler
	return nil
}

// Handler returns a handler for the task type.
func (r *Registry) Handler(taskType string) (DomainHandler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[taskType]
	return h, ok
}

// MustRegister registers a handler or panics on error.
func (r *Registry) MustRegister(reg HandlerRegistration) {
	if err := r.Register(reg); err != nil {
		panic(fmt.Errorf("task: handler registration failed: %w", err))
	}
}
