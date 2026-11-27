/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

package rpc

import (
	"errors"
	"fmt"
	"sync"
)

// DeclarationReader reads what an operation's contract declares about it and
// writes that onto the handler a registry is about to store, or refuses the
// registration. It is handed the registry's own copy (owned), so what it writes
// is what Get, All, the HTTP mounts, the mesh export and the publication read,
// and nothing the registering caller still holds changes. It may set the
// handler's Exposure, Middleware and Routes (each route then checked as any
// declared route is, checkRoutes), and wrap its Func or Stream in a function
// that calls the one it replaces; a reader that changes the handler's
// namespace, domain or operation has the registration refused, because the FQN
// is the key the registry has already checked.
//
// loom knows no contract: the process's composition root installs the reader
// (InstallDeclarationReader) and decides what it reads and how it maps it.
type DeclarationReader func(h *Handler) error

// ErrDeclarationReaderInstalled is InstallDeclarationReader's refusal of a second
// reader. With two, a handler's stored declaration would depend on which reader
// was installed when it registered.
var ErrDeclarationReaderInstalled = errors.New("rpc: a declaration reader is already installed")

var (
	declarationMu     sync.RWMutex
	declarationReader DeclarationReader
)

// InstallDeclarationReader makes read the process's declaration reader. From then
// on every Register, RegisterAll and RegisterProxy of every Registry hands it the
// copy it is about to store, after the scope checks, and a refusal refuses the
// registration (RegisterAll and RegisterProxy then store none of their batch). It
// is refused for a nil function and when a reader is already installed. A
// registration made before the install is never read, so a composition root
// installs the reader before it composes anything.
func InstallDeclarationReader(read DeclarationReader) error {
	if read == nil {
		return errors.New("rpc: InstallDeclarationReader needs a function")
	}
	declarationMu.Lock()
	defer declarationMu.Unlock()
	if declarationReader != nil {
		return ErrDeclarationReaderInstalled
	}
	declarationReader = read
	return nil
}

// readDeclaration runs the installed reader over stored, the copy prepareStored
// returns, and refuses what the reader refuses and a reader that renamed the
// handler. With no reader installed it changes nothing.
func readDeclaration(stored *Handler) error {
	declarationMu.RLock()
	read := declarationReader
	declarationMu.RUnlock()
	if read == nil {
		return nil
	}
	fqn := stored.FQN()
	if err := read(stored); err != nil {
		return fmt.Errorf("handler %s: its contract: %w", fqn, err)
	}
	if got := stored.FQN(); got != fqn {
		return fmt.Errorf("handler %s: the declaration reader renamed it %s", fqn, got)
	}
	return nil
}
