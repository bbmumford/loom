// Stub of the loom rpc.Registry surface the pass resolves against — only the
// three registration methods, plus a same-named method on a different type the
// pass must not flag. The bodies are irrelevant; only the types and the import
// path (github.com/bbmumford/loom/pkg/rpc) matter to the analyzer.
package rpc

// Handler is the registration payload; its contents are irrelevant here.
type Handler struct{}

// Registry mirrors the real registry's registration method set.
type Registry struct{}

func (r *Registry) Register(h Handler) error         { return nil }
func (r *Registry) RegisterAll(hs []Handler) error   { return nil }
func (r *Registry) RegisterProxy(hs []Handler) error { return nil }

// Unrelated carries a same-named Register method on a different type; a
// discarded call to it must NOT be flagged.
type Unrelated struct{}

func (u *Unrelated) Register(h Handler) error { return nil }
