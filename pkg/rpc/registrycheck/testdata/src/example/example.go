// Fixture exercising every form the pass must and must not flag.
package example

import "github.com/bbmumford/loom/pkg/rpc"

// discarded holds the fail-open forms the pass must diagnose.
func discarded(reg *rpc.Registry, other *rpc.Unrelated) {
	// Bare call statements — the single error result is dropped.
	reg.Register(rpc.Handler{})        // want `registration error from Registry.Register is discarded`
	reg.RegisterAll([]rpc.Handler{})   // want `registration error from Registry.RegisterAll is discarded`
	reg.RegisterProxy([]rpc.Handler{}) // want `registration error from Registry.RegisterProxy is discarded`

	// Blank-identifier assignments — the error is thrown away explicitly.
	_ = reg.Register(rpc.Handler{})        // want `registration error from Registry.Register is discarded`
	_ = reg.RegisterProxy([]rpc.Handler{}) // want `registration error from Registry.RegisterProxy is discarded`

	// Same-named method on another type — not a Registry call, not flagged.
	other.Register(rpc.Handler{})
	_ = other.Register(rpc.Handler{})
}

// consumed holds the forms that already fail closed; none may be flagged.
func consumed(reg *rpc.Registry) error {
	// Checked if-init.
	if err := reg.Register(rpc.Handler{}); err != nil {
		return err
	}
	// Named assignment then check.
	err := reg.RegisterAll([]rpc.Handler{})
	if err != nil {
		return err
	}
	// Return propagates the error.
	return reg.RegisterProxy([]rpc.Handler{})
}
