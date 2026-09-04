package rpc

import (
	"testing"

	"github.com/bbmumford/loom/pkg/rpc/scope"
)

// TestRegistryShim_OmitsRequirement_MeshInvariant pins the load-bearing invariant
// that keeps the loom two-arm on the rpc path only: the mesh-export adapter
// registryShim must NOT expose a Requirement(), so node/handlers
// validateTenantScope falls to the ScopeTag presence path for every mesh-exported
// handler. If registryShim gains a Requirement() method, mesh-exported two-arm
// handlers fail-closed on the mesh HandlerRegistry — which the endpoint spine
// seeds with a presence policy but no authz policy — so seed the mesh registry's
// SetAuthzPolicy in the same change if this is ever lifted.
func TestRegistryShim_OmitsRequirement_MeshInvariant(t *testing.T) {
	var meta interface{} = newRegistryShim(&Handler{
		Namespace: "ns", Domain: "d", Operation: "Op", Scope: ScopeTenant,
	})
	if _, ok := meta.(interface{ Requirement() *scope.Requirement }); ok {
		t.Fatal("registryShim must NOT implement Requirement(): validateTenantScope would then honor a " +
			"two-arm Requirement on the mesh HandlerRegistry and fail-close every mesh-exported Requirement " +
			"handler on the unseeded mesh registry. If lifting this deliberately, seed the mesh registry's " +
			"SetAuthzPolicy in the same change.")
	}
}
