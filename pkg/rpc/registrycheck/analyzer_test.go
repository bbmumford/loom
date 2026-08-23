package registrycheck_test

import (
	"testing"

	"github.com/bbmumford/loom/pkg/rpc/registrycheck"
	"golang.org/x/tools/go/analysis/analysistest"
)

// TestAnalyzer runs the pass over the example fixture. Every discard form
// carries a // want marker the pass must produce; every consuming form and the
// same-named method on another type carry none and must stay silent.
func TestAnalyzer(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), registrycheck.Analyzer, "example")
}
