/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */
package node

import (
	"bytes"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/bbmumford/argus"
)

// TestNodeDebugLoggersAreArgusLoggers pins that node's debug loggers live in argus's registry, the one an
// endpoint's DEBUG list, argus.Configure and argus.GetNamespaces act on: node's namespaces are listed there,
// and argus's switch turns them on and names their writer. A node logger built from a private registry
// would be invisible to the endpoint's debug surface and deaf to its switch, and this test fails.
func TestNodeDebugLoggersAreArgusLoggers(t *testing.T) {
	if !slices.Contains(argus.GetNamespaces(), "mesh.node.runtime") {
		t.Fatalf("argus.GetNamespaces() lacks node's %q namespace: %v", "mesh.node.runtime", argus.GetNamespaces())
	}
	var buf bytes.Buffer
	argus.Configure(true, &buf)
	t.Cleanup(func() { argus.Configure(false, os.Stderr) })
	dbgNode.Printf("argus switch probe")
	if !strings.Contains(buf.String(), "argus switch probe") {
		t.Fatalf("argus.Configure(true, w) did not reach node's logger; w holds %q", buf.String())
	}
}
