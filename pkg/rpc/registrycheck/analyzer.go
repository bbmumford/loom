// Package registrycheck is a go/analysis pass that reports a discarded error
// from an rpc.Registry registration call (Register, RegisterAll, RegisterProxy).
//
// A dropped registration error means a handler silently failed to register and
// the endpoint still comes up (fail-open). This pass makes such a call fail
// closed at composition startup by requiring the returned error be consumed:
// it flags exactly a bare call statement and a blank-identifier assignment,
// and leaves every consuming form (named assignment, checked if-init, return,
// wrapping) untouched. Callers wire it into their vet step so a newly added
// discard fails the build.
package registrycheck

import (
	"go/ast"
	"go/types"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// registryPkg is the import path whose *Registry methods this pass guards.
// Resolution is by *types.Func plus receiver named type plus this path, never
// by identifier text, so a same-named method on any other type is not flagged.
const registryPkg = "github.com/bbmumford/loom/pkg/rpc"

// Analyzer reports a discarded error result from a Registry registration call.
var Analyzer = &analysis.Analyzer{
	Name:     "registrycheck",
	Doc:      "reports discarded errors from rpc.Registry Register/RegisterAll/RegisterProxy calls",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

func run(pass *analysis.Pass) (any, error) {
	insp := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

	// Only statement positions where the single error result can be dropped:
	// a bare call statement, or an assignment whose sole target is blank.
	filter := []ast.Node{
		(*ast.ExprStmt)(nil),
		(*ast.AssignStmt)(nil),
	}
	insp.Preorder(filter, func(n ast.Node) {
		switch s := n.(type) {
		case *ast.ExprStmt:
			call, ok := s.X.(*ast.CallExpr)
			if !ok {
				return
			}
			if name, ok := targetMethod(pass, call); ok {
				report(pass, call, name)
			}
		case *ast.AssignStmt:
			// `_ = reg.RegisterProxy(...)`: exactly one call on the right whose
			// single error result is assigned to the blank identifier. A named
			// target, or any multi-value shape, consumes the error and is left.
			if len(s.Lhs) != 1 || len(s.Rhs) != 1 || !isBlank(s.Lhs[0]) {
				return
			}
			call, ok := s.Rhs[0].(*ast.CallExpr)
			if !ok {
				return
			}
			if name, ok := targetMethod(pass, call); ok {
				report(pass, call, name)
			}
		}
	})
	return nil, nil
}

// targetMethod returns the method name when call's callee resolves to
// Register, RegisterAll or RegisterProxy on *rpc.Registry from registryPkg,
// and false for anything else (including a same-named method on another type).
func targetMethod(pass *analysis.Pass, call *ast.CallExpr) (string, bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return "", false
	}
	fn, ok := pass.TypesInfo.Uses[sel.Sel].(*types.Func)
	if !ok {
		return "", false
	}
	switch fn.Name() {
	case "Register", "RegisterAll", "RegisterProxy":
	default:
		return "", false
	}
	sig, ok := fn.Type().(*types.Signature)
	if !ok || sig.Recv() == nil {
		return "", false
	}
	recv := sig.Recv().Type()
	if ptr, ok := recv.(*types.Pointer); ok {
		recv = ptr.Elem()
	}
	named, ok := recv.(*types.Named)
	if !ok {
		return "", false
	}
	obj := named.Obj()
	if obj.Name() != "Registry" || obj.Pkg() == nil || obj.Pkg().Path() != registryPkg {
		return "", false
	}
	return fn.Name(), true
}

func isBlank(e ast.Expr) bool {
	id, ok := e.(*ast.Ident)
	return ok && id.Name == "_"
}

func report(pass *analysis.Pass, call *ast.CallExpr, method string) {
	pass.Reportf(call.Pos(),
		"registration error from Registry.%s is discarded; a failed handler registration must abort composition startup — assign and return/propagate the error (or log.Fatal at a top-level init), never drop it",
		method)
}
