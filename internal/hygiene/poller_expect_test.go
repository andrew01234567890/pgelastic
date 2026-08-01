/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package hygiene

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A helper that takes a Gomega must assert through it, never through the package-level
// Expect.
//
// Gomega's poller only retries a failure raised through the Gomega it injected. The
// package-level Expect goes to Ginkgo's Fail, which records the failure and panics; the
// poller sees a panic with no assertion recorded against its own handler and re-panics it
// instead of trying again. So one bad read - a kubectl exec against a Pod that is restarting,
// a Get during a leader change - ends the spec rather than being waited out, and because
// these suites use Ordered containers without ContinueOnFailure, everything after it is
// skipped. The retry is the whole reason the helper is called from inside Eventually.
//
// Taking a Gomega parameter is the signal: it says the caller may be a poller, and the only
// safe thing to do with that is use it.
func TestAHelperGivenAGomegaAssertsThroughIt(t *testing.T) {
	roots := []string{"../../test/e2e", "../../internal"}
	var offences []string

	for _, root := range roots {
		err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || !strings.HasSuffix(path, "_test.go") {
				return err
			}
			offences = append(offences, globalExpectsInGomegaFuncs(t, path)...)
			return nil
		})
		if err != nil {
			t.Fatalf("walking %s: %v", root, err)
		}
	}

	for _, offence := range offences {
		t.Errorf("%s asserts through the package-level Expect while holding a Gomega, so a "+
			"failure there aborts the spec instead of being retried", offence)
	}
}

// globalExpectsInGomegaFuncs reports every function in one file that takes a Gomega and still
// calls the package-level Expect.
func globalExpectsInGomegaFuncs(t *testing.T, path string) []string {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parsing %s: %v", path, err)
	}

	var offences []string
	for _, decl := range file.Decls {
		function, ok := decl.(*ast.FuncDecl)
		if !ok || function.Body == nil || !takesGomega(function.Type) {
			continue
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			if name, ok := call.Fun.(*ast.Ident); ok && name.Name == "Expect" {
				offences = append(offences, fset.Position(call.Pos()).String())
			}
			return true
		})
	}
	return offences
}

func takesGomega(signature *ast.FuncType) bool {
	if signature.Params == nil {
		return false
	}
	for _, param := range signature.Params.List {
		name, ok := param.Type.(*ast.Ident)
		if ok && name.Name == "Gomega" {
			return true
		}
		if selector, ok := param.Type.(*ast.SelectorExpr); ok && selector.Sel.Name == "Gomega" {
			return true
		}
	}
	return false
}
