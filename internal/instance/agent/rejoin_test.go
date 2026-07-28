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

package agent

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/andrew01234567890/pgelastic/internal/instance/pgtool"
)

// TestRejoinFallsBackToARecloneAndSaysSo drives the rejoin against a toolchain whose
// binaries do not exist, which is what a data directory pg_rewind cannot be run on looks
// like from here: the control file cannot be read, the target cannot be proven shut down
// cleanly, and the only way back is to take the primary's history wholesale.
//
// The re-clone is then reported before it starts rather than after it finishes. A member
// silently rebuilding itself for ten minutes is a member the operator counts as available
// headroom it does not have.
func TestRejoinFallsBackToARecloneAndSaysSo(t *testing.T) {
	root := t.TempDir()
	options := Options{
		Member:      holderOne,
		Instance:    leaseName,
		Namespace:   "default",
		PeerService: leaseName + "-peers",
		DataDir:     filepath.Join(root, "data"),
		WALDir:      filepath.Join(root, "wal"),
		BinDir:      filepath.Join(root, "bin"),
	}
	tools := pgtool.Toolchain{BinDir: options.BinDir, DataDir: options.DataDir, WALDir: options.WALDir}

	// The context is already done, so the wait for the primary's socket ends at once rather
	// than running out its own five minutes. What is being proven is which path was taken,
	// not how long dialling a host that does not exist takes.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var methods []RejoinMethod
	err := Rejoin(ctx, options, tools, holderTwo, func(method RejoinMethod) {
		methods = append(methods, method)
	})

	if err == nil {
		t.Fatal("a re-clone from a primary that does not exist has to fail")
	}
	want := []RejoinMethod{RejoinRewinding, RejoinRecloning}
	if len(methods) != len(want) || methods[0] != want[0] || methods[1] != want[1] {
		t.Fatalf("reported methods = %v, want %v", methods, want)
	}
}

func TestRejoinToleratesAnAbsentObserver(t *testing.T) {
	root := t.TempDir()
	options := Options{
		Member:      holderOne,
		Instance:    leaseName,
		Namespace:   "default",
		PeerService: leaseName + "-peers",
		DataDir:     filepath.Join(root, "data"),
		WALDir:      filepath.Join(root, "wal"),
		BinDir:      filepath.Join(root, "bin"),
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := Rejoin(ctx, options, pgtool.Toolchain{BinDir: options.BinDir}, holderTwo, nil); err == nil {
		t.Fatal("the bootstrap path passes no observer and must still reach the re-clone")
	}
}
