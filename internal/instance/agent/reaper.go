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

// Package agent is the instance manager: PID 1 in every Postgres pod, with the postmaster
// as an ordinary child process rather than something it exec'd into. That structure is
// what buys in-place restart, fencing and online agent upgrade without a Pod restart.
package agent

import (
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// postmasterExecutable is the exact program name a reapable orphan must be running. The
// match is exact rather than a prefix so that pg_basebackup, pg_rewind and postgres_fdw
// helper processes are never mistaken for one.
const postmasterExecutable = "postgres"

// Process is the minimum a reaper needs to know about a process.
type Process struct {
	// PID is the process id.
	PID int
	// PPid is the parent process id.
	PPid int
	// Executable is the base name of the program the process is running.
	Executable string
}

// SelectReapable picks the orphans this agent is responsible for waiting on.
//
// The scoping is the whole point. A generic init reaper - tini, dumb-init, a bare
// wait(-1) loop - steals the postmaster's own wait status, and with it the agent's only
// way to tell a clean shutdown from a crash. What actually needs reaping is narrower:
// syslogger.c sets SIG_IGN on SIGINT, SIGTERM and SIGQUIT, so the logging collector
// deliberately outlives the postmaster and is reparented to PID 1. So: adopted by PID 1,
// running exactly "postgres", and not the postmaster itself.
// A postmasterPID of zero means the caller does not know which process the postmaster is,
// and nothing is reaped at all. Reaping too little only leaks a zombie until the next
// sweep; reaping too much consumes the postmaster's exit status and destroys the agent's
// only way to tell a clean shutdown from a crash.
func SelectReapable(processes []Process, postmasterPID int) []int {
	if postmasterPID <= 0 {
		return nil
	}
	var reapable []int
	for _, process := range processes {
		if process.PPid != 1 {
			continue
		}
		if process.PID == postmasterPID || process.PID == os.Getpid() {
			continue
		}
		if process.Executable != postmasterExecutable {
			continue
		}
		reapable = append(reapable, process.PID)
	}
	slices.Sort(reapable)
	return reapable
}

// ProcFS reads the process table from /proc.
type ProcFS struct {
	// Root is the mount point, "/proc" in every real deployment and a fixture directory
	// in tests.
	Root string
}

// Processes enumerates /proc.
func (p ProcFS) Processes() ([]Process, error) {
	root := p.Root
	if root == "" {
		root = "/proc"
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	processes := make([]Process, 0, len(entries))
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		process, ok := readProcess(root, pid)
		if !ok {
			continue
		}
		processes = append(processes, process)
	}
	return processes, nil
}

// readProcess reads one process's parent and executable name. A process that exits
// between the directory listing and the read is not an error: it is simply gone, and the
// kernel has already reaped whatever was left of it.
func readProcess(root string, pid int) (Process, bool) {
	status, err := os.ReadFile(filepath.Join(root, strconv.Itoa(pid), "status"))
	if err != nil {
		return Process{}, false
	}
	process := Process{PID: pid, PPid: -1}
	for line := range strings.SplitSeq(string(status), "\n") {
		field, value, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		value = strings.TrimSpace(value)
		switch field {
		case "Name":
			process.Executable = value
		case "PPid":
			parent, err := strconv.Atoi(value)
			if err != nil {
				return Process{}, false
			}
			process.PPid = parent
		}
	}
	return process, process.PPid >= 0
}

// PostmasterPID reads the postmaster's own pid out of postmaster.pid, for the case where
// the agent adopted a postmaster it did not spawn. When the agent spawned it, the exec.Cmd
// pid is authoritative and this is only a cross-check. An unreadable or malformed file
// yields zero, which SelectReapable treats as "reap nothing".
func PostmasterPID(dataDir string) int {
	contents, err := os.ReadFile(filepath.Join(dataDir, "postmaster.pid"))
	if err != nil {
		return 0
	}
	first, _, _ := strings.Cut(string(contents), "\n")
	pid, err := strconv.Atoi(strings.TrimSpace(first))
	if err != nil {
		return 0
	}
	return pid
}
