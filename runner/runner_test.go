// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package runner_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/ax/internal/workspace"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/google/ax/runner"
)

func TestRun_StartsTaskCommandWithInjectedEnv(t *testing.T) {
	h := newHarness(t)
	outFile := filepath.Join(t.TempDir(), "out.txt")
	h.task.Spec.Env = []*v1alpha1.EnvVar{{Name: "DEMO_ENV", Value: "from-task"}}
	h.task.Spec.Command = []string{"sh", "-c", `echo "$DEMO_ENV $AX_METADATA_URL $(pwd)" > ` + outFile}

	exit := h.runUntilCommandExits(t)
	if exit.Err != nil || exit.ExitCode != 0 {
		t.Fatalf("unexpected exit: %+v", exit)
	}

	out, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("command did not write its output: %v", err)
	}
	fields := strings.Fields(string(out))
	if len(fields) != 3 || fields[0] != "from-task" || fields[1] != fmt.Sprintf("http://127.0.0.1:%d", h.cfg.Port) {
		t.Errorf("unexpected env in command: %q", out)
	}
	// The working directory is the workspace path; compare resolved paths since
	// temp dirs may involve symlinks.
	wantDir, _ := filepath.EvalSymlinks(h.wsPath)
	if gotDir, _ := filepath.EvalSymlinks(fields[2]); gotDir != wantDir {
		t.Errorf("command cwd = %q, want workspace %q", fields[2], h.wsPath)
	}
}

func TestRun_ReportsCommandExitCode(t *testing.T) {
	h := newHarness(t)
	h.task.Spec.Command = []string{"sh", "-c", "exit 3"}

	exit := h.runUntilCommandExits(t)
	if exit.ExitCode != 3 || exit.Err == nil {
		t.Errorf("expected exit code 3 with an error, got %+v", exit)
	}
}

func TestRun_KeepsServingAfterCommandExits(t *testing.T) {
	h := newHarness(t)
	h.task.Spec.Command = []string{"true"}

	h.runUntilCommandExits(t)

	// The command is gone but the runner, and with it the metadata server, is not.
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/healthz", h.cfg.Port))
	if err != nil {
		t.Fatalf("metadata server not reachable after command exit: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("healthz = %d, want 200", resp.StatusCode)
	}
	select {
	case <-h.finished:
		t.Fatalf("Run returned before cancellation: %v", h.err)
	default:
	}
}

func TestRun_StopsRunningCommandOnCancel(t *testing.T) {
	h := newHarness(t)
	// A shell with a sleeping child: the whole process group must be stopped.
	h.task.Spec.Command = []string{"sh", "-c", "sleep 30; sleep 30"}
	h.start(t)

	// Give the command a moment to start before cancelling.
	time.Sleep(100 * time.Millisecond)
	h.cancel()

	h.waitFinished(t, 5*time.Second)
	if exit := h.waitExit(t); exit.Err == nil {
		t.Errorf("expected the stopped command to report a signal exit, got %+v", exit)
	}
}

func TestRun_WithoutCommandWaitsForContext(t *testing.T) {
	h := newHarness(t)
	h.start(t)

	select {
	case <-h.finished:
		t.Fatalf("Run returned before cancellation: %v", h.err)
	case <-time.After(200 * time.Millisecond):
	}

	h.cancel()
	h.waitFinished(t, 5*time.Second)
}

func TestRun_DefaultsWithoutTask(t *testing.T) {
	// A nil Task still serves metadata; the port comes from Config.
	h := newHarness(t)
	h.cfg.Task = nil
	h.start(t)

	waitHealthy(t, h.cfg.Port)
	h.cancel()
	h.waitFinished(t, 5*time.Second)
}

// waitHealthy polls the metadata server's health endpoint until it answers.
func waitHealthy(t *testing.T, port int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/healthz", port))
		if err == nil {
			resp.Body.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("metadata server never became reachable")
}

// harness runs the runner in the background with all state under a temp dir,
// a free port, and a hook that captures the command's exit.
type harness struct {
	cfg    runner.Config
	task   *v1alpha1.Task
	wsPath string

	cancel   context.CancelFunc
	finished chan struct{}
	err      error
	exited   chan runner.CommandExit
}

func TestRun_CommandWaitsForWorkspaceGoal(t *testing.T) {
	t.Setenv("AX_GOAL_AGENT", "claude")
	t.Setenv("ANTHROPIC_API_KEY", "test-key")
	t.Setenv("AX_BOOTSTRAP_TIMEOUT", "5s")
	events := filepath.Join(t.TempDir(), "events")
	// A fake "claude" binary standing in for the goal agent: it records that
	// the goal ran before returning, with a short sleep so a command that
	// started too early would very likely win the race and be observed first.
	bin := t.TempDir()
	script := `#!/bin/sh
sleep 0.2
echo goal >> ` + events + `
`
	if err := os.WriteFile(filepath.Join(bin, "claude"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))

	h := newHarness(t)
	h.task.Spec.Workspaces[0].Goal = "do something"
	h.task.Spec.Command = []string{"sh", "-c", "echo command >> " + events}

	exit := h.runUntilCommandExits(t)
	if exit.Err != nil || exit.ExitCode != 0 {
		t.Fatalf("unexpected exit: %+v", exit)
	}

	out, err := os.ReadFile(events)
	if err != nil {
		t.Fatalf("events file missing: %v", err)
	}
	if got := strings.TrimSpace(string(out)); got != "goal\ncommand" {
		t.Errorf("event order = %q, want the goal to finish before the command runs", got)
	}
}

func TestRun_SkipsGoalForCanonicalDuplicatePath(t *testing.T) {
	t.Setenv("AX_GOAL_AGENT", "claude")
	t.Setenv("ANTHROPIC_API_KEY", "test-key")
	t.Setenv("AX_BOOTSTRAP_TIMEOUT", "5s")
	events := filepath.Join(t.TempDir(), "events")
	bin := t.TempDir()
	script := `#!/bin/sh
echo goal >> ` + events + `
`
	if err := os.WriteFile(filepath.Join(bin, "claude"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))

	h := newHarness(t)
	// Two bindings naming the same real directory with textually distinct
	// paths; without canonicalizing before dedup, both would get their own
	// goal goroutine and race on the same goal marker.
	h.task.Spec.Workspaces[0].Goal = "do something"
	h.task.Spec.Workspaces = append(h.task.Spec.Workspaces, &v1alpha1.WorkspaceRef{
		Name: "ws-alias",
		Path: h.wsPath + "/.",
		Goal: "do something",
	})
	h.task.Spec.Command = []string{"true"}

	exit := h.runUntilCommandExits(t)
	if exit.Err != nil || exit.ExitCode != 0 {
		t.Fatalf("unexpected exit: %+v", exit)
	}

	out, err := os.ReadFile(events)
	if err != nil {
		t.Fatalf("events file missing: %v", err)
	}
	if got := strings.TrimSpace(string(out)); got != "goal" {
		t.Errorf("goal ran %d time(s) (%q), want exactly once for two aliased paths", strings.Count(got, "goal"), got)
	}
}

// TestRun_SerializesGoalsForOverlappingPaths covers two bindings whose paths
// are distinct but nested (one under the other): running their goals
// concurrently would let two agents edit or delete the same underlying
// files, since a descendant directory's contents are part of its ancestor's
// filesystem tree too.
func TestRun_SerializesGoalsForOverlappingPaths(t *testing.T) {
	t.Setenv("AX_GOAL_AGENT", "claude")
	t.Setenv("ANTHROPIC_API_KEY", "test-key")
	t.Setenv("AX_BOOTSTRAP_TIMEOUT", "5s")
	events := filepath.Join(t.TempDir(), "events")
	bin := t.TempDir()
	// Records a start/end pair bracketing a sleep, identified by cwd (which
	// the runner sets to the goal's workspace path), so the test can detect
	// whether two goals' windows ever overlapped.
	script := `#!/bin/sh
echo "start $(pwd)" >> ` + events + `
sleep 0.2
echo "end $(pwd)" >> ` + events + `
`
	if err := os.WriteFile(filepath.Join(bin, "claude"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))

	h := newHarness(t)
	h.task.Spec.Workspaces[0].Goal = "do something"
	h.task.Spec.Workspaces = append(h.task.Spec.Workspaces, &v1alpha1.WorkspaceRef{
		Name: "nested",
		Path: filepath.Join(h.wsPath, "nested"),
		Goal: "do something",
	})
	h.task.Spec.Command = []string{"true"}

	exit := h.runUntilCommandExits(t)
	if exit.Err != nil || exit.ExitCode != 0 {
		t.Fatalf("unexpected exit: %+v", exit)
	}

	out, err := os.ReadFile(events)
	if err != nil {
		t.Fatalf("events file missing: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) != 4 {
		t.Fatalf("got %d event lines, want 4 (start/end for each of 2 goals): %q", len(lines), lines)
	}
	open := 0
	for _, line := range lines {
		switch {
		case strings.HasPrefix(line, "start "):
			open++
			if open > 1 {
				t.Fatalf("goals overlapped: %q", lines)
			}
		case strings.HasPrefix(line, "end "):
			open--
		default:
			t.Fatalf("unexpected event line: %q", line)
		}
	}
}

// TestRun_SerializesGoalsAcrossTransitivelyOverlappingPaths covers three
// bindings where two (a, b) don't directly overlap each other but a third
// (the common parent) overlaps both: all three must land in one serial
// queue. Joining the parent to only the first match it finds, instead of
// merging every queue it overlaps, would let b's goal still run concurrently
// with the parent's.
func TestRun_SerializesGoalsAcrossTransitivelyOverlappingPaths(t *testing.T) {
	t.Setenv("AX_GOAL_AGENT", "claude")
	t.Setenv("ANTHROPIC_API_KEY", "test-key")
	t.Setenv("AX_BOOTSTRAP_TIMEOUT", "5s")
	events := filepath.Join(t.TempDir(), "events")
	bin := t.TempDir()
	script := `#!/bin/sh
echo "start $(pwd)" >> ` + events + `
sleep 0.2
echo "end $(pwd)" >> ` + events + `
`
	if err := os.WriteFile(filepath.Join(bin, "claude"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))

	h := newHarness(t)
	// a and b are siblings (no direct overlap); root (h.wsPath) is their
	// common ancestor and is placed last so it joins two already-separate
	// queues instead of starting the first one itself.
	h.task.Spec.Workspaces[0] = &v1alpha1.WorkspaceRef{
		Name: "a",
		Path: filepath.Join(h.wsPath, "a"),
		Goal: "do something",
	}
	h.task.Spec.Workspaces = append(h.task.Spec.Workspaces,
		&v1alpha1.WorkspaceRef{Name: "b", Path: filepath.Join(h.wsPath, "b"), Goal: "do something"},
		&v1alpha1.WorkspaceRef{Name: "root", Path: h.wsPath, Goal: "do something"},
	)
	h.task.Spec.Command = []string{"true"}

	exit := h.runUntilCommandExits(t)
	if exit.Err != nil || exit.ExitCode != 0 {
		t.Fatalf("unexpected exit: %+v", exit)
	}

	out, err := os.ReadFile(events)
	if err != nil {
		t.Fatalf("events file missing: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) != 6 {
		t.Fatalf("got %d event lines, want 6 (start/end for each of 3 goals): %q", len(lines), lines)
	}
	open := 0
	for _, line := range lines {
		switch {
		case strings.HasPrefix(line, "start "):
			open++
			if open > 1 {
				t.Fatalf("goals overlapped: %q", lines)
			}
		case strings.HasPrefix(line, "end "):
			open--
		default:
			t.Fatalf("unexpected event line: %q", line)
		}
	}
}

// TestRun_WorkspaceReadyDoesNotWaitForGoal covers /readyz?check=workspace: it
// must report ready as soon as file setup finishes, independent of a
// still-running goal. Goals can run far longer than the reconciler's own
// WorkspaceReadyTimeout poll window, and nothing currently re-triggers
// reconciliation once that poll gives up, so waiting for goals here would
// leave the condition stuck False long after the workspace actually
// finished setting up.
func TestRun_WorkspaceReadyDoesNotWaitForGoal(t *testing.T) {
	t.Setenv("AX_GOAL_AGENT", "claude")
	t.Setenv("ANTHROPIC_API_KEY", "test-key")
	t.Setenv("AX_BOOTSTRAP_TIMEOUT", "5s")
	bin := t.TempDir()
	script := "#!/bin/sh\nsleep 5\n"
	if err := os.WriteFile(filepath.Join(bin, "claude"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))

	h := newHarness(t)
	h.task.Spec.Workspaces[0].Goal = "do something"
	h.start(t)

	readyzURL := fmt.Sprintf("http://127.0.0.1:%d/readyz?check=workspace", h.cfg.Port)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(readyzURL)
		if err == nil {
			ok := resp.StatusCode == http.StatusOK
			resp.Body.Close()
			if ok {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("workspace never reported ready while its goal was still running (5s sleep)")
}

func TestRun_SkipsCommandAfterGoalCanceled(t *testing.T) {
	t.Setenv("AX_GOAL_AGENT", "claude")
	t.Setenv("ANTHROPIC_API_KEY", "test-key")
	t.Setenv("AX_BOOTSTRAP_TIMEOUT", "5s")
	events := filepath.Join(t.TempDir(), "events")
	bin := t.TempDir()
	// A goal agent that outlives the cancellation below; exec.CommandContext
	// kills it, so it never gets to write anything.
	script := `#!/bin/sh
sleep 5
echo goal >> ` + events + `
`
	if err := os.WriteFile(filepath.Join(bin, "claude"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))

	h := newHarness(t)
	h.task.Spec.Workspaces[0].Goal = "do something"
	h.task.Spec.Command = []string{"sh", "-c", "echo command >> " + events}
	h.start(t)

	// Give the goal a moment to actually start before cancelling, as a
	// suspend or delete arriving mid-goal would.
	time.Sleep(100 * time.Millisecond)
	h.cancel()
	h.waitFinished(t, 5*time.Second)

	if out, err := os.ReadFile(events); err == nil {
		t.Fatalf("events file should not exist (goal killed, command must not run after cancellation), got %q", out)
	} else if !os.IsNotExist(err) {
		t.Fatalf("unexpected error checking events file: %v", err)
	}
}

func TestRun_TaskEnvCannotDisableGitCredentialHelper(t *testing.T) {
	t.Setenv("GITHUB_TOKEN", "ghs_test_secret")
	h := newHarness(t)
	outFile := filepath.Join(t.TempDir(), "out.txt")
	// A task spec.env that happens to (or is crafted to) set these would, without
	// the fix, silently override the controller-managed credential helper for
	// the command process since os/exec keeps the last duplicate env value.
	h.task.Spec.Env = []*v1alpha1.EnvVar{
		{Name: "GITHUB_TOKEN", Value: "ghs_test_secret"},
		{Name: "GIT_CONFIG_COUNT", Value: "0"},
	}
	h.task.Spec.Command = []string{"sh", "-c", `echo "$GIT_CONFIG_COUNT" > ` + outFile}

	exit := h.runUntilCommandExits(t)
	if exit.Err != nil || exit.ExitCode != 0 {
		t.Fatalf("unexpected exit: %+v", exit)
	}

	out, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("command did not write its output: %v", err)
	}
	if got := strings.TrimSpace(string(out)); got != "2" {
		t.Errorf("GIT_CONFIG_COUNT in command env = %q, want the helper's own value (2), task env must not override it", got)
	}
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	dir := t.TempDir()

	origAXDir := workspace.AXDir
	workspace.AXDir = filepath.Join(dir, "ax")
	t.Cleanup(func() { workspace.AXDir = origAXDir })

	h := &harness{
		wsPath:   filepath.Join(dir, "workspace"),
		finished: make(chan struct{}),
		exited:   make(chan runner.CommandExit, 1),
	}
	h.task = &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "lib-task", Atespace: "default"},
		Spec:     &v1alpha1.TaskSpec{Workspaces: []*v1alpha1.WorkspaceRef{{Name: "ws", Path: h.wsPath}}},
	}
	h.cfg = runner.Config{
		Port:          freePort(t),
		Task:          h.task,
		OnCommandExit: func(e runner.CommandExit) { h.exited <- e },
	}
	return h
}

// start launches Run in the background. Cleanup cancels it and waits for it to
// return so no runner outlives its test.
func (h *harness) start(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	go func() {
		h.err = runner.Run(ctx, h.cfg)
		close(h.finished)
	}()
	t.Cleanup(func() {
		cancel()
		h.waitFinished(t, 15*time.Second)
	})
}

// waitFinished blocks until Run has returned and fails the test if it returned
// an error or did not return in time. It is safe to call more than once.
func (h *harness) waitFinished(t *testing.T, timeout time.Duration) {
	t.Helper()
	select {
	case <-h.finished:
		if h.err != nil {
			t.Fatalf("Run returned error: %v", h.err)
		}
	case <-time.After(timeout):
		t.Fatal("Run did not return in time")
	}
}

func (h *harness) waitExit(t *testing.T) runner.CommandExit {
	t.Helper()
	select {
	case e := <-h.exited:
		return e
	case <-time.After(10 * time.Second):
		t.Fatal("command did not exit in time")
		return runner.CommandExit{}
	}
}

// runUntilCommandExits starts the runner and blocks until the command has
// exited, leaving the runner itself alive.
func (h *harness) runUntilCommandExits(t *testing.T) runner.CommandExit {
	t.Helper()
	h.start(t)
	return h.waitExit(t)
}

func freePort(t *testing.T) int {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	return lis.Addr().(*net.TCPAddr).Port
}

func TestRun_SetsUpEveryWorkspace(t *testing.T) {
	h := newHarness(t)
	root := t.TempDir()
	codePath := filepath.Join(root, "code")
	toolsPath := filepath.Join(root, "tools")
	outFile := filepath.Join(root, "out.txt")

	h.task.Spec.Workspaces = []*v1alpha1.WorkspaceRef{
		{Name: "code", Path: codePath},
		{Name: "tools", Path: toolsPath},
	}
	h.cfg.Workspaces = []*v1alpha1.Workspace{
		{Metadata: &v1alpha1.ObjectMeta{Name: "tools"}, Spec: &v1alpha1.WorkspaceSpec{}},
		{Metadata: &v1alpha1.ObjectMeta{Name: "code"}, Spec: &v1alpha1.WorkspaceSpec{}},
	}
	h.task.Spec.Command = []string{"sh", "-c", "pwd > " + outFile}

	exit := h.runUntilCommandExits(t)
	if exit.Err != nil || exit.ExitCode != 0 {
		t.Fatalf("unexpected exit: %+v", exit)
	}

	// Both workspace directories exist and each has its own maiden-run marker.
	for _, p := range []string{codePath, toolsPath} {
		if st, err := os.Stat(p); err != nil || !st.IsDir() {
			t.Errorf("workspace dir %s missing: %v", p, err)
		}
		marker := filepath.Join(workspace.AXDir, workspace.MarkerName(p))
		if _, err := os.Stat(marker); err != nil {
			t.Errorf("marker %s missing: %v", marker, err)
		}
	}

	// The command ran in the first workspace.
	out, _ := os.ReadFile(outFile)
	wantDir, _ := filepath.EvalSymlinks(codePath)
	if gotDir, _ := filepath.EvalSymlinks(strings.TrimSpace(string(out))); gotDir != wantDir {
		t.Errorf("command cwd = %q, want first workspace %q", strings.TrimSpace(string(out)), codePath)
	}

	// Readiness reflects all workspaces, and the metadata server lists them in binding order.
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/readyz", h.cfg.Port))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("readyz = %d, want 200", resp.StatusCode)
	}
	resp, err = http.Get(fmt.Sprintf("http://127.0.0.1:%d/metadata/v1alpha1/ax/workspaces", h.cfg.Port))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "name: code") || !strings.Contains(string(body), "name: tools") || strings.Index(string(body), "name: code") > strings.Index(string(body), "name: tools") {
		t.Errorf("workspaces endpoint should list code then tools:\n%s", body)
	}
}
