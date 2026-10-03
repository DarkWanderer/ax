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

package workspace_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/ax/internal/workspace"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

func TestSetupWorkspace_MaidenRun(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "ax-ws-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	ws := &v1alpha1.Workspace{
		Metadata: &v1alpha1.ObjectMeta{
			Name: "test-ws",
		},
		Spec: &v1alpha1.WorkspaceSpec{
			Skills: &v1alpha1.SkillsConfig{
				Path: filepath.Join(tempDir, "skills"),
			},
		},
	}

	stateDir := filepath.Join(tempDir, "ax-state")
	origAXDir := workspace.AXDir
	workspace.AXDir = stateDir
	defer func() { workspace.AXDir = origAXDir }()

	// 1. Maiden run
	res1, err := workspace.SetupWorkspace(context.Background(), ws, tempDir, "Test maiden run setup")
	if err != nil {
		t.Fatalf("maiden setup failed: %v", err)
	}
	if !res1.IsMaidenRun {
		t.Errorf("expected maiden run to be true")
	}

	// Verify marker file exists under stateDir/initialized
	markerFile := filepath.Join(stateDir, workspace.MarkerName(tempDir))
	if _, err := os.Stat(markerFile); os.IsNotExist(err) {
		t.Errorf("expected marker file %s to exist", markerFile)
	}

	// Verify workspace directory has no .ax-initialized or .ax directory
	if _, err := os.Stat(filepath.Join(tempDir, ".ax-initialized")); !os.IsNotExist(err) {
		t.Errorf("expected .ax-initialized not to be in workspace")
	}
	if _, err := os.Stat(filepath.Join(tempDir, ".ax")); !os.IsNotExist(err) {
		t.Errorf("expected .ax directory not to be in workspace")
	}

	// 2. Second run (should detect already initialized)
	res2, err := workspace.SetupWorkspace(context.Background(), ws, tempDir, "")
	if err != nil {
		t.Fatalf("second setup failed: %v", err)
	}
	if res2.IsMaidenRun {
		t.Errorf("expected maiden run to be false on second run")
	}
}

func TestSetupWorkspace_GitSubdir(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "ax-ws-git-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a dummy source git repository
	srcDir, err := os.MkdirTemp("", "ax-src-git-*")
	if err != nil {
		t.Fatalf("failed to create src dir: %v", err)
	}
	defer os.RemoveAll(srcDir)

	// Init source repo
	// git init -b requires git 2.28+; symbolic-ref works on any version.
	initCmd := exec.Command("sh", "-c", "git init && git symbolic-ref HEAD refs/heads/main && git config user.email 'test@ax.io' && git config user.name 'AX' && echo 'hello' > README.md && git add . && git commit -m 'initial'")
	initCmd.Dir = srcDir
	if out, err := initCmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to init test git repo: %v (%s)", err, string(out))
	}

	ws := &v1alpha1.Workspace{
		Metadata: &v1alpha1.ObjectMeta{
			Name: "test-git-ws",
		},
		Spec: &v1alpha1.WorkspaceSpec{
			Git: []*v1alpha1.GitRepo{
				{
					Name: "ax",
					Repo: srcDir,
				},
			},
		},
	}

	stateDir := filepath.Join(tempDir, "ax-state")
	origAXDir := workspace.AXDir
	workspace.AXDir = stateDir
	defer func() { workspace.AXDir = origAXDir }()

	res, err := workspace.SetupWorkspace(context.Background(), ws, tempDir, "")
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	if len(res.ClonedRepos) != 1 {
		t.Errorf("expected 1 cloned repo, got %d", len(res.ClonedRepos))
	}

	// Verify repo was cloned into subdirectory 'ax' inside tempDir
	clonedGit := filepath.Join(tempDir, "ax", ".git")
	if _, err := os.Stat(clonedGit); os.IsNotExist(err) {
		t.Errorf("expected %s to exist", clonedGit)
	}

	// Verify git-success.log is created under stateDir and NOT inside workspace
	logFile := filepath.Join(stateDir, "git-success.log")
	if _, err := os.Stat(logFile); os.IsNotExist(err) {
		t.Errorf("expected git log file %s to exist in stateDir", logFile)
	}
	if _, err := os.Stat(filepath.Join(tempDir, ".ax-git-success.log")); !os.IsNotExist(err) {
		t.Errorf("expected .ax-git-success.log NOT to exist in workspace")
	}
}

func TestSetupWorkspace_GitError(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "ax-ws-git-err-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	ws := &v1alpha1.Workspace{
		Metadata: &v1alpha1.ObjectMeta{
			Name: "test-git-err-ws",
		},
		Spec: &v1alpha1.WorkspaceSpec{
			Git: []*v1alpha1.GitRepo{
				{
					Name: "invalid-repo",
					Repo: "https://127.0.0.1:9/nonexistent/repo.git",
				},
			},
		},
	}

	stateDir := filepath.Join(tempDir, "ax-state")
	origAXDir := workspace.AXDir
	workspace.AXDir = stateDir
	defer func() { workspace.AXDir = origAXDir }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, _ = workspace.SetupWorkspace(ctx, ws, tempDir, "")

	// Verify git-error.log is created under stateDir and NOT inside workspace
	errLog := filepath.Join(stateDir, "git-error.log")
	if _, err := os.Stat(errLog); os.IsNotExist(err) {
		t.Errorf("expected git error log %s to exist under stateDir", errLog)
	}

	// Verify workspace directory does not have .ax-git-error.log or .ax
	if _, err := os.Stat(filepath.Join(tempDir, ".ax-git-error.log")); !os.IsNotExist(err) {
		t.Errorf("expected .ax-git-error.log NOT to exist in workspace")
	}
	if _, err := os.Stat(filepath.Join(tempDir, ".ax")); !os.IsNotExist(err) {
		t.Errorf("expected .ax directory NOT to exist in workspace")
	}

	// Verify initialization marker was not created due to failure
	markerFile := filepath.Join(stateDir, workspace.MarkerName(tempDir))
	if _, err := os.Stat(markerFile); !os.IsNotExist(err) {
		t.Errorf("expected marker file %s NOT to exist after failure", markerFile)
	}
}

func TestSetupWorkspace_GitDepth(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "ax-ws-git-depth-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a dummy source git repository with 3 commits
	srcDir, err := os.MkdirTemp("", "ax-src-git-*")
	if err != nil {
		t.Fatalf("failed to create src dir: %v", err)
	}
	defer os.RemoveAll(srcDir)

	initCmd := exec.Command("sh", "-c", `
git init &&
git symbolic-ref HEAD refs/heads/main &&
git config user.email 'test@ax.io' &&
git config user.name 'AX' &&
echo '1' > file.txt && git add . && git commit -m 'commit 1' &&
echo '2' > file.txt && git add . && git commit -m 'commit 2' &&
echo '3' > file.txt && git add . && git commit -m 'commit 3'
`)
	initCmd.Dir = srcDir
	if out, err := initCmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to init test git repo with commits: %v (%s)", err, string(out))
	}

	ws := &v1alpha1.Workspace{
		Metadata: &v1alpha1.ObjectMeta{
			Name: "test-shallow-git-ws",
		},
		Spec: &v1alpha1.WorkspaceSpec{
			Git: []*v1alpha1.GitRepo{
				{
					Name:  "ax",
					Repo:  srcDir,
					Depth: 1,
				},
			},
		},
	}

	stateDir := filepath.Join(tempDir, "ax-state")
	origAXDir := workspace.AXDir
	workspace.AXDir = stateDir
	defer func() { workspace.AXDir = origAXDir }()

	res, err := workspace.SetupWorkspace(context.Background(), ws, tempDir, "")
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	if len(res.ClonedRepos) != 1 {
		t.Errorf("expected 1 cloned repo, got %d", len(res.ClonedRepos))
	}

	// Verify commit count in cloned repo is exactly 1
	countCmd := exec.Command("git", "rev-list", "--count", "HEAD")
	countCmd.Dir = filepath.Join(tempDir, "ax")
	out, err := countCmd.Output()
	if err != nil {
		t.Fatalf("failed to count commits: %v", err)
	}
	if got := string(out); got != "1\n" && got != "1" {
		t.Errorf("expected 1 commit with depth=1, got %q", got)
	}
}

func TestMarkerName(t *testing.T) {
	// Paths that normalize to the same clean form must produce the same name.
	equivalent := [][2]string{
		{"/workspace", "/workspace/"},
		{"/workspace", ""},
		{"/workspace/tools", "/workspace/tools/"},
	}
	for _, pair := range equivalent {
		a, b := workspace.MarkerName(pair[0]), workspace.MarkerName(pair[1])
		if a != b {
			t.Errorf("MarkerName(%q) = %q, MarkerName(%q) = %q, want equal", pair[0], a, pair[1], b)
		}
	}

	// A readable prefix should still be present for a human skimming AXDir.
	prefixes := map[string]string{
		"/workspace":       "initialized-workspace-",
		"/":                "initialized-root-",
		"/workspace/tools": "initialized-workspace-tools-",
		"/srv/data":        "initialized-srv-data-",
	}
	for path, prefix := range prefixes {
		if got := workspace.MarkerName(path); !strings.HasPrefix(got, prefix) {
			t.Errorf("MarkerName(%q) = %q, want prefix %q", path, got, prefix)
		}
	}
}

func TestMarkerNameCollisionResistant(t *testing.T) {
	// These would collide under a naive "/" -> "-" substitution, or under an
	// escape scheme that only doubles "-" before replacing "/".
	pairs := [][2]string{
		{"/workspace/a-b", "/workspace/a/b"},
		{"/a--b", "/a-/b"},
		{"/workspace/a-/b", "/workspace/a/-b"},
		// "/" trims to the same empty string that "root" substitutes for
		// display, so hashing after that substitution would make these two
		// distinct, valid paths indistinguishable.
		{"/", "/root"},
	}
	for _, pair := range pairs {
		a, b := workspace.MarkerName(pair[0]), workspace.MarkerName(pair[1])
		if a == b {
			t.Errorf("MarkerName(%q) and MarkerName(%q) collide: both %q", pair[0], pair[1], a)
		}
	}
}

func TestMarkerNameStaysWithinNAME_MAX(t *testing.T) {
	// A long but entirely valid nested path (each component well under 255
	// bytes) that would, without a bound on the readable prefix, flatten into
	// a single filename component over Linux's 255-byte NAME_MAX.
	segment := strings.Repeat("a", 40)
	path := "/" + strings.Repeat(segment+"/", 10)

	name := workspace.MarkerName(path)
	if len(name) > 255 {
		t.Fatalf("MarkerName length = %d, want <= 255 (NAME_MAX): %q", len(name), name)
	}
	// And the goal marker's own suffix must also fit.
	if got := len(name) + len(".goal"); got > 255 {
		t.Fatalf("MarkerName+.goal length = %d, want <= 255", got)
	}
}

func TestSetupWorkspace_InlinedFiles(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "ax-ws-files-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	ws := &v1alpha1.Workspace{
		Metadata: &v1alpha1.ObjectMeta{Name: "files-ws"},
		Spec: &v1alpha1.WorkspaceSpec{
			Files: []*v1alpha1.File{
				{Path: "config/settings.json", Content: `{"env": "production"}`},
				{Path: "notes.txt", Content: "Hello AX"},
			},
		},
	}

	stateDir := filepath.Join(tempDir, "ax-state")
	origAXDir := workspace.AXDir
	workspace.AXDir = stateDir
	defer func() { workspace.AXDir = origAXDir }()

	res, err := workspace.SetupWorkspace(context.Background(), ws, tempDir, "")
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	if !res.IsMaidenRun {
		t.Errorf("expected maiden run to be true")
	}

	configData, err := os.ReadFile(filepath.Join(tempDir, "config", "settings.json"))
	if err != nil {
		t.Fatalf("reading config/settings.json: %v", err)
	}
	if string(configData) != `{"env": "production"}` {
		t.Errorf("unexpected content in config/settings.json: %s", string(configData))
	}

	notesData, err := os.ReadFile(filepath.Join(tempDir, "notes.txt"))
	if err != nil {
		t.Fatalf("reading notes.txt: %v", err)
	}
	if string(notesData) != "Hello AX" {
		t.Errorf("unexpected content in notes.txt: %s", string(notesData))
	}
}

func TestRunGoalClaudeWithoutAnthropicKeyDoesNotUseGemini(t *testing.T) {
	t.Setenv("AX_GOAL_AGENT", "claude")
	t.Setenv("GEMINI_API_KEY", "gemini-test-key")
	t.Setenv("ANTHROPIC_API_KEY", "")
	stateDir := t.TempDir()
	origAXDir := workspace.AXDir
	workspace.AXDir = stateDir
	t.Cleanup(func() { workspace.AXDir = origAXDir })
	path := t.TempDir()
	if workspace.RunGoal(context.Background(), path, "write code") {
		t.Fatal("goal ran without Anthropic credentials")
	}
	if _, err := os.Stat(filepath.Join(stateDir, workspace.GoalMarkerName(path, "write code"))); !os.IsNotExist(err) {
		t.Fatalf("goal marker exists without completion: %v", err)
	}
}

func TestGoalPredatesSplit(t *testing.T) {
	stateDir := t.TempDir()
	origAXDir := workspace.AXDir
	workspace.AXDir = stateDir
	t.Cleanup(func() { workspace.AXDir = origAXDir })
	path := t.TempDir()

	if workspace.GoalPredatesSplit(path) {
		t.Fatal("no marker at all should not read as predating the split")
	}

	// A marker written by a pre-split runner: no goal_tracking line.
	legacyMarker := filepath.Join(stateDir, workspace.MarkerName(path))
	if err := os.WriteFile(legacyMarker, []byte("workspace: w\ninitialized_at: 2020-01-01T00:00:00Z\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if !workspace.GoalPredatesSplit(path) {
		t.Fatal("a marker without the split sentinel should read as predating the split")
	}

	// A marker written by this runner (via a real maiden-run setup) carries
	// the sentinel and must not be flagged.
	freshPath := filepath.Join(t.TempDir(), "ws2")
	if _, err := workspace.PrepareWorkspace(context.Background(), nil, freshPath); err != nil {
		t.Fatal(err)
	}
	if workspace.GoalPredatesSplit(freshPath) {
		t.Fatal("a marker written by this runner should not read as predating the split")
	}
}

// TestGoalPredatesSplit_LegacyMarkerName covers the actual migration scenario
// the legacy-compat shim remains safely useful for: a single-segment path (no
// "/" left after trimming, so its flattened legacy name can't collide with
// some other path's) initialized by a runner from before per-path marker
// names included a digest has its marker at the old, undigested name, which
// the current MarkerName no longer produces. GoalPredatesSplit must still
// find it there and treat the workspace as pre-split. (A nested or
// hyphenated path no longer gets this treatment at all -- see
// TestGoalPredatesSplit_AmbiguousLegacyMarkerNameNotTrusted -- since this
// function can't tell a genuine single-writer history apart from a collision
// with some other path without visibility into every workspace this task
// binds.)
func TestGoalPredatesSplit_LegacyMarkerName(t *testing.T) {
	stateDir := t.TempDir()
	origAXDir := workspace.AXDir
	workspace.AXDir = stateDir
	t.Cleanup(func() { workspace.AXDir = origAXDir })
	// A single-segment absolute path (no "/" or "-" in it, so it's eligible
	// for legacy trust). GoalPredatesSplit only ever touches AXDir, never
	// this path itself, so it need not exist on disk.
	path := "/legacysinglesegmentworkspace"

	legacyName := "initialized-" + strings.TrimPrefix(path, "/")
	if err := os.WriteFile(filepath.Join(stateDir, legacyName), []byte("workspace: w\ninitialized_at: 2020-01-01T00:00:00Z\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	if !workspace.GoalPredatesSplit(path) {
		t.Fatal("a marker at the pre-digest legacy name should read as predating the split")
	}
}

// TestGoalPredatesSplit_AmbiguousLegacyMarkerNameNotTrusted covers the
// pre-digest legacy scheme's inherent ambiguity: it substitutes "-" for every
// "/", so "/base/a-b" and "/base/a/b" both flatten to the same legacy marker
// name. A legacy marker at that name could actually have been written for
// either path, so a path whose own segments contain a literal "-" (like
// "/base/a-b") must not trust a same-named legacy marker as proof that IT was
// already initialized -- it may really belong to the distinct, slash-separated
// path it collides with.
func TestGoalPredatesSplit_AmbiguousLegacyMarkerNameNotTrusted(t *testing.T) {
	stateDir := t.TempDir()
	origAXDir := workspace.AXDir
	workspace.AXDir = stateDir
	t.Cleanup(func() { workspace.AXDir = origAXDir })
	base := t.TempDir()

	hyphenPath := filepath.Join(base, "a-b")
	slashPath := filepath.Join(base, "a", "b")

	// A legacy marker at the name both hyphenPath and slashPath flatten to
	// (written, in this scenario, for slashPath).
	legacyName := "initialized-" + strings.ReplaceAll(strings.Trim(slashPath, "/"), "/", "-")
	if err := os.WriteFile(filepath.Join(stateDir, legacyName), []byte("workspace: w\ninitialized_at: 2020-01-01T00:00:00Z\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// hyphenPath must not trust that marker as proof of its own prior
	// initialization: it may really belong to the colliding slashPath.
	if workspace.GoalPredatesSplit(hyphenPath) {
		t.Fatal("a legacy marker name ambiguous with another path was trusted")
	}
	res, err := workspace.PrepareWorkspace(context.Background(), nil, hyphenPath)
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsMaidenRun {
		t.Fatal("a path whose legacy marker name is ambiguous with another path skipped its maiden run")
	}
}

// TestGoalPredatesSplit_SlashPathDoesNotTrustCollidingLegacyMarker covers the
// other side of the same collision: the slash-separated path itself (e.g.
// "/base/a/b", colliding with "/base/a-b") contains no literal hyphen in any
// of its own segments, so rejecting legacy trust only for hyphenated paths
// would still let it compute and trust that same, collision-prone flattened
// name -- even though the marker there might genuinely have been written for
// the other, hyphenated path. legacyMarkerName has no visibility into sibling
// workspace paths to tell them apart, so any multi-segment path must decline
// legacy trust altogether, not just the hyphenated half of each pair.
func TestGoalPredatesSplit_SlashPathDoesNotTrustCollidingLegacyMarker(t *testing.T) {
	stateDir := t.TempDir()
	origAXDir := workspace.AXDir
	workspace.AXDir = stateDir
	t.Cleanup(func() { workspace.AXDir = origAXDir })
	base := t.TempDir()

	hyphenPath := filepath.Join(base, "a-b")
	slashPath := filepath.Join(base, "a", "b")

	// A legacy marker at the name both paths flatten to, written, in this
	// scenario, for hyphenPath.
	legacyName := "initialized-" + strings.ReplaceAll(strings.Trim(hyphenPath, "/"), "/", "-")
	if err := os.WriteFile(filepath.Join(stateDir, legacyName), []byte("workspace: w\ninitialized_at: 2020-01-01T00:00:00Z\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	if workspace.GoalPredatesSplit(slashPath) {
		t.Fatal("a legacy marker name ambiguous with another path was trusted by the slash-separated side")
	}
	res, err := workspace.PrepareWorkspace(context.Background(), nil, slashPath)
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsMaidenRun {
		t.Fatal("the slash-separated side of a colliding legacy marker name skipped its maiden run")
	}
}

// TestGoalPredatesSplit_RootSentinelLegacyMarkerNameNotTrusted covers another
// collision in the pre-digest legacy scheme, distinct from a literal "-" in a
// path segment: trimming the true root path "/" leaves an empty string, which
// the scheme substitutes the literal name "root" for -- the exact same name a
// sibling path of "/root" would itself trim to. A legacy marker actually
// written for one must not be mistaken for proof that the other was already
// initialized.
func TestGoalPredatesSplit_RootSentinelLegacyMarkerNameNotTrusted(t *testing.T) {
	stateDir := t.TempDir()
	origAXDir := workspace.AXDir
	workspace.AXDir = stateDir
	t.Cleanup(func() { workspace.AXDir = origAXDir })

	// A legacy marker at the name both "/" and "/root" flatten to (written,
	// in this scenario, for "/root").
	if err := os.WriteFile(filepath.Join(stateDir, "initialized-root"), []byte("workspace: w\ninitialized_at: 2020-01-01T00:00:00Z\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// "/" must not trust that marker as proof of its own prior
	// initialization: it may really belong to the colliding "/root".
	if workspace.GoalPredatesSplit("/") {
		t.Fatal("a root-sentinel legacy marker name ambiguous with /root was trusted for /")
	}
}

func TestMarkGoalHandledByLegacySetup(t *testing.T) {
	stateDir := t.TempDir()
	origAXDir := workspace.AXDir
	workspace.AXDir = stateDir
	t.Cleanup(func() { workspace.AXDir = origAXDir })
	path := t.TempDir()

	workspace.MarkGoalHandledByLegacySetup(path)

	if _, err := os.Stat(filepath.Join(stateDir, workspace.MarkerName(path)+".goal")); err != nil {
		t.Fatalf("goal marker missing after MarkGoalHandledByLegacySetup: %v", err)
	}
	// RunGoal must now treat the goal as already done and not run it again.
	if workspace.RunGoal(context.Background(), path, "some goal") {
		t.Fatal("RunGoal re-ran a goal already marked handled by legacy setup")
	}
}

func TestRunGoalClaudePassesPromptOnStdin(t *testing.T) {
	t.Setenv("AX_GOAL_AGENT", "claude")
	t.Setenv("ANTHROPIC_API_KEY", "test-key")
	t.Setenv("AX_BOOTSTRAP_TIMEOUT", "5s")
	bin := t.TempDir()
	script := `#!/bin/sh
case "$*" in
  *"create greeting"*) exit 2 ;;
esac
IFS= read -r prompt || true
[ "$prompt" = "create greeting" ]
`
	if err := os.WriteFile(filepath.Join(bin, "claude"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	stateDir := t.TempDir()
	origAXDir := workspace.AXDir
	workspace.AXDir = stateDir
	t.Cleanup(func() { workspace.AXDir = origAXDir })
	path := t.TempDir()
	if !workspace.RunGoal(context.Background(), path, "create greeting") {
		t.Fatal("Claude goal did not complete")
	}
	marker := filepath.Join(stateDir, workspace.GoalMarkerName(path, "create greeting"))
	content, err := os.ReadFile(marker)
	if err != nil || !strings.Contains(string(content), "goal: create greeting") {
		t.Fatalf("goal marker missing or incorrect: %v", err)
	}
}

// TestRunGoalDistinguishesAliasedGoals covers two accepted bindings that
// alias the same physical directory under different path spellings (here,
// "<dir>" and "<dir>/.", both normalized to the same canonical path by
// filepath.Clean) but specify different goals. The runner's queue grouping
// keeps both bindings queued rather than dropping the second as a duplicate,
// so RunGoal must still be able to tell them apart: a path-only completion
// marker would let the first goal's marker make RunGoal silently skip the
// second binding's distinct goal.
func TestRunGoalDistinguishesAliasedGoals(t *testing.T) {
	t.Setenv("AX_GOAL_AGENT", "claude")
	t.Setenv("ANTHROPIC_API_KEY", "test-key")
	t.Setenv("AX_BOOTSTRAP_TIMEOUT", "5s")
	bin := t.TempDir()
	script := `#!/bin/sh
exit 0
`
	if err := os.WriteFile(filepath.Join(bin, "claude"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	stateDir := t.TempDir()
	origAXDir := workspace.AXDir
	workspace.AXDir = stateDir
	t.Cleanup(func() { workspace.AXDir = origAXDir })

	path := t.TempDir()
	alias := path + "/."

	if !workspace.RunGoal(context.Background(), path, "goal one") {
		t.Fatal("first binding's goal did not run")
	}
	if !workspace.RunGoal(context.Background(), alias, "goal two") {
		t.Fatal("second binding's distinct goal was skipped as already completed")
	}

	m1 := filepath.Join(stateDir, workspace.GoalMarkerName(path, "goal one"))
	m2 := filepath.Join(stateDir, workspace.GoalMarkerName(alias, "goal two"))
	c1, err := os.ReadFile(m1)
	if err != nil || !strings.Contains(string(c1), "goal: goal one") {
		t.Fatalf("marker for goal one missing or incorrect: %v", err)
	}
	c2, err := os.ReadFile(m2)
	if err != nil || !strings.Contains(string(c2), "goal: goal two") {
		t.Fatalf("marker for goal two missing or incorrect: %v", err)
	}
}

// TestRunGoalClaude_KillsToolSubprocessOnTimeout covers AX_BOOTSTRAP_TIMEOUT
// firing while one of Claude's allowed Bash tools still has a child process
// running: exec.CommandContext's default cancellation kills only the claude
// process itself, leaving such a child orphaned and able to keep modifying
// the workspace (or otherwise running) well after RunGoal returns. claude
// must run in its own process group so the whole group, including any such
// child, is killed on timeout.
func TestRunGoalClaude_KillsToolSubprocessOnTimeout(t *testing.T) {
	t.Setenv("AX_GOAL_AGENT", "claude")
	t.Setenv("ANTHROPIC_API_KEY", "test-key")
	t.Setenv("AX_BOOTSTRAP_TIMEOUT", "300ms")
	bin := t.TempDir()
	childMarker := filepath.Join(t.TempDir(), "child-completed")
	// Simulates claude leaving a Bash-tool child process running (sleeping
	// longer than the bootstrap timeout) while claude itself also hangs past
	// the timeout.
	script := `#!/bin/sh
(sleep 2; echo done > ` + childMarker + `) &
sleep 10
`
	if err := os.WriteFile(filepath.Join(bin, "claude"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	stateDir := t.TempDir()
	origAXDir := workspace.AXDir
	workspace.AXDir = stateDir
	t.Cleanup(func() { workspace.AXDir = origAXDir })
	path := t.TempDir()

	if workspace.RunGoal(context.Background(), path, "do something") {
		t.Fatal("RunGoal reported success for a goal that timed out")
	}

	// Give the child process (sleeping 2s) ample time to have written its
	// marker, were it not killed along with claude's process group.
	time.Sleep(3 * time.Second)
	if _, err := os.Stat(childMarker); err == nil {
		t.Fatal("tool subprocess survived claude's timeout and kept running")
	}
}

func TestRunGoalClaudeUsesGatewayTokenAndModel(t *testing.T) {
	t.Setenv("AX_GOAL_AGENT", "claude")
	t.Setenv("ANTHROPIC_API_KEY", "")
	t.Setenv("ANTHROPIC_AUTH_TOKEN", "gateway-token")
	t.Setenv("AX_CLAUDE_MODEL", "openrouter/free")
	t.Setenv("AX_BOOTSTRAP_TIMEOUT", "5s")
	bin := t.TempDir()
	script := `#!/bin/sh
case " $* " in
  *" --model openrouter/free "*) ;;
  *) exit 1 ;;
esac
IFS= read -r prompt || true
[ "$prompt" = "create greeting" ]
`
	if err := os.WriteFile(filepath.Join(bin, "claude"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	stateDir := t.TempDir()
	origAXDir := workspace.AXDir
	workspace.AXDir = stateDir
	t.Cleanup(func() { workspace.AXDir = origAXDir })
	path := t.TempDir()
	if !workspace.RunGoal(context.Background(), path, "create greeting") {
		t.Fatal("Claude gateway goal did not complete")
	}
	if _, err := os.Stat(filepath.Join(stateDir, workspace.GoalMarkerName(path, "create greeting"))); err != nil {
		t.Fatalf("goal marker missing: %v", err)
	}
}
