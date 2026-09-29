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
	tests := map[string]string{
		"/workspace":        "initialized-workspace",
		"/workspace/":       "initialized-workspace",
		"":                  "initialized-workspace",
		"/":                 "initialized-root",
		"/workspace/tools":  "initialized-workspace-tools",
		"/workspace/a/b":    "initialized-workspace-a-b",
		"/srv/data":         "initialized-srv-data",
		"/workspace/tools/": "initialized-workspace-tools",
	}
	for path, want := range tests {
		if got := workspace.MarkerName(path); got != want {
			t.Errorf("MarkerName(%q) = %q, want %q", path, got, want)
		}
	}
}

func TestMarkerNameCollisionResistant(t *testing.T) {
	// These would collide under a naive "/" -> "-" substitution.
	pairs := [][2]string{
		{"/workspace/a-b", "/workspace/a/b"},
		{"/a--b", "/a-/b"},
	}
	for _, pair := range pairs {
		a, b := workspace.MarkerName(pair[0]), workspace.MarkerName(pair[1])
		if a == b {
			t.Errorf("MarkerName(%q) and MarkerName(%q) collide: both %q", pair[0], pair[1], a)
		}
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
	if _, err := os.Stat(filepath.Join(stateDir, workspace.MarkerName(path)+".goal")); !os.IsNotExist(err) {
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
	marker := filepath.Join(stateDir, workspace.MarkerName(path)+".goal")
	content, err := os.ReadFile(marker)
	if err != nil || !strings.Contains(string(content), "goal: create greeting") {
		t.Fatalf("goal marker missing or incorrect: %v", err)
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
	if _, err := os.Stat(filepath.Join(stateDir, workspace.MarkerName(path)+".goal")); err != nil {
		t.Fatalf("goal marker missing: %v", err)
	}
}
