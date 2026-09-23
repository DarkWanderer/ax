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

package workspace

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/ax/pkg/apis/v1alpha1"
)

const (
	// DefaultAXDir is the default directory inside the container used for AX system state and logs.
	DefaultAXDir = "/ax"
	// InitializedMarkerFilename is the base name of the marker file written after a successful maiden run.
	InitializedMarkerFilename = "initialized"
	// goalMarkerSuffix is appended to a workspace's marker name for the file
	// recording that its goal was carried out. The goal is tracked separately
	// because it runs after the workspace is otherwise prepared.
	goalMarkerSuffix = ".goal"

	defaultWorkspacePath = "/workspace"
	defaultBranch        = "main"
	defaultRepoDirName   = "repo"

	gitSuccessLog = "git-success.log"
	gitErrorLog   = "git-error.log"
	gitRetries    = 5
	gitRetryDelay = 2 * time.Second

	bootstrapScriptPath = "/usr/local/bin/antigravity_bootstrap.py"
	// bootstrapAPIKeyEnv must be set for the Antigravity agent to run.
	bootstrapAPIKeyEnv = "GEMINI_API_KEY"
	goalAgentEnv       = "AX_GOAL_AGENT"
	claudeAPIKeyEnv    = "ANTHROPIC_API_KEY"
	// bootstrapTimeoutEnv overrides the default bootstrap timeout with a Go duration string.
	bootstrapTimeoutEnv = "AX_BOOTSTRAP_TIMEOUT"
	// bootstrapDataDir, under AXDir, is where the agent keeps its own state so
	// conversation logs and caches stay out of the workspace.
	bootstrapDataDir = "antigravity"
	// defaultBootstrapTimeout allows the agent enough time to install toolchains and
	// dependencies. The goal runs after the sandbox reports ready.
	defaultBootstrapTimeout = 10 * time.Minute

	dirPerm  = 0o755
	filePerm = 0o644
)

// AXDir is the directory inside the container used for AX system state and logs.
// Tests override it to keep state out of the workspace.
var AXDir = DefaultAXDir

// SetupResult contains details about the workspace maiden initialization.
type SetupResult struct {
	WorkspacePath string
	ClonedRepos   []string
	SkillsMounted string
	IsMaidenRun   bool
	// BootstrapRan reports whether the Antigravity agent ran to completion for the goal.
	BootstrapRan bool
}

// SetupWorkspace prepares a workspace and then carries out its goal, in that order.
// Callers that serve a readiness endpoint should use PrepareWorkspace and RunGoal
// directly instead: an agent working towards a goal needs the network, and a sandbox
// gets none until it reports ready.
func SetupWorkspace(ctx context.Context, ws *v1alpha1.Workspace, targetPath string, goal string) (*SetupResult, error) {
	res, err := PrepareWorkspace(ctx, ws, targetPath)
	if err != nil {
		return res, err
	}
	res.BootstrapRan = RunGoal(ctx, targetPath, goal)
	return res, nil
}

// PrepareWorkspace prepares the workspace directory on its maiden run: it clones the
// declared Git repositories and creates the skills path. A marker file under AXDir
// records a completed maiden run so subsequent calls are no-ops. The workspace's goal
// is not part of this; see RunGoal.
//
// Git failures are logged and recorded under AXDir but do not abort setup. The marker
// is withheld in that case so the next start retries the clone.
func PrepareWorkspace(ctx context.Context, ws *v1alpha1.Workspace, targetPath string) (*SetupResult, error) {
	if targetPath == "" {
		targetPath = defaultWorkspacePath
	}
	// Tooling downstream (git, the Antigravity agent) scopes itself to this path,
	// so it must be absolute.
	if abs, err := filepath.Abs(targetPath); err == nil {
		targetPath = abs
	}
	res := &SetupResult{WorkspacePath: targetPath}

	if err := os.MkdirAll(targetPath, dirPerm); err != nil {
		return nil, fmt.Errorf("creating workspace directory %s: %w", targetPath, err)
	}
	if err := os.MkdirAll(AXDir, dirPerm); err != nil {
		slog.Warn("creating ax state directory", "dir", AXDir, "error", err)
	}

	markerPath := filepath.Join(AXDir, MarkerName(targetPath))
	if _, err := os.Stat(markerPath); err == nil {
		slog.Info("workspace already initialized; skipping maiden run setup", "path", targetPath)
		return res, nil
	}

	slog.Info("executing workspace maiden run setup", "path", targetPath)
	res.IsMaidenRun = true

	gitOK := true
	if ws != nil && ws.Spec != nil {
		res.ClonedRepos, gitOK = cloneRepos(ctx, ws.Spec.Git, targetPath)
		res.SkillsMounted = setupSkills(ws.Spec.Skills)
	}

	if !gitOK {
		slog.Warn("maiden run workspace setup completed with errors; marker omitted to allow retry", "path", targetPath)
		return res, nil
	}

	writeMarker(markerPath, ws)
	slog.Info("maiden run workspace setup completed successfully", "path", targetPath)
	return res, nil
}

// RunGoal hands a workspace's goal to the agent, once. It reports whether the agent
// ran to completion, recording that in its own marker file under AXDir so a later boot
// does not repeat work the agent already did. A goal that could have run and did not --
// an unreachable model, a failed agent -- leaves no marker and is attempted again on
// the next boot.
//
// This must run after the sandbox reports ready. Substrate's egress proxy only carries
// traffic for an actor its control plane considers running, so an agent started during
// workspace preparation cannot reach its model at all.
func RunGoal(ctx context.Context, targetPath, goal string) bool {
	if goal == "" {
		return false
	}
	if targetPath == "" {
		targetPath = defaultWorkspacePath
	}
	if abs, err := filepath.Abs(targetPath); err == nil {
		targetPath = abs
	}

	markerPath := filepath.Join(AXDir, MarkerName(targetPath)+goalMarkerSuffix)
	if _, err := os.Stat(markerPath); err == nil {
		slog.Info("workspace goal already carried out; skipping", "path", targetPath)
		return false
	}

	ran, _ := runBootstrap(ctx, goal, targetPath)
	if !ran {
		return false
	}

	if err := os.WriteFile(markerPath, []byte(fmt.Sprintf("goal: %s\ncompleted_at: %s\n",
		goal, time.Now().UTC().Format(time.RFC3339))), filePerm); err != nil {
		slog.Warn("failed to write goal marker file", "path", markerPath, "error", err)
	}
	return true
}

// cloneRepos fetches each declared repository into the workspace. It returns the
// repositories that succeeded and whether every clone succeeded.
func cloneRepos(ctx context.Context, repos []*v1alpha1.GitRepo, targetPath string) ([]string, bool) {
	var cloned []string
	ok := true
	for _, repo := range repos {
		if repo == nil || repo.Repo == "" {
			continue
		}

		dest := cloneDestination(repo, targetPath)
		if err := os.MkdirAll(dest, dirPerm); err != nil {
			slog.Warn("failed to create destination dir", "dir", dest, "error", err)
		}

		branch := repo.Branch
		if branch == "" {
			branch = defaultBranch
		}

		slog.Info("initializing and fetching git repo", "repo", repo.Repo, "branch", branch, "dir", dest, "depth", repo.Depth)
		out, err := retry(ctx, gitRetries, gitRetryDelay, func() ([]byte, error) {
			return fetchRepo(ctx, dest, repo.Repo, branch, repo.Depth)
		})
		if err != nil {
			ok = false
			slog.Warn("git fetch error (continuing setup)", "repo", repo.Repo, "error", err, "output", string(out))
			writeStateFile(gitErrorLog, fmt.Appendf(nil, "error: %v\noutput: %s\n", err, out))
			continue
		}

		cloned = append(cloned, repo.Repo)
		writeStateFile(gitSuccessLog, out)
	}
	return cloned, ok
}

// cloneDestination resolves where a repository is checked out.
//
// An explicit Dir wins: "." means the workspace root, an absolute path is used as-is,
// and anything else is relative to the workspace. Otherwise the directory is named after
// the repository's Name, unless that is empty or a generic placeholder such as "origin",
// in which case the name is derived from the repository URL.
func cloneDestination(repo *v1alpha1.GitRepo, targetPath string) string {
	if repo.Dir != "" {
		switch {
		case repo.Dir == ".":
			return targetPath
		case filepath.IsAbs(repo.Dir):
			return repo.Dir
		default:
			return filepath.Join(targetPath, repo.Dir)
		}
	}

	name := repo.Name
	if name == "" || name == defaultRepoDirName || name == "origin" {
		if derived := RepoDirName(repo.Repo); derived != "" {
			name = derived
		} else if name == "" {
			name = defaultRepoDirName
		}
	}
	return filepath.Join(targetPath, name)
}

// fetchRepo initializes dir as a git repository pointed at repoURL, fetches branch,
// and checks it out. If depth > 0, it performs a shallow fetch. It is idempotent so it
// can be retried against a partially initialized directory. Arguments are passed
// directly to git, never through a shell.
func fetchRepo(ctx context.Context, dir, repoURL, branch string, depth int32) ([]byte, error) {
	var combined []byte
	run := func(args ...string) error {
		cmd := exec.CommandContext(ctx, "git", args...)
		cmd.Dir = dir
		out, err := cmd.CombinedOutput()
		combined = append(combined, out...)
		if err != nil {
			return fmt.Errorf("git %s: %w", args[0], err)
		}
		return nil
	}

	if err := run("init"); err != nil {
		return combined, err
	}
	// A retry lands on an already initialized directory, so fall back to updating the remote.
	if err := run("remote", "add", "origin", repoURL); err != nil {
		if err := run("remote", "set-url", "origin", repoURL); err != nil {
			return combined, err
		}
	}
	fetchArgs := []string{"fetch"}
	if depth > 0 {
		fetchArgs = append(fetchArgs, fmt.Sprintf("--depth=%d", depth))
	}
	fetchArgs = append(fetchArgs, "origin", branch)
	if err := run(fetchArgs...); err != nil {
		return combined, err
	}
	if err := run("checkout", "-f", "FETCH_HEAD"); err != nil {
		return combined, err
	}
	return combined, nil
}

// retry runs op up to attempts times, waiting delay between failures. It stops early
// when ctx is done and never sleeps after the final attempt.
func retry(ctx context.Context, attempts int, delay time.Duration, op func() ([]byte, error)) ([]byte, error) {
	var (
		out []byte
		err error
	)
	for attempt := 1; attempt <= attempts; attempt++ {
		out, err = op()
		if err == nil {
			return out, nil
		}
		if attempt == attempts {
			break
		}
		slog.Warn("git operation failed, will retry", "attempt", attempt, "max", attempts, "error", err, "output", string(out))
		select {
		case <-ctx.Done():
			return out, errors.Join(err, ctx.Err())
		case <-time.After(delay):
		}
	}
	return out, err
}

// setupSkills creates the skills directory if one is declared and returns its path.
func setupSkills(skills *v1alpha1.SkillsConfig) string {
	if skills == nil || skills.Path == "" {
		return ""
	}
	if err := os.MkdirAll(skills.Path, dirPerm); err != nil {
		slog.Warn("creating skills dir", "path", skills.Path, "error", err)
	}
	return skills.Path
}

// runBootstrap hands the goal to the Antigravity agent so it can prepare the workspace.
// It reports whether the agent completed the goal, and whether a later boot should
// retry. The default agent needs the bootstrap script and a Gemini API key. Missing
// prerequisites skip the goal; an agent failure leaves it for the next boot.
func runBootstrap(ctx context.Context, goal, targetPath string) (ran bool, retry bool) {
	if os.Getenv(goalAgentEnv) == "claude" {
		return runClaudeBootstrap(ctx, goal, targetPath)
	}
	if _, err := os.Stat(bootstrapScriptPath); err != nil {
		slog.Info("Antigravity bootstrap script not installed; skipping", "script", bootstrapScriptPath)
		return false, false
	}
	if os.Getenv(bootstrapAPIKeyEnv) == "" {
		slog.Warn("workspace goal set but no API key available; skipping Antigravity bootstrap", "env", bootstrapAPIKeyEnv)
		return false, false
	}

	timeout := bootstrapTimeout()
	slog.Info("invoking Antigravity workspace bootstrap with goal", "goal", goal, "dir", targetPath, "timeout", timeout)
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	dataDir := filepath.Join(AXDir, bootstrapDataDir)
	if err := os.MkdirAll(dataDir, dirPerm); err != nil {
		slog.Warn("creating Antigravity data dir", "dir", dataDir, "error", err)
	}

	cmd := exec.CommandContext(ctx, "python3", bootstrapScriptPath,
		"--goal", goal,
		"--workspace", targetPath,
		"--data-dir", dataDir,
	)
	cmd.Dir = targetPath
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			slog.Warn("Antigravity bootstrap timed out (continuing)", "timeout", timeout)
		} else {
			slog.Warn("Antigravity bootstrap failed (continuing)", "error", err)
		}
		return false, true
	}
	slog.Info("Antigravity bootstrap completed successfully")
	return true, false
}

// runClaudeBootstrap uses Claude Code inside the task sandbox after egress is ready.
func runClaudeBootstrap(ctx context.Context, goal, targetPath string) (ran bool, retry bool) {
	if os.Getenv(claudeAPIKeyEnv) == "" {
		slog.Warn("workspace goal set but no Claude API key available", "env", claudeAPIKeyEnv)
		return false, false
	}
	if _, err := exec.LookPath("claude"); err != nil {
		slog.Warn("Claude Code executable not installed", "error", err)
		return false, false
	}
	timeout := bootstrapTimeout()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	// gVisor disallows setuid in this sandbox. Allow only the tools this coding
	// goal needs and decline any other permission request without prompting.
	cmd := exec.CommandContext(ctx, "claude", "--print", "--permission-mode", "dontAsk",
		"--permission-prompts", "none", "--allowedTools", "Bash,Edit,Write,Read,Glob,Grep", goal)
	cmd.Dir = targetPath
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			slog.Warn("Claude workspace goal timed out", "timeout", timeout)
		} else {
			slog.Warn("Claude workspace goal failed", "error", err)
		}
		return false, true
	}
	slog.Info("Claude workspace goal completed successfully")
	return true, false
}

// bootstrapTimeout returns the configured bootstrap timeout, falling back to the default
// when the override is unset or unparsable.
func bootstrapTimeout() time.Duration {
	raw := os.Getenv(bootstrapTimeoutEnv)
	if raw == "" {
		return defaultBootstrapTimeout
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		slog.Warn("invalid bootstrap timeout; using default", "env", bootstrapTimeoutEnv, "value", raw, "default", defaultBootstrapTimeout)
		return defaultBootstrapTimeout
	}
	return d
}

// MarkerName returns the maiden-run marker file name for a workspace mounted at
// path. The name is derived from the path so several workspaces in one
// container track their setup independently.
func MarkerName(path string) string {
	if path == "" {
		path = defaultWorkspacePath
	}
	clean := strings.Trim(filepath.Clean(path), "/")
	if clean == "" {
		clean = "root"
	}
	return InitializedMarkerFilename + "-" + strings.ReplaceAll(clean, "/", "-")
}

// writeMarker records a completed maiden run.
func writeMarker(path string, ws *v1alpha1.Workspace) {
	name := "unknown"
	if ws != nil && ws.GetMetadata() != nil && ws.GetMetadata().GetName() != "" {
		name = ws.GetMetadata().GetName()
	}
	content := fmt.Sprintf("workspace: %s\ninitialized_at: %s\n", name, time.Now().UTC().Format(time.RFC3339))
	if err := os.WriteFile(path, []byte(content), filePerm); err != nil {
		slog.Warn("failed to write initialized marker file", "path", path, "error", err)
	}
}

// writeStateFile writes a diagnostic file under AXDir, logging rather than failing on error.
func writeStateFile(name string, data []byte) {
	path := filepath.Join(AXDir, name)
	if err := os.WriteFile(path, data, filePerm); err != nil {
		slog.Warn("failed to write state file", "path", path, "error", err)
	}
}

// RepoDirName extracts the repository name from a git URL or local path.
// For example "https://github.com/chalk/chalk.git" yields "chalk".
func RepoDirName(repoURL string) string {
	trimmed := strings.TrimSuffix(strings.TrimSpace(repoURL), "/")
	trimmed = strings.TrimSuffix(trimmed, ".git")
	idx := strings.LastIndexAny(trimmed, "/:")
	if idx < 0 {
		return trimmed
	}
	return trimmed[idx+1:]
}
