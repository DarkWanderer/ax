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

package controller

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/ax/internal/credentials"
	"github.com/google/ax/internal/model"
	"github.com/google/ax/internal/substrate"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v3"
)

const (
	geminiSecretName     = "gemini-api-secret"
	geminiSecretKey      = "GEMINI_API_KEY"
	claudeAgentEnv       = "AX_GOAL_AGENT"
	claudeAgent          = "claude"
	anthropicSecretName  = "anthropic-api-secret"
	anthropicSecretKey   = "ANTHROPIC_API_KEY"
	openRouterProvider   = "openrouter"
	openRouterSecretName = "openrouter-api-secret"
	openRouterSecretKey  = "OPENROUTER_API_KEY"
	// secretLookupTimeout bounds the Kubernetes secret lookup so a slow or
	// unreachable cluster cannot stall reconciliation.
	secretLookupTimeout = 2 * time.Second
	// templateDigestBytes is how many bytes of the spec digest go into a
	// per-task ActorTemplate name.
	templateDigestBytes = 4
	// defaultWorkspaceReadyTimeout is how long Reconcile waits for a freshly
	// resumed actor to finish workspace setup.
	defaultWorkspaceReadyTimeout = 15 * time.Second
	workspaceReadyPollInterval   = 500 * time.Millisecond
	// cleanupRevokeTimeout bounds a compensating token revoke after some other
	// step failed. It deliberately does not reuse ctx: ctx may be the reason
	// the other step failed (canceled, deadline exceeded), and revoking the
	// only copy of a freshly minted token must not be skipped just because the
	// caller's context is now dead.
	cleanupRevokeTimeout = 5 * time.Second
)

// SecretResolver looks up a key from a Kubernetes secret in the given namespace.
type SecretResolver func(ctx context.Context, namespace, secretName, key string) (string, error)

type InstallationTokens interface {
	Mint(context.Context, *v1alpha1.GitHubAppCredential, string) (string, error)
	Revoke(context.Context, string) error
}

// TaskReconciler reconciles Task resources by provisioning and orchestrating
// sandboxed Actors on Agent Substrate.
type TaskReconciler struct {
	client                  *substrate.Client
	httpClient              *http.Client
	defaultTemplate         string
	defaultTemplateAtespace string

	// SecretResolver resolves task API keys for task containers. It defaults
	// to the Kubernetes secret lookup; tests replace it to avoid touching a cluster.
	SecretResolver     SecretResolver
	InstallationTokens InstallationTokens

	// WorkspaceReadyTimeout bounds how long Reconcile waits for the actor's workspace
	// to report ready before recording it as still initializing.
	WorkspaceReadyTimeout time.Duration
}

// NewTaskReconciler creates a new TaskReconciler.
func NewTaskReconciler(client *substrate.Client, defaultTemplate, defaultTemplateAtespace string) *TaskReconciler {
	if defaultTemplate == "" {
		defaultTemplate = "default-template"
	}
	if defaultTemplateAtespace == "" {
		defaultTemplateAtespace = "ax-system"
	}
	return &TaskReconciler{
		client:                  client,
		httpClient:              &http.Client{Timeout: 2 * time.Second},
		defaultTemplate:         defaultTemplate,
		defaultTemplateAtespace: defaultTemplateAtespace,
		SecretResolver:          model.GetKubernetesSecret,
		InstallationTokens:      &credentials.GitHubClient{},
		WorkspaceReadyTimeout:   defaultWorkspaceReadyTimeout,
	}
}

// Reconcile handles the reconciliation loop for a single Task.
func (r *TaskReconciler) Reconcile(ctx context.Context, task *v1alpha1.Task, workspaces ...*v1alpha1.Workspace) (*v1alpha1.Task, error) {
	return r.ReconcileWithProvider(ctx, task, nil, workspaces...)
}

func (r *TaskReconciler) ReconcileWithProvider(ctx context.Context, task *v1alpha1.Task, provider *v1alpha1.CredentialProvider, workspaces ...*v1alpha1.Workspace) (*v1alpha1.Task, error) {
	if task.Metadata == nil {
		task.Metadata = &v1alpha1.ObjectMeta{}
	}
	if task.Status == nil {
		task.Status = &v1alpha1.TaskStatus{}
	}
	atespace := task.Metadata.Atespace
	if atespace == "" {
		atespace = "default"
	}
	task.Metadata.Atespace = atespace

	if task.Spec == nil {
		task.Spec = &v1alpha1.TaskSpec{}
	}
	if task.Spec.Image == "" {
		task.Spec.Image = v1alpha1.DefaultTaskImage
	}

	slog.Info("reconciling task", "name", task.Metadata.Name, "atespace", atespace, "image", task.Spec.Image)

	now := time.Now()

	// 1. Ensure the Atespace exists in Substrate
	if err := r.client.EnsureAtespace(ctx, atespace); err != nil {
		r.setCondition(task, "Ready", "False", "AtespaceCreationFailed", err.Error(), now)
		task.Status.Phase = "Failed"
		return task, fmt.Errorf("ensuring atespace: %w", err)
	}

	// 2. The actor is always named after the task, so the two can be used
	// interchangeably (for example in the router's ate-target-actor header).
	// Whatever a client put in status.actor is overwritten.
	actorName := task.Metadata.Name
	task.Status.Actor = actorName
	if task.Status.Id == "" {
		task.Status.Id = fmt.Sprintf("task-%s-%d", task.Metadata.Name, now.Unix())
	}
	// ref, not provider, is what says this task uses a credential provider: the
	// provider object itself may be unavailable (deleted, or failing validation
	// after an edit) while an already-provisioned actor still needs to be
	// suspended and have its baked-in token revoked.
	ref := task.Spec.GetCredentialProvider()
	var existingActor *ateapipb.Actor
	if ref != nil {
		actor, err := r.client.GetActor(ctx, atespace, actorName)
		if err != nil && status.Code(err) != codes.NotFound {
			return r.credentialFailure(task, "could not inspect actor", now)
		}
		existingActor = actor
	}
	taskSuspending := task.Status.Phase == "Suspended"
	// Suspending an actor that already exists only needs the token already baked
	// into it; it does not mint or need workspace access, so a credential-provider
	// update or deletion must never block it.
	suspendingExisting := taskSuspending && existingActor != nil

	if ref != nil && !suspendingExisting {
		if provider == nil || provider.GetMetadata().GetName() != ref.GetName() || provider.GetMetadata().GetAtespace() != atespace {
			return r.credentialFailure(task, "credential provider is missing from the task atespace", now)
		}
		if err := v1alpha1.ValidateCredentialProvider(provider); err != nil {
			return r.credentialFailure(task, err.Error(), now)
		}
		if err := validateCredentialedWorkspaces(provider, workspaces); err != nil {
			return r.credentialFailure(task, err.Error(), now)
		}
	}
	if ref != nil {
		if existingActor != nil && (existingActor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_RUNNING || existingActor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_RESUMING) && !taskSuspending {
			token, err := r.actorToken(ctx, existingActor)
			if err != nil || token == "" {
				return r.credentialFailure(task, "running actor has no GitHub installation token; suspend and resume the task", now)
			}
		}
		// Only an already-provisioned actor needs an explicit suspend-and-revoke;
		// a task that starts out suspended (fresh create) still needs its actor
		// provisioned below, and gets suspended (and its freshly minted token
		// revoked) by the generic phase handling.
		if suspendingExisting {
			if err := r.suspendAndRevoke(ctx, atespace, actorName, existingActor); err != nil {
				return r.credentialFailure(task, err.Error(), now)
			}
			task.Status.WorkerIp = ""
			task.Status.Phase = "Suspended"
			r.setCondition(task, condReady, "False", "TaskSuspended", "Task is suspended", now)
			return task, nil
		}
	}

	// 3. Ensure Actor exists on Substrate
	templateName := r.defaultTemplate
	templateAtespace := r.defaultTemplateAtespace
	if parts := strings.SplitN(templateName, "/", 2); len(parts) == 2 {
		templateAtespace = parts[0]
		templateName = parts[1]
	}

	// Prepare container environment variables (task env + credentials + specs)
	extraEnv := make(map[string]string)
	if task.Spec != nil {
		for _, e := range task.Spec.Env {
			if e.Name != "" {
				extraEnv[e.Name] = e.Value
			}
		}
	}
	newToken := ""
	if provider != nil {
		state := existingActor.GetStatus().GetState()
		if existingActor == nil || (state != ateapipb.ActorState_ACTOR_STATE_RUNNING && state != ateapipb.ActorState_ACTOR_STATE_RESUMING) {
			// Rotating a suspended (or crashed) actor's token: revoke whatever it
			// still holds first, so a resume never leaves two live tokens behind.
			if existingActor != nil {
				if oldToken, err := r.actorToken(ctx, existingActor); err != nil {
					return r.credentialFailure(task, fmt.Sprintf("could not read previous installation token: %v", err), now)
				} else if oldToken != "" {
					if err := r.InstallationTokens.Revoke(ctx, oldToken); err != nil {
						return r.credentialFailure(task, fmt.Sprintf("could not revoke previous installation token: %v", err), now)
					}
				}
			}
			keyRef := provider.GetSpec().GetGithubApp().GetPrivateKeySecret()
			secretCtx, cancel := context.WithTimeout(ctx, secretLookupTimeout)
			privateKey, err := r.SecretResolver(secretCtx, atespace, keyRef.GetName(), keyRef.GetKey())
			cancel()
			if err != nil || privateKey == "" {
				return r.credentialFailure(task, "could not read GitHub App private key", now)
			}
			newToken, err = r.InstallationTokens.Mint(ctx, provider.GetSpec().GetGithubApp(), privateKey)
			if err != nil {
				return r.credentialFailure(task, "could not mint GitHub installation token", now)
			}
			extraEnv["GITHUB_TOKEN"] = newToken
		}
	}

	if extraEnv[claudeAgentEnv] == claudeAgent {
		if extraEnv["AX_CLAUDE_PROVIDER"] == openRouterProvider {
			if openRouterKey := r.lookupSecret(ctx, atespace, openRouterSecretName, openRouterSecretKey); openRouterKey != "" {
				extraEnv["ANTHROPIC_BASE_URL"] = "https://openrouter.ai/api"
				extraEnv["ANTHROPIC_AUTH_TOKEN"] = openRouterKey
				extraEnv[anthropicSecretKey] = ""
			}
		} else if anthropicKey := r.lookupSecret(ctx, atespace, anthropicSecretName, anthropicSecretKey); anthropicKey != "" {
			extraEnv[anthropicSecretKey] = anthropicKey
		}
	} else if geminiKey := r.lookupGeminiKey(ctx, atespace); geminiKey != "" {
		extraEnv[geminiSecretKey] = geminiKey
	}

	// Only launch configuration belongs in the template; status changes
	// must not create new golden snapshots.
	launchTask := proto.Clone(task).(*v1alpha1.Task)
	launchTask.Status = nil
	// The runner re-applies every AX_TASK_YAML spec.env entry over the
	// container's real environment on startup (see runner.Run), which would
	// otherwise stomp the credentials just resolved above -- e.g. restoring a
	// task-supplied ANTHROPIC_API_KEY that was deliberately cleared for
	// OpenRouter, or an ANTHROPIC_AUTH_TOKEN the task itself happened to set.
	// Strip anything the controller manages so only its resolved value reaches
	// the container.
	if launchTask.Spec != nil && len(launchTask.Spec.Env) > 0 {
		kept := launchTask.Spec.Env[:0]
		for _, e := range launchTask.Spec.Env {
			switch e.GetName() {
			case anthropicSecretKey, "ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_BASE_URL", geminiSecretKey:
			default:
				kept = append(kept, e)
			}
		}
		launchTask.Spec.Env = kept
	}
	if taskYAML, err := yaml.Marshal(launchTask); err == nil {
		extraEnv["AX_TASK_YAML"] = string(taskYAML)
	}

	// Inject the bound Workspace specs as a multi-document YAML stream.
	if wsYAML, err := marshalWorkspaces(workspaces); err == nil && wsYAML != "" {
		extraEnv["AX_WORKSPACES_YAML"] = wsYAML
	}

	// If a custom image, workspace, or extra environment is specified, provision or use a dedicated ActorTemplate
	if !(provider != nil && newToken == "" && existingActor != nil) && task.Spec != nil && (task.Spec.Image != "" || len(extraEnv) > 0) {
		slog.Info("ensuring custom ActorTemplate for task", "image", task.Spec.Image)
		customTemplateName := taskTemplateName(task.Metadata.Name, task.Spec.Image, extraEnv)

		tmpl, err := r.client.EnsureActorTemplateWithImage(ctx, templateAtespace, templateName, atespace, customTemplateName, task.Spec.Image, extraEnv)
		if err != nil && provider != nil {
			r.revokeForCleanup(newToken)
			return r.credentialFailure(task, "could not create credentialed actor template", now)
		} else if err != nil {
			slog.Warn("could not create custom ActorTemplate, falling back to default template", "error", err)
		} else if tmpl != nil && tmpl.Metadata != nil {
			templateAtespace = tmpl.Metadata.Atespace
			templateName = tmpl.Metadata.Name
			slog.Info("using custom ActorTemplate for actor", "templateAtespace", templateAtespace, "templateName", templateName)
		}
	}
	// A crashed actor is only reverted to SUSPENDED by EnsureActor below (via
	// its own AlreadyExists/RevertActor recovery), and SetActorTemplate needs
	// a suspended actor -- so the template switch has to happen after
	// EnsureActor, against whatever actor it actually returns, not the
	// pre-call existingActor. For an actor that was already suspended (not
	// crashed), EnsureActor returns that same actor, so this is equivalent to
	// switching before it in every other case.
	ensuredActor, err := r.client.EnsureActor(ctx, atespace, actorName, templateAtespace, templateName)
	if err != nil {
		if newToken != "" {
			r.revokeForCleanup(newToken)
		}
		r.setNotReady(task, "ActorCreationFailed", err.Error(), now)
		task.Status.Phase = "Failed"
		return task, fmt.Errorf("ensuring actor: %w", err)
	}
	if provider != nil && newToken != "" && (ensuredActor.GetActorTemplate().GetName() != templateName || ensuredActor.GetActorTemplate().GetAtespace() != templateAtespace) {
		if _, err := r.client.SetActorTemplate(ctx, ensuredActor, templateAtespace, templateName); err != nil {
			r.revokeForCleanup(newToken)
			return r.credentialFailure(task, "could not switch actor template", now)
		}
	}

	// 4. Suspend or Resume the Actor
	// Tasks are suspended by default upon creation until explicitly resumed to "Running".
	if task.Status.Phase == "Suspended" || task.Status.Phase == "" {
		slog.Info("suspending actor on Substrate", "actor", actorName)
		if err := r.client.SuspendActor(ctx, atespace, actorName); err != nil {
			if provider != nil && newToken != "" {
				// The actor may still be running with this token; revoking it now,
				// and reporting the task safely suspended, would be wrong either way.
				return r.credentialFailure(task, fmt.Sprintf("could not suspend actor: %v", err), now)
			}
			slog.Warn("could not suspend actor on Substrate", "error", err)
		}
		// A credentialed task that starts out suspended still minted a token above
		// for the eventual resume; it must not sit valid on an actor that never ran.
		if provider != nil && newToken != "" {
			if err := r.InstallationTokens.Revoke(ctx, newToken); err != nil {
				return r.credentialFailure(task, fmt.Sprintf("could not revoke installation token: %v", err), now)
			}
		}
		task.Status.WorkerIp = ""
		task.Status.Phase = "Suspended"
		r.setCondition(task, condReady, "False", "TaskSuspended", "Task is suspended", now)
		slog.Info("task successfully suspended", "name", task.Metadata.Name, "actor", actorName)
		return task, nil
	}

	// Resume the Actor to activate container execution
	slog.Info("resuming actor on Substrate worker", "actor", actorName)
	_, workerIP, err := r.client.ResumeActor(ctx, atespace, actorName)
	if err != nil {
		if newToken != "" {
			// ResumeActor's error is ambiguous: Substrate may have resumed the
			// actor anyway and only the response was lost, in which case it is
			// now running with newToken baked into its template. Revoking that
			// token without also suspending would leave a running actor with a
			// dead credential, and the next ResumeTask would see it already
			// RUNNING and skip rotating the token, so it would never recover.
			// Force it back to SUSPENDED first (a no-op if it never actually
			// resumed) so the next attempt mints and applies a fresh token.
			//
			// ctx itself may be why ResumeActor just failed (canceled or past
			// its deadline), in which case reusing it here would make this
			// suspend fail too without ever reaching Substrate -- so use a
			// fresh, independently bounded context. And only revoke once that
			// suspend is confirmed: revoking on an unconfirmed suspend risks
			// the actor still running with a now-dead token for good, whereas
			// leaving a still-valid token on a running actor is harmless.
			cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupRevokeTimeout)
			suspendErr := r.client.SuspendActor(cleanupCtx, atespace, actorName)
			cancel()
			if suspendErr != nil {
				slog.Warn("could not suspend actor after ambiguous resume failure; leaving its token live rather than risk stranding a running actor with a dead one", "actor", actorName, "error", suspendErr)
			} else {
				r.revokeForCleanup(newToken)
			}
		}
		r.setNotReady(task, "ActorResumeFailed", err.Error(), now)
		task.Status.Phase = "Failed"
		return task, fmt.Errorf("resuming actor: %w", err)
	}

	task.Status.WorkerIp = workerIP
	task.Status.Phase = "Running"

	// Check if workspace setup inside the actor has completed
	host := workerIP
	port := "80"
	if h, p, err := net.SplitHostPort(workerIP); err == nil {
		host = h
		port = p
	}
	readyURL := fmt.Sprintf("http://%s:%s/readyz?check=workspace", host, port)
	// Workspace setup happens once per task. After it has completed, WorkspaceReady stays
	// True across suspend/resume cycles, so only poll while it is still initializing.
	workspaceReady := r.conditionTrue(task, condWorkspaceReady)
	workspaceFailed := false
	if workerIP != "" && !workspaceReady {
		// Poll briefly for workspace setup completion
		pollCtx, cancel := context.WithTimeout(ctx, r.WorkspaceReadyTimeout)
		defer cancel()

		ticker := time.NewTicker(workspaceReadyPollInterval)
		defer ticker.Stop()

		checkReady := func() bool {
			// 1. Direct readyz check
			req, _ := http.NewRequestWithContext(pollCtx, http.MethodGet, readyURL, nil)
			if resp, err := r.httpClient.Do(req); err == nil {
				_ = resp.Body.Close()
				if provider != nil && resp.StatusCode == http.StatusFailedDependency {
					workspaceFailed = true
					return false
				}
				if resp.StatusCode == http.StatusOK {
					return true
				}
			}

			// 2. Router-based readyz check (in-cluster via atenet-router)
			if routerAddr := os.Getenv("ATENET_ROUTER_ADDR"); routerAddr != "" {
				rReq, _ := http.NewRequestWithContext(pollCtx, http.MethodGet, fmt.Sprintf("http://%s/readyz?check=workspace", routerAddr), nil)
				if rReq != nil {
					rReq.Header.Set("ate-target-actor", fmt.Sprintf("%s/%s", atespace, actorName))
					if resp, err := r.httpClient.Do(rReq); err == nil {
						_ = resp.Body.Close()
						if provider != nil && resp.StatusCode == http.StatusFailedDependency {
							workspaceFailed = true
							return false
						}
						if resp.StatusCode == http.StatusOK {
							return true
						}
					}
				}
			}
			return false
		}

		// Initial check
		workspaceReady = checkReady()
		if workspaceFailed {
			goto DonePolling
		}

		for !workspaceReady && !workspaceFailed {
			select {
			case <-pollCtx.Done():
				goto DonePolling
			case <-ticker.C:
				if checkReady() {
					workspaceReady = true
					goto DonePolling
				}
			}
		}
	}
DonePolling:
	if workspaceFailed {
		failNow := time.Now()
		if actor, err := r.client.GetActor(ctx, atespace, actorName); err == nil {
			if suspendErr := r.suspendAndRevoke(ctx, atespace, actorName, actor); suspendErr != nil {
				slog.Warn("could not suspend and revoke actor after workspace failure", "actor", actorName, "error", suspendErr)
			}
		}
		return r.credentialFailure(task, "credentialed workspace setup failed", failNow)
	}
	if provider != nil && !workspaceReady {
		actor, err := r.client.GetActor(ctx, atespace, actorName)
		if err == nil && actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_CRASHED {
			// The actor is already down; only the token needs cleaning up.
			if token, tokErr := r.actorToken(ctx, actor); tokErr == nil && token != "" {
				r.revokeForCleanup(token)
			}
			return r.credentialFailure(task, "credentialed actor failed during workspace setup", time.Now())
		}
	}

	// The task is Ready only once its actor is running and the workspace inside it is set up.
	if workspaceReady {
		if !r.conditionTrue(task, condWorkspaceReady) {
			r.setCondition(task, condWorkspaceReady, "True", "SetupComplete", fmt.Sprintf("Workspace setup completed at %s", workerIP), time.Now())
		}
		r.setCondition(task, condReady, "True", "TaskRunning", "Task is running and its workspace is ready", time.Now())
	} else {
		r.setCondition(task, condWorkspaceReady, "False", "Initializing", fmt.Sprintf("Workspace is initializing at %s", workerIP), time.Now())
		r.setCondition(task, condReady, "False", "WorkspaceInitializing", "Waiting for workspace setup to complete", time.Now())
	}

	slog.Info("task successfully reconciled and running",
		"name", task.Metadata.Name,
		"actor", actorName,
		"workerIP", workerIP,
		"workspaceReady", workspaceReady,
		"phase", task.Status.Phase,
	)

	return task, nil
}

// Condition types reported on Task status.
const (
	// condReady reports whether the task as a whole is ready to do work: its actor is
	// running and the workspace inside it has finished setting up.
	condReady = "Ready"
	// condWorkspaceReady reports whether the workspace inside the actor has finished setting up.
	condWorkspaceReady = "WorkspaceReady"
)

// setNotReady marks the task's Ready condition False. WorkspaceReady is left untouched:
// workspace setup is a one-time step whose result outlives actor failures and suspends.
func (r *TaskReconciler) setNotReady(task *v1alpha1.Task, reason, message string, t time.Time) {
	r.setCondition(task, condReady, "False", reason, message, t)
}

func (r *TaskReconciler) credentialFailure(task *v1alpha1.Task, message string, now time.Time) (*v1alpha1.Task, error) {
	task.Status.Phase = "Failed"
	r.setNotReady(task, "CredentialFailed", message, now)
	return task, errors.New(message)
}

// revokeForCleanup revokes a token minted earlier in a Reconcile call that
// then failed a later step, using a fresh context instead of the caller's:
// that context may itself be why the later step failed (canceled or expired),
// and the only copy of the token must still be revoked.
func (r *TaskReconciler) revokeForCleanup(token string) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupRevokeTimeout)
	defer cancel()
	if err := r.InstallationTokens.Revoke(cleanupCtx, token); err != nil {
		slog.Warn("could not revoke installation token during cleanup", "error", err)
	}
}

// suspendAndRevoke suspends actor on Substrate and revokes its current GitHub
// installation token, so no credentialed actor is left running, or holding a
// live token, once the task is no longer meant to be active. Revoke is safe to
// call repeatedly (the GitHub API treats an already-void token as success), so
// this always re-reads and re-revokes rather than tracking "already revoked"
// in task status: that status can be lost (a failed or skipped status write)
// independently of whether the actor's actual, current token was revoked.
func (r *TaskReconciler) suspendAndRevoke(ctx context.Context, atespace, actorName string, actor *ateapipb.Actor) error {
	state := actor.GetStatus().GetState()
	// A crashed actor is already stopped, not merely in need of suspending --
	// and Substrate requires RevertActor (back to SUSPENDED) before it will
	// accept a SuspendActor call on it, which this helper has no reason to
	// do on behalf of a caller that just wants the token cleaned up. Treat it
	// like SUSPENDED: skip straight to revoking.
	if state != ateapipb.ActorState_ACTOR_STATE_SUSPENDED && state != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		if err := r.client.SuspendActor(ctx, atespace, actorName); err != nil {
			return fmt.Errorf("could not suspend actor: %w", err)
		}
	}
	// A fresh context for both the token read and its revoke: ctx may be
	// close to its deadline after SuspendActor above (which can itself run
	// long), and the actor is confirmed stopped at this point regardless --
	// losing the token read to a now-exhausted ctx would report this failed
	// even though suspension genuinely succeeded, and leave the token
	// unrevoked until a retry.
	cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupRevokeTimeout)
	defer cancel()
	token, err := r.actorToken(cleanupCtx, actor)
	if err != nil {
		return fmt.Errorf("could not read actor token for revocation: %w", err)
	}
	if token != "" {
		if err := r.InstallationTokens.Revoke(cleanupCtx, token); err != nil {
			return fmt.Errorf("could not revoke installation token: %w", err)
		}
	}
	return nil
}

func validateCredentialedWorkspaces(provider *v1alpha1.CredentialProvider, workspaces []*v1alpha1.Workspace) error {
	allowed := make(map[string]bool)
	for _, name := range provider.GetSpec().GetGithubApp().GetRepositories() {
		allowed[strings.ToLower(name)] = true
	}
	for _, ws := range workspaces {
		for _, repo := range ws.GetSpec().GetGit() {
			raw := repo.GetRepo()
			lower := strings.ToLower(raw)
			if strings.HasPrefix(lower, "git@github.com:") || strings.HasPrefix(lower, "ssh://git@github.com/") {
				return fmt.Errorf("credentialed GitHub repository must use HTTPS")
			}
			u, err := url.Parse(raw)
			if err != nil {
				return fmt.Errorf("invalid Git repository URL")
			}
			if !strings.EqualFold(u.Hostname(), "github.com") {
				continue
			}
			if u.Scheme != "https" || u.User != nil {
				return fmt.Errorf("credentialed GitHub repository must use HTTPS without embedded credentials")
			}
			parts := strings.Split(strings.Trim(u.Path, "/"), "/")
			if len(parts) != 2 || !allowed[strings.ToLower(strings.TrimSuffix(parts[1], ".git"))] {
				return fmt.Errorf("GitHub repository is not listed in the credential provider")
			}
		}
	}
	return nil
}

func (r *TaskReconciler) actorToken(ctx context.Context, actor *ateapipb.Actor) (string, error) {
	ref := actor.GetActorTemplate()
	if ref == nil {
		return "", nil
	}
	tmpl, err := r.client.GetActorTemplate(ctx, ref.GetAtespace(), ref.GetName())
	if err != nil {
		return "", err
	}
	return templateToken(tmpl), nil
}

// templateToken extracts the GITHUB_TOKEN baked into an ActorTemplate's
// container environment, or "" if it has none.
func templateToken(tmpl *ateapipb.ActorTemplate) string {
	for _, container := range tmpl.GetContainers() {
		for _, env := range container.GetEnv() {
			if env.GetName() == "GITHUB_TOKEN" {
				return env.GetValue()
			}
		}
	}
	return ""
}

// conditionTrue reports whether the task currently has the given condition with status True.
func (r *TaskReconciler) conditionTrue(task *v1alpha1.Task, condType string) bool {
	if task.Status == nil {
		return false
	}
	for _, c := range task.Status.Conditions {
		if c.Type == condType {
			return c.Status == "True"
		}
	}
	return false
}

func (r *TaskReconciler) setCondition(task *v1alpha1.Task, condType, status, reason, message string, t time.Time) {
	ts := timestamppb.New(t)
	for i, c := range task.Status.Conditions {
		if c.Type == condType {
			task.Status.Conditions[i].Status = status
			task.Status.Conditions[i].Reason = reason
			task.Status.Conditions[i].Message = message
			task.Status.Conditions[i].LastTransitionTime = ts
			return
		}
	}
	task.Status.Conditions = append(task.Status.Conditions, &v1alpha1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: ts,
	})
}

// lookupGeminiKey resolves the Gemini API key for the task container, preferring the
// Kubernetes secret in the task's atespace and falling back to the server's own
// environment. It returns "" when neither source has a value.
func (r *TaskReconciler) lookupGeminiKey(ctx context.Context, atespace string) string {
	if key := r.lookupSecret(ctx, atespace, geminiSecretName, geminiSecretKey); key != "" {
		return key
	}
	if key := os.Getenv(geminiSecretKey); key != "" {
		slog.Info("resolved GEMINI_API_KEY from server environment for actor template")
		return key
	}
	return ""
}

func (r *TaskReconciler) lookupSecret(ctx context.Context, atespace, secretName, secretKey string) string {
	if r.SecretResolver == nil {
		return ""
	}
	lookupCtx, cancel := context.WithTimeout(ctx, secretLookupTimeout)
	defer cancel()
	key, err := r.SecretResolver(lookupCtx, atespace, secretName, secretKey)
	if err != nil || key == "" {
		return ""
	}
	slog.Info("resolved task API key from kubernetes secret for actor template", "atespace", atespace, "secret", secretName)
	return key
}

// taskTemplateName derives the per-task ActorTemplate name from the task name and a
// digest of the image and container environment, so a spec change yields a new template.
func taskTemplateName(taskName, image string, env map[string]string) string {
	h := sha256.New()
	h.Write([]byte(image))
	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		h.Write([]byte(k + "=" + env[k] + ";"))
	}
	return fmt.Sprintf("%s-tmpl-%x", taskName, h.Sum(nil)[:templateDigestBytes])
}

// taskTemplatePattern matches every ActorTemplate name taskTemplateName can produce
// for the given task, across all spec revisions.
func taskTemplatePattern(taskName string) *regexp.Regexp {
	return regexp.MustCompile(fmt.Sprintf("^%s-tmpl-[0-9a-f]{%d}$", regexp.QuoteMeta(taskName), 2*templateDigestBytes))
}

// ReconcileDelete cleans up Substrate resources when a Task is deleted: the actor,
// which shares the task's name, then every ActorTemplate provisioned for the task.
// hasCredentialProvider must be true only when the deleted task's spec referenced a
// CredentialProvider, so a GITHUB_TOKEN a task set directly via spec.env (shared or
// externally managed) is never revoked here.
func (r *TaskReconciler) ReconcileDelete(ctx context.Context, atespace, taskName string, hasCredentialProvider bool) error {
	if atespace == "" {
		atespace = "default"
	}
	slog.Info("deleting Substrate actor for task", "atespace", atespace, "task", taskName)
	actor, err := r.client.GetActor(ctx, atespace, taskName)
	if err != nil && status.Code(err) != codes.NotFound {
		return err
	}
	var token string
	if actor != nil && hasCredentialProvider {
		token, err = r.actorToken(ctx, actor)
		if err != nil {
			return fmt.Errorf("reading actor credential for revocation: %w", err)
		}
	}
	// Revoke before deleting the actor: once it is gone, a retry after a failed
	// revoke can no longer read the token back out of its template.
	if token != "" {
		if err := r.InstallationTokens.Revoke(ctx, token); err != nil {
			return fmt.Errorf("revoking actor credential: %w", err)
		}
	}
	if err := r.client.DeleteActor(ctx, atespace, taskName); err != nil {
		return err
	}
	if err := r.deleteTaskTemplates(ctx, atespace, taskName, hasCredentialProvider); err != nil {
		slog.Warn("could not clean up actor templates for task", "task", taskName, "error", err)
	}
	return nil
}

// deleteTaskTemplates removes all ActorTemplates belonging to the task. Templates
// accumulate across spec revisions, so this matches by name pattern rather than
// recomputing a single digest. If an actor deletion is still finishing in Substrate,
// template deletion may briefly return Aborted, so we retry with backoff.
//
// A task's current actor only ever references its newest template, so a
// GITHUB_TOKEN baked into an older, orphaned one (left behind by a failed or
// skipped revoke on an earlier spec revision) is never reached by the revoke
// in ReconcileDelete above. revokeCredentials must be true only when the
// task referenced a CredentialProvider, matching ReconcileDelete's own gate.
func (r *TaskReconciler) deleteTaskTemplates(ctx context.Context, atespace, taskName string, revokeCredentials bool) error {
	templates, err := r.client.ListActorTemplates(ctx, atespace)
	if err != nil {
		return err
	}
	pattern := taskTemplatePattern(taskName)
	var errs []error
	for _, tmpl := range templates {
		name := tmpl.GetMetadata().GetName()
		if !pattern.MatchString(name) {
			continue
		}
		if revokeCredentials {
			full, err := r.client.GetActorTemplate(ctx, atespace, name)
			if err != nil {
				// Deleting this template now would destroy the only stored
				// copy of whatever token it holds, with no later retry able
				// to read it back out. Leave it in place for a retry instead.
				errs = append(errs, fmt.Errorf("reading actor template %s for revocation: %w", name, err))
				continue
			}
			if token := templateToken(full); token != "" {
				if err := r.InstallationTokens.Revoke(ctx, token); err != nil {
					errs = append(errs, fmt.Errorf("revoking token from actor template %s: %w", name, err))
					continue
				}
			}
		}
		slog.Info("deleting Substrate actor template for task", "atespace", atespace, "task", taskName, "template", name)
		var delErr error
		for attempt := 0; attempt < 5; attempt++ {
			delErr = r.client.DeleteActorTemplate(ctx, atespace, name)
			if delErr == nil || status.Code(delErr) == codes.NotFound {
				delErr = nil
				break
			}
			select {
			case <-ctx.Done():
				delErr = ctx.Err()
				break
			case <-time.After(500 * time.Millisecond):
			}
		}
		if delErr != nil {
			errs = append(errs, delErr)
		}
	}
	return errors.Join(errs...)
}

// marshalWorkspaces renders the workspaces as a multi-document YAML stream in
// order, skipping nil entries. It returns "" when there is nothing to render.
func marshalWorkspaces(workspaces []*v1alpha1.Workspace) (string, error) {
	var sb strings.Builder
	enc := yaml.NewEncoder(&sb)
	for _, ws := range workspaces {
		if ws == nil {
			continue
		}
		if err := enc.Encode(ws); err != nil {
			return "", err
		}
	}
	if err := enc.Close(); err != nil {
		return "", err
	}
	return sb.String(), nil
}
