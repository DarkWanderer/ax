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
	// workspaceFailureCleanupAttempts bounds the retries for suspending and
	// revoking an actor after its workspace setup reports failure: with no
	// background reconciliation loop, a single transient failure here (a busy
	// control plane, a momentary network blip) would otherwise leave the
	// actor running indefinitely with a live installation token.
	workspaceFailureCleanupAttempts = 3
	workspaceFailureCleanupDelay    = 2 * time.Second
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

	// CleanupRetryDelay is the delay between retries in
	// suspendAndRevokeWithRetry. It defaults to workspaceFailureCleanupDelay;
	// tests shorten it so a retried cleanup doesn't slow down the suite.
	CleanupRetryDelay time.Duration
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
		CleanupRetryDelay:       workspaceFailureCleanupDelay,
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
		// These checks can fail for a task whose actor is already RUNNING (the
		// provider was deleted, or edited into an invalid or more restrictive
		// shape, after the actor was resumed under its old configuration): a
		// plain credentialFailure here would only mark the stored task Failed
		// and return, leaving that actor running with its already-minted,
		// still-live installation token indefinitely, since there is no
		// background reconciliation to suspend and revoke it later. Stop the
		// existing actor first in that case, same as an explicit suspend
		// would, rather than reporting failure while the actor and its
		// credential remain live.
		if provider == nil || provider.GetMetadata().GetName() != ref.GetName() || provider.GetMetadata().GetAtespace() != atespace {
			return r.credentialFailureStoppingActor(task, "credential provider is missing from the task atespace", now, atespace, actorName, existingActor)
		}
		if err := v1alpha1.ValidateCredentialProvider(provider); err != nil {
			return r.credentialFailureStoppingActor(task, err.Error(), now, atespace, actorName, existingActor)
		}
		if err := validateCredentialedWorkspaces(provider, workspaces); err != nil {
			return r.credentialFailureStoppingActor(task, err.Error(), now, atespace, actorName, existingActor)
		}
	}
	if ref != nil {
		if existingActor != nil && (existingActor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_RUNNING || existingActor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_RESUMING) && !taskSuspending {
			token, err := r.actorToken(ctx, existingActor)
			if err != nil || token == "" {
				// Same reasoning as the provider-validation failures above: the
				// actor is already running (with, for all this inconclusive
				// check knows, a still-live token), and plain credentialFailure
				// would just mark the task Failed and return, leaving it
				// running indefinitely with no background reconciliation to
				// stop it later.
				return r.credentialFailureStoppingActor(task, "running actor has no GitHub installation token; suspend and resume the task", now, atespace, actorName, existingActor)
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

	// A lookup failure here (as opposed to the secret being authoritatively
	// unconfigured) must not silently compute a credential-less template and
	// switch an already-running actor to it -- see templateApplicable below.
	var secretLookupErr error
	if extraEnv[claudeAgentEnv] == claudeAgent {
		if extraEnv["AX_CLAUDE_PROVIDER"] == openRouterProvider {
			// Cleared unconditionally, not only when the lookup below succeeds:
			// a task can supply its own stale ANTHROPIC_API_KEY in spec.env, and
			// leaving it in extraEnv would give Claude a valid Anthropic
			// credential to silently run against directly -- against the wrong
			// provider's billing -- instead of either using the OpenRouter
			// gateway or (if that lookup is itself what's failing) having no
			// working credential at all, which is the correct, visible outcome
			// for an OpenRouter-configured task missing its secret.
			extraEnv[anthropicSecretKey] = ""
			openRouterKey, err := r.lookupSecret(ctx, atespace, openRouterSecretName, openRouterSecretKey)
			secretLookupErr = err
			if openRouterKey != "" {
				extraEnv["ANTHROPIC_BASE_URL"] = "https://openrouter.ai/api"
				extraEnv["ANTHROPIC_AUTH_TOKEN"] = openRouterKey
			}
		} else {
			anthropicKey, err := r.lookupSecret(ctx, atespace, anthropicSecretName, anthropicSecretKey)
			secretLookupErr = err
			if anthropicKey != "" {
				extraEnv[anthropicSecretKey] = anthropicKey
			}
		}
	} else {
		geminiKey, err := r.lookupGeminiKey(ctx, atespace)
		secretLookupErr = err
		if geminiKey != "" {
			extraEnv[geminiSecretKey] = geminiKey
		}
	}
	if secretLookupErr != nil {
		slog.Warn("could not resolve task API key; leaving the actor's current template in place this round", "task", task.Metadata.GetName(), "error", secretLookupErr)
		// A GitHub token rotation (newToken != "") still forces a template
		// switch below despite this failure, to avoid leaving the actor on a
		// template whose GitHub credential was just revoked (see the switch
		// condition's own comment). That replacement template is otherwise
		// built from extraEnv as of right now, which is missing the model
		// credential this round's lookup couldn't resolve -- carrying the
		// previous template's own model credential forward keeps the new
		// template complete, instead of resuming the actor into a workspace
		// goal that can't run for lack of credentials while WorkspaceReady may
		// already be stuck true from an earlier round. This must overwrite
		// rather than merely fill extraEnv: these are controller-managed keys
		// that the task's own spec.env may also set (and the success path
		// below unconditionally overwrites, then strips from AX_TASK_YAML, for
		// exactly that reason -- see the launchTask.Spec.Env filtering further
		// down), so a stale task-supplied value must not win over the
		// template's last known-working one just because it happened to
		// already be non-empty.
		if newToken != "" && existingActor != nil {
			oldTmpl, err := r.client.GetActorTemplate(ctx, existingActor.GetActorTemplate().GetAtespace(), existingActor.GetActorTemplate().GetName())
			if err != nil {
				// Proceeding here would build and apply a replacement template
				// missing the model credential, with no way to carry it
				// forward: that's worse than stopping now, since the actor
				// hasn't been touched yet this round and so remains on its
				// current (if GitHub-token-stale) template rather than one
				// that's also missing its model credential.
				r.revokeForCleanup(newToken)
				return r.credentialFailure(task, fmt.Sprintf("could not read previous actor template to carry its model credential forward: %v", err), now)
			}
			for _, key := range []string{anthropicSecretKey, "ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_BASE_URL", geminiSecretKey} {
				if v := templateEnvValue(oldTmpl, key); v != "" {
					extraEnv[key] = v
				}
			}
		}
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
	//
	// templateApplicable tracks whether templateName/templateAtespace were
	// actually (re)computed against the current extraEnv this round: when
	// this step is skipped below (a credentialed resume that didn't mint a
	// fresh token), they're left at the generic default and must not be
	// compared against the actor's real, already-correct custom template.
	templateApplicable := false
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
			templateApplicable = true
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
	// Not gated on provider/newToken: a suspended actor's template can go
	// stale from a controller-managed Claude/Gemini secret changing just as
	// much as from a GitHub token rotating, and EnsureActor above already
	// created the new template either way -- only switching the actor to it
	// was still missing for the non-credentialed case. Still gated on
	// templateApplicable, so a round where the custom-template step was
	// itself skipped doesn't compare the actor's real template against a
	// stale default and "switch" it away from the correct one. Also gated on
	// secretLookupErr == nil unless this round minted a fresh GitHub
	// installation token (newToken != ""): a transient (not authoritative
	// "unconfigured") secret-lookup failure must not switch an existing actor
	// away from a template that may already have the credential this round
	// couldn't resolve -- UNLESS the actor's *current* template is already
	// known-stale because its GitHub token was revoked above (line ~240) as
	// part of minting newToken. In that case staying on the old template
	// would leave the actor pointed at a dead credential, which is strictly
	// worse than switching to the new template (correct GitHub token, if
	// anything a stale model secret for one more round). This never affects
	// first-time creation -- a freshly created actor's template already
	// equals templateName, so the switch condition below is false for it
	// regardless.
	if templateApplicable && (newToken != "" || secretLookupErr == nil) && (ensuredActor.GetActorTemplate().GetName() != templateName || ensuredActor.GetActorTemplate().GetAtespace() != templateAtespace) {
		// SetActorTemplate's contract only supports a suspended actor. A
		// running actor (e.g. a non-credentialed resume whose secret rotated
		// mid-flight) can't take the new template now; skip it here rather
		// than failing the whole reconcile and marking an otherwise-healthy
		// running task Failed -- it picks up the current template next time
		// it's actually suspended and resumed.
		if ensuredActor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
			slog.Info("actor template changed but actor is not suspended; deferring the switch", "actor", actorName, "state", ensuredActor.GetStatus().GetState())
		} else if _, err := r.client.SetActorTemplate(ctx, ensuredActor, templateAtespace, templateName); err != nil {
			if newToken != "" {
				r.revokeForCleanup(newToken)
			}
			r.setNotReady(task, "TemplateSwitchFailed", err.Error(), now)
			task.Status.Phase = "Failed"
			return task, fmt.Errorf("switching actor template: %w", err)
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
		// A fresh, independently bounded context for the revoke: ctx may be close
		// to its deadline after SuspendActor above (which can itself run long),
		// and the actor is confirmed suspended at this point regardless -- losing
		// the revoke to a now-exhausted ctx would report this call failed and the
		// task record never created, even though the token is the only thing left
		// live, with no SuspendActor failure path above to have already reported it.
		if provider != nil && newToken != "" {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupRevokeTimeout)
			err := r.InstallationTokens.Revoke(cleanupCtx, newToken)
			cancel()
			if err != nil {
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
		if err := r.suspendAndRevokeWithRetry(atespace, actorName); err != nil {
			slog.Error("could not suspend and revoke actor after workspace failure; actor may still be running with a live installation token and needs manual cleanup", "actor", actorName, "error", err)
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

// credentialFailureStoppingActor reports the same terminal credentialFailure,
// but first stops an already-provisioned actor (suspend + revoke, with
// retries) when one exists: a validation failure here can occur while the
// actor is RUNNING with a live installation token -- e.g. its credential
// provider was deleted or edited into an invalid or more restrictive shape
// after it was last resumed -- and with no background reconciliation to
// suspend and revoke it later, reporting failure without also stopping it
// would leave that actor and its credential live indefinitely.
func (r *TaskReconciler) credentialFailureStoppingActor(task *v1alpha1.Task, message string, now time.Time, atespace, actorName string, existingActor *ateapipb.Actor) (*v1alpha1.Task, error) {
	if existingActor != nil {
		if err := r.suspendAndRevokeWithRetry(atespace, actorName); err != nil {
			slog.Error("could not stop actor after a credential validation failure; actor may still be running with a live installation token and needs manual cleanup", "actor", actorName, "error", err)
		}
	}
	return r.credentialFailure(task, message, now)
}

// revokeForCleanup revokes a token minted earlier in a Reconcile call that
// then failed a later step, using a fresh context instead of the caller's:
// that context may itself be why the later step failed (canceled or expired),
// and the only copy of the token must still be revoked. It retries a few
// times: this is often the token's only remaining trace (e.g. when the
// actor template that would have stored it was never successfully created),
// so a single transient failure here would otherwise leave it live until
// GitHub expires it, with no background reconciliation to retry later.
func (r *TaskReconciler) revokeForCleanup(token string) {
	var lastErr error
	for attempt := 1; attempt <= workspaceFailureCleanupAttempts; attempt++ {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupRevokeTimeout)
		err := r.InstallationTokens.Revoke(cleanupCtx, token)
		cancel()
		if err == nil {
			return
		}
		lastErr = err
		if attempt < workspaceFailureCleanupAttempts {
			slog.Warn("could not revoke installation token during cleanup; retrying", "attempt", attempt, "error", err)
			time.Sleep(r.CleanupRetryDelay)
		}
	}
	slog.Error("could not revoke installation token during cleanup; token may still be live and needs manual cleanup", "error", lastErr)
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

// suspendAndRevokeWithRetry re-reads the actor and retries suspendAndRevoke a
// few times, so a transient failure in GetActor, SuspendActor, the template
// lookup, or the revoke itself doesn't immediately strand a credentialed
// actor running with a live installation token: there is no background
// reconciliation to pick this up later (see the reconcile loop's own
// WorkspaceReadyTimeout comment), so this call is the only chance to clean up
// before the task is reported Failed. Each attempt re-reads the actor rather
// than reusing a stale one, since a GetActor failure is itself one of the
// steps being retried.
func (r *TaskReconciler) suspendAndRevokeWithRetry(atespace, actorName string) error {
	var lastErr error
	for attempt := 1; attempt <= workspaceFailureCleanupAttempts; attempt++ {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupRevokeTimeout)
		actor, err := r.client.GetActor(cleanupCtx, atespace, actorName)
		if err == nil {
			err = r.suspendAndRevoke(cleanupCtx, atespace, actorName, actor)
		}
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		if attempt < workspaceFailureCleanupAttempts {
			slog.Warn("cleanup attempt failed after workspace failure; retrying", "actor", actorName, "attempt", attempt, "error", err)
			time.Sleep(r.CleanupRetryDelay)
		}
	}
	return lastErr
}

// scpLikeGitHubRemote matches Git's generic scp-like syntax ([user@]host:path,
// recognized whenever a colon precedes the first slash and no "scheme://" is
// present -- see git-clone(1)) when it targets github.com, with any or no
// user, not just the conventional "git@github.com:" spelling (e.g. a bare
// "github.com:org/private.git" or "alice@github.com:org/private.git" would
// otherwise fall through url.Parse with no recognizable hostname and skip
// GitHub-specific validation entirely). The user portion is matched by what
// it excludes, not an allowlist of characters it permits: Git imposes no
// charset restriction on it (e.g. "foo+bar@github.com:..." is valid scp-like
// syntax), so matching is anchored on reaching "github.com:" with nothing
// resembling another "@", ":", "/" or whitespace in between, rather than on
// which characters a username may contain.
var scpLikeGitHubRemote = regexp.MustCompile(`(?i)^(?:[^@:/\s]*@)?github\.com:`)

func validateCredentialedWorkspaces(provider *v1alpha1.CredentialProvider, workspaces []*v1alpha1.Workspace) error {
	allowed := make(map[string]bool)
	for _, name := range provider.GetSpec().GetGithubApp().GetRepositories() {
		allowed[strings.ToLower(name)] = true
	}
	for _, ws := range workspaces {
		for _, repo := range ws.GetSpec().GetGit() {
			raw := repo.GetRepo()
			lower := strings.ToLower(raw)
			if strings.HasPrefix(lower, "ssh://git@github.com/") || scpLikeGitHubRemote.MatchString(raw) {
				return fmt.Errorf("credentialed GitHub repository must use HTTPS")
			}
			u, err := url.Parse(raw)
			if err != nil {
				return fmt.Errorf("invalid Git repository URL")
			}
			// A trailing dot makes an otherwise-identical hostname a valid,
			// distinct absolute DNS name ("github.com."), which DNS and Git
			// both still resolve to github.com -- so it must not let a
			// repository skip GitHub-specific validation below. It is
			// rejected outright rather than normalized and accepted: the
			// runner's credential helper matches the host Git supplies
			// exactly, so a validated-but-not-exactly-"github.com" URL would
			// pass validation here and then fail to authenticate at clone
			// time instead.
			host := u.Hostname()
			trimmedHost := strings.TrimSuffix(host, ".")
			if !strings.EqualFold(trimmedHost, "github.com") {
				continue
			}
			if host != trimmedHost {
				return fmt.Errorf("credentialed GitHub repository hostname must not have a trailing dot")
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
	return templateEnvValue(tmpl, "GITHUB_TOKEN")
}

// templateEnvValue extracts a named container env var from an ActorTemplate,
// or "" if it has none.
func templateEnvValue(tmpl *ateapipb.ActorTemplate, name string) string {
	for _, container := range tmpl.GetContainers() {
		for _, env := range container.GetEnv() {
			if env.GetName() == name {
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
// environment. It returns ("", nil) when neither source has a value, and a non-nil
// error only when the secret lookup itself failed in a way that isn't an
// authoritative "not configured" (see lookupSecret).
func (r *TaskReconciler) lookupGeminiKey(ctx context.Context, atespace string) (string, error) {
	key, err := r.lookupSecret(ctx, atespace, geminiSecretName, geminiSecretKey)
	if err != nil {
		return "", err
	}
	if key != "" {
		return key, nil
	}
	if key := os.Getenv(geminiSecretKey); key != "" {
		slog.Info("resolved GEMINI_API_KEY from server environment for actor template")
		return key, nil
	}
	return "", nil
}

// lookupSecret resolves a secret key, returning ("", nil) when it is
// authoritatively not configured (model.ErrSecretNotFound) and ("", err) for
// any other lookup failure (network, auth, transient infra), so a caller
// resuming an already-running actor can tell "genuinely unconfigured" (safe
// to proceed without the key) apart from "couldn't tell this round" (unsafe
// to recompute and switch away from a template that may already have it).
func (r *TaskReconciler) lookupSecret(ctx context.Context, atespace, secretName, secretKey string) (string, error) {
	if r.SecretResolver == nil {
		return "", nil
	}
	lookupCtx, cancel := context.WithTimeout(ctx, secretLookupTimeout)
	defer cancel()
	key, err := r.SecretResolver(lookupCtx, atespace, secretName, secretKey)
	if err != nil {
		if errors.Is(err, model.ErrSecretNotFound) {
			return "", nil
		}
		return "", fmt.Errorf("looking up secret %s/%s: %w", atespace, secretName, err)
	}
	if key == "" {
		return "", nil
	}
	slog.Info("resolved task API key from kubernetes secret for actor template", "atespace", atespace, "secret", secretName)
	return key, nil
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
	// Propagated, not merely logged: deleteTaskTemplates deliberately leaves
	// a template in place when reading or revoking its token failed, so a
	// retry can still reach it -- but DeleteTask only skips deleting the
	// task record when ReconcileDelete itself returns an error, and that
	// record is this template's only remaining path to a retry.
	if err := r.deleteTaskTemplates(ctx, atespace, taskName, hasCredentialProvider); err != nil {
		return fmt.Errorf("cleaning up actor templates: %w", err)
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
