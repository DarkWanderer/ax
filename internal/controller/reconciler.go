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
	geminiSecretName = "gemini-api-secret"
	geminiSecretKey  = "GEMINI_API_KEY"
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
	// credentialRevokeTimeout bounds revoking tokens whose template could not be created.
	credentialRevokeTimeout = 10 * time.Second
	// reasonCredentialIssueFailed is the Ready condition reason when a task's
	// credentials could not be put into its ActorTemplate.
	reasonCredentialIssueFailed = "CredentialIssueFailed"
)

// SecretResolver looks up a key from a Kubernetes secret in the given namespace.
type SecretResolver func(ctx context.Context, namespace, secretName, key string) (string, error)

// TaskReconciler reconciles Task resources by provisioning and orchestrating
// sandboxed Actors on Agent Substrate.
type TaskReconciler struct {
	client                  *substrate.Client
	httpClient              *http.Client
	defaultTemplate         string
	defaultTemplateAtespace string

	// SecretResolver resolves the Gemini API key for task containers. It defaults
	// to the Kubernetes secret lookup; tests replace it to avoid touching a cluster.
	SecretResolver SecretResolver

	// WorkspaceReadyTimeout bounds how long Reconcile waits for the actor's workspace
	// to report ready before recording it as still initializing.
	WorkspaceReadyTimeout time.Duration

	// Credentials issues the tokens of the credentials a task binds. It is only
	// needed while a task's ActorTemplate is being created: a template that
	// already exists holds its tokens.
	Credentials *credentials.Service
}

// NewTaskReconciler creates a new TaskReconciler.
func NewTaskReconciler(client *substrate.Client, defaultTemplate, defaultTemplateAtespace string) *TaskReconciler {
	if defaultTemplate == "" {
		defaultTemplate = "default-template"
	}
	if defaultTemplateAtespace == "" {
		defaultTemplateAtespace = v1alpha1.DefaultSystemNamespace
	}
	return &TaskReconciler{
		client:                  client,
		httpClient:              &http.Client{Timeout: 2 * time.Second},
		defaultTemplate:         defaultTemplate,
		defaultTemplateAtespace: defaultTemplateAtespace,
		SecretResolver:          model.GetKubernetesSecret,
		WorkspaceReadyTimeout:   defaultWorkspaceReadyTimeout,
	}
}

// Reconcile handles the reconciliation loop for a single Task. creds are the
// credentials the task binds; one that is missing from creds is an error only
// when the task's ActorTemplate has to be created.
func (r *TaskReconciler) Reconcile(ctx context.Context, task *v1alpha1.Task, workspaces []*v1alpha1.Workspace, creds []*v1alpha1.Credential) (*v1alpha1.Task, error) {
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
		if substrate.IsTransientError(err) {
			r.setNotReady(task, "SubstrateUnavailable", err.Error(), now)
			task.Status.Phase = "Pending"
			return task, fmt.Errorf("ensuring atespace: %w", err)
		}
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

	if geminiKey := r.lookupGeminiKey(ctx, atespace); geminiKey != "" {
		extraEnv[geminiSecretKey] = geminiKey
	}

	// Only launch configuration belongs in the template; status changes
	// must not create new golden snapshots.
	launchTask := proto.Clone(task).(*v1alpha1.Task)
	launchTask.Status = nil
	if taskYAML, err := yaml.Marshal(launchTask); err == nil {
		extraEnv["AX_TASK_YAML"] = string(taskYAML)
	}

	// Inject the bound Workspace specs as a multi-document YAML stream.
	if wsYAML, err := marshalWorkspaces(workspaces); err == nil && wsYAML != "" {
		extraEnv["AX_WORKSPACES_YAML"] = wsYAML
	}

	// If a custom image, workspace, or extra environment is specified, provision or use a dedicated ActorTemplate
	if task.Spec != nil && (task.Spec.Image != "" || len(extraEnv) > 0) {
		slog.Info("ensuring custom ActorTemplate for task", "image", task.Spec.Image)
		bindings := task.Spec.GetCredentials()
		customTemplateName := taskTemplateName(task.Metadata.Name, task.Spec.Image, extraEnv, bindings)

		var tmpl *ateapipb.ActorTemplate
		var err error
		if len(bindings) > 0 {
			tmpl, err = r.ensureCredentialedTemplate(ctx, task, creds, templateAtespace, templateName, atespace, customTemplateName, extraEnv)
			if err != nil {
				// The default template would start the task without its
				// credentials and let it fail later in a harder-to-diagnose way.
				r.setNotReady(task, reasonCredentialIssueFailed, err.Error(), now)
				// A timeout or an unavailable Substrate says nothing about the
				// credentials, so the task stays retryable like in the other steps.
				task.Status.Phase = "Failed"
				if substrate.IsTransientError(err) {
					task.Status.Phase = "Pending"
				}
				return task, fmt.Errorf("preparing task credentials: %w", err)
			}
		} else {
			tmpl, err = r.client.EnsureActorTemplateWithImage(ctx, templateAtespace, templateName, atespace, customTemplateName, task.Spec.Image, extraEnv)
			if err != nil {
				slog.Warn("could not create custom ActorTemplate, falling back to default template", "error", err)
			}
		}
		if err == nil && tmpl != nil && tmpl.Metadata != nil {
			templateAtespace = tmpl.Metadata.Atespace
			templateName = tmpl.Metadata.Name
			slog.Info("using custom ActorTemplate for actor", "templateAtespace", templateAtespace, "templateName", templateName)
		}
	}

	_, err := r.client.EnsureActor(ctx, atespace, actorName, templateAtespace, templateName)
	if err != nil {
		if substrate.IsTransientError(err) {
			r.setNotReady(task, "SubstrateUnavailable", err.Error(), now)
			task.Status.Phase = "Pending"
			return task, fmt.Errorf("ensuring actor: %w", err)
		}
		r.setNotReady(task, "ActorCreationFailed", err.Error(), now)
		task.Status.Phase = "Failed"
		return task, fmt.Errorf("ensuring actor: %w", err)
	}

	// 4. Suspend or Resume the Actor
	// Tasks are suspended by default upon creation until explicitly resumed to "Running".
	if task.Status.Phase == "Suspended" || task.Status.Phase == "" {
		slog.Info("suspending actor on Substrate", "actor", actorName)
		if err := r.client.SuspendActor(ctx, atespace, actorName); err != nil {
			return task, fmt.Errorf("suspending actor: %w", err)
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
		if substrate.IsTransientError(err) {
			r.setNotReady(task, "SubstrateUnavailable", err.Error(), now)
			task.Status.Phase = "Pending"
			return task, fmt.Errorf("resuming actor: %w", err)
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
	readyURL := fmt.Sprintf("http://%s/readyz?check=workspace", net.JoinHostPort(host, port))
	// Workspace setup happens once per task. After it has completed, WorkspaceReady stays
	// True across suspend/resume cycles, so only poll while it is still initializing.
	workspaceReady := r.conditionTrue(task, condWorkspaceReady)
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

		for !workspaceReady {
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
	if r.SecretResolver != nil {
		lookupCtx, cancel := context.WithTimeout(ctx, secretLookupTimeout)
		defer cancel()
		if key, err := r.SecretResolver(lookupCtx, atespace, geminiSecretName, geminiSecretKey); err == nil && key != "" {
			slog.Info("resolved GEMINI_API_KEY from kubernetes secret for actor template", "atespace", atespace)
			return key
		}
	}
	if key := os.Getenv(geminiSecretKey); key != "" {
		slog.Info("resolved GEMINI_API_KEY from server environment for actor template")
		return key
	}
	return ""
}

// taskTemplateName derives the per-task ActorTemplate name from the task name and a
// digest of the image, the container environment and the credential bindings, so a
// spec change yields a new template. Token values are deliberately not part of the
// digest: they change on every issue, and the template must stay put once created.
func taskTemplateName(taskName, image string, env map[string]string, bindings []*v1alpha1.CredentialBinding) string {
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
	// Written only when there are bindings, so the name of a task without any is
	// the one it had before credentials existed and no template is recreated.
	pairs := make([]string, 0, len(bindings))
	for _, b := range bindings {
		pairs = append(pairs, b.GetName()+"\x00"+b.GetEnv())
	}
	sort.Strings(pairs)
	for _, p := range pairs {
		h.Write([]byte("credential:" + p + ";"))
	}
	return fmt.Sprintf("%s-tmpl-%x", taskName, h.Sum(nil)[:templateDigestBytes])
}

// issuedToken is a token handed out for a bound credential.
type issuedToken struct {
	cred *v1alpha1.Credential
	tok  credentials.Token
}

// ensureCredentialedTemplate returns the task's ActorTemplate, creating it with
// the bound credentials' tokens as environment when it does not exist yet.
//
// A template that exists already holds its tokens, so resuming a task does not
// mint new ones. That bounds the tokens to one per template, at the price that
// they are not renewed (see docs/credentials.md).
func (r *TaskReconciler) ensureCredentialedTemplate(ctx context.Context, task *v1alpha1.Task, creds []*v1alpha1.Credential,
	baseAtespace, baseTemplate, atespace, name string, extraEnv map[string]string) (*ateapipb.ActorTemplate, error) {
	existing, err := r.client.GetActorTemplate(ctx, atespace, name)
	switch {
	case err == nil && existing != nil:
		return existing, nil
	case err != nil && status.Code(err) != codes.NotFound:
		return nil, fmt.Errorf("looking up actor template %s/%s: %w", atespace, name, err)
	}

	issued, err := r.issueBound(ctx, task.Spec.GetCredentials(), creds)
	if err != nil {
		return nil, err
	}

	// The tokens are applied last so that they win over any other source of the
	// same variable name.
	env := make(map[string]string, len(extraEnv)+len(task.Spec.GetCredentials()))
	for k, v := range extraEnv {
		env[k] = v
	}
	for _, b := range task.Spec.GetCredentials() {
		env[b.GetEnv()] = issued[b.GetName()].tok.Value
	}

	tmpl, err := r.client.EnsureActorTemplateWithImage(ctx, baseAtespace, baseTemplate, atespace, name, task.Spec.Image, env)
	if err != nil {
		// Nothing will ever hold these tokens, so do not leave them valid. The
		// caller's context may be what failed, hence a fresh one.
		r.revokeIssued(ctx, issued)
		return nil, err
	}
	return tmpl, nil
}

// issueBound issues one token for each distinct credential the bindings name,
// keyed by credential name. If any fails, those already issued are revoked.
func (r *TaskReconciler) issueBound(ctx context.Context, bindings []*v1alpha1.CredentialBinding, creds []*v1alpha1.Credential) (map[string]issuedToken, error) {
	byName := make(map[string]*v1alpha1.Credential, len(creds))
	for _, c := range creds {
		byName[c.GetMetadata().GetName()] = c
	}
	issued := make(map[string]issuedToken, len(bindings))
	fail := func(err error) (map[string]issuedToken, error) {
		r.revokeIssued(ctx, issued)
		return nil, err
	}
	for _, b := range bindings {
		if _, done := issued[b.GetName()]; done {
			continue
		}
		cred, ok := byName[b.GetName()]
		if !ok {
			return fail(fmt.Errorf("credential %q not found", b.GetName()))
		}
		if r.Credentials == nil {
			return fail(errors.New("credentials are not configured on this server"))
		}
		tok, err := r.Credentials.Issue(ctx, cred)
		if err != nil {
			return fail(fmt.Errorf("issuing a token for credential %q: %w", b.GetName(), err))
		}
		issued[b.GetName()] = issuedToken{cred: cred, tok: tok}
	}
	return issued, nil
}

// revokeIssued revokes tokens that ended up unused. It runs on a context that is
// not canceled with the caller's but is bounded, and it only logs failures: the
// error being reported is the one that made the tokens unused.
func (r *TaskReconciler) revokeIssued(ctx context.Context, issued map[string]issuedToken) {
	if len(issued) == 0 {
		return
	}
	revokeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), credentialRevokeTimeout)
	defer cancel()
	for name, it := range issued {
		if _, err := r.Credentials.Revoke(revokeCtx, it.cred, it.tok); err != nil {
			slog.Warn("could not revoke an unused credential token", "credential", name, "error", err)
		}
	}
}

// taskTemplatePattern matches every ActorTemplate name taskTemplateName can produce
// for the given task, across all spec revisions.
func taskTemplatePattern(taskName string) *regexp.Regexp {
	return regexp.MustCompile(fmt.Sprintf("^%s-tmpl-[0-9a-f]{%d}$", regexp.QuoteMeta(taskName), 2*templateDigestBytes))
}

// ReconcileDelete cleans up Substrate resources when a Task is deleted: the actor,
// which shares the task's name, then every ActorTemplate provisioned for the task.
func (r *TaskReconciler) ReconcileDelete(ctx context.Context, atespace, taskName string) error {
	if atespace == "" {
		atespace = "default"
	}
	slog.Info("deleting Substrate actor for task", "atespace", atespace, "task", taskName)
	if err := r.client.DeleteActor(ctx, atespace, taskName); err != nil {
		return err
	}
	if err := r.deleteTaskTemplates(ctx, atespace, taskName); err != nil {
		slog.Warn("could not clean up actor templates for task", "task", taskName, "error", err)
	}
	return nil
}

// deleteTaskTemplates removes all ActorTemplates belonging to the task. Templates
// accumulate across spec revisions, so this matches by name pattern rather than
// recomputing a single digest. If an actor deletion is still finishing in Substrate,
// template deletion may briefly return Aborted, so we retry with backoff.
func (r *TaskReconciler) deleteTaskTemplates(ctx context.Context, atespace, taskName string) error {
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
