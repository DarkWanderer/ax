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

package controller_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/ax/internal/controller"
	"github.com/google/ax/internal/substrate"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type fakeInstallationTokens struct {
	minted       int
	revoked      []string
	repositories []string
	revokeFails  int             // the next N Revoke calls return an error instead of succeeding
	revokeErrFor map[string]bool // Revoke fails for exactly these tokens, regardless of call order
}

func (f *fakeInstallationTokens) Mint(_ context.Context, app *v1alpha1.GitHubAppCredential, _ string) (string, error) {
	f.minted++
	f.repositories = append([]string(nil), app.GetRepositories()...)
	return "ghs_test_token_" + string(rune('0'+f.minted)), nil
}
func (f *fakeInstallationTokens) Revoke(_ context.Context, token string) error {
	if f.revokeFails > 0 {
		f.revokeFails--
		return errors.New("transient revoke error")
	}
	if f.revokeErrFor[token] {
		return errors.New("transient revoke error")
	}
	f.revoked = append(f.revoked, token)
	return nil
}

func TestCredentialedTaskLifecycleAcrossWorkspaces(t *testing.T) {
	ctx := context.Background()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	mock := &mockControlServer{}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mock)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()
	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	r := controller.NewTaskReconciler(client, "test-template", "ax-system")
	r.SecretResolver = func(_ context.Context, atespace, name, key string) (string, error) {
		if name != "app-key" {
			return "", nil
		}
		if atespace != "team" || key != "pem" {
			t.Fatalf("wrong secret lookup %s/%s/%s", atespace, name, key)
		}
		return "private-key", nil
	}
	fake := &fakeInstallationTokens{}
	r.InstallationTokens = fake
	provider := &v1alpha1.CredentialProvider{Metadata: &v1alpha1.ObjectMeta{Name: "github", Atespace: "team"}, Spec: &v1alpha1.CredentialProviderSpec{GithubApp: &v1alpha1.GitHubAppCredential{AppId: 1, InstallationId: 2, PrivateKeySecret: &v1alpha1.SecretKeyRef{Name: "app-key", Key: "pem"}, Repositories: []string{"first", "second", "third"}, Permissions: map[string]string{"contents": "read"}}}}
	task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "job", Atespace: "team"}, Spec: &v1alpha1.TaskSpec{CredentialProvider: &v1alpha1.CredentialProviderRef{Name: "github"}, Workspaces: []*v1alpha1.WorkspaceRef{{Name: "one"}, {Name: "two"}}}, Status: &v1alpha1.TaskStatus{Conditions: []*v1alpha1.Condition{{Type: "WorkspaceReady", Status: "True"}}}}
	ws1 := &v1alpha1.Workspace{Metadata: &v1alpha1.ObjectMeta{Name: "one"}, Spec: &v1alpha1.WorkspaceSpec{Git: []*v1alpha1.GitRepo{{Repo: "https://github.com/org/first.git"}, {Repo: "https://github.com/org/second.git"}}}}
	ws2 := &v1alpha1.Workspace{Metadata: &v1alpha1.ObjectMeta{Name: "two"}, Spec: &v1alpha1.WorkspaceSpec{Git: []*v1alpha1.GitRepo{{Repo: "https://github.com/org/third.git"}}}}
	ws2.Spec.Git[0].Repo = "https://github.com/org/unlisted-private.git"
	if _, err := r.ReconcileWithProvider(ctx, task, provider, ws1, ws2); err == nil || fake.minted != 0 {
		t.Fatalf("unlisted repository should fail before mint: %v", err)
	}
	ws2.Spec.Git[0].Repo = "https://github.com/org/third.git"
	if _, err := r.ReconcileWithProvider(ctx, task, provider, ws1, ws2); err != nil {
		t.Fatal(err)
	}
	if fake.minted != 1 || len(fake.repositories) != 3 {
		t.Fatalf("minted=%d repos=%v", fake.minted, fake.repositories)
	}
	initialTemplate := mock.actor.GetActorTemplate().GetName()
	var workspaceYAML, taskYAML, token string
	for _, e := range mock.createdTemplates[0].GetContainers()[0].GetEnv() {
		switch e.GetName() {
		case "AX_WORKSPACES_YAML":
			workspaceYAML = e.GetValue()
		case "AX_TASK_YAML":
			taskYAML = e.GetValue()
		case "GITHUB_TOKEN":
			token = e.GetValue()
		}
	}
	for _, repo := range []string{"first.git", "second.git", "third.git"} {
		if !strings.Contains(workspaceYAML, repo) {
			t.Errorf("workspace YAML missing %s", repo)
		}
	}
	if token == "" || strings.Contains(taskYAML, token) {
		t.Fatal("token missing from template or present in Task YAML")
	}
	for _, e := range mock.createdTemplates[0].GetContainers()[0].GetEnv() {
		if e.GetName() == "GITHUB_TOKEN" {
			e.Value = ""
			if _, err := r.ReconcileWithProvider(ctx, task, provider, ws1, ws2); err == nil {
				t.Fatal("running actor without token used uncredentialed fallback")
			}
			e.Value = token
			break
		}
	}
	for _, e := range mock.createdTemplates[0].GetContainers()[0].GetEnv() {
		if strings.Contains(e.GetValue(), "private-key") {
			t.Fatal("GitHub App private key entered actor template")
		}
	}
	if _, err := r.ReconcileWithProvider(ctx, task, provider, ws1, ws2); err != nil {
		t.Fatal(err)
	}
	if fake.minted != 1 {
		t.Fatalf("reconciliation minted %d tokens", fake.minted)
	}
	task.Status.Phase = "Suspended"
	if _, err := r.ReconcileWithProvider(ctx, task, provider, ws1, ws2); err != nil {
		t.Fatal(err)
	}
	if len(fake.revoked) != 1 || fake.revoked[0] != token {
		t.Fatalf("revoked=%v", fake.revoked)
	}
	// Revoke is idempotent, so a repeat suspend revoking the same
	// already-void token again is harmless and expected.
	if _, err := r.ReconcileWithProvider(ctx, task, provider, ws1, ws2); err != nil {
		t.Fatal(err)
	}
	if len(fake.revoked) != 2 || fake.revoked[1] != token {
		t.Fatalf("revoked=%v, want the same token revoked again", fake.revoked)
	}
	task.Status.Phase = "Running"
	if _, err := r.ReconcileWithProvider(ctx, task, provider, ws1, ws2); err != nil {
		t.Fatal(err)
	}
	if fake.minted != 2 || mock.actor.GetActorTemplate().GetName() == initialTemplate {
		t.Fatalf("resume did not rotate template: mint=%d template=%s", fake.minted, mock.actor.GetActorTemplate().GetName())
	}
	if len(mock.createdActors) != 1 {
		t.Fatalf("durable actor was recreated: %v", mock.createdActors)
	}
}

// TestCredentialedTaskCreatedSuspendedRevokesUnusedToken covers a task created
// with a credential provider and no explicit resume: it starts out suspended
// (Status.Phase is unset), so its actor is created and immediately suspended
// without ever running. The token minted for it must not be left valid.
func TestCredentialedTaskCreatedSuspendedRevokesUnusedToken(t *testing.T) {
	ctx := context.Background()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	mock := &mockControlServer{}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mock)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()
	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	r := controller.NewTaskReconciler(client, "test-template", "ax-system")
	r.SecretResolver = func(_ context.Context, _, _, _ string) (string, error) { return "private-key", nil }
	fake := &fakeInstallationTokens{}
	r.InstallationTokens = fake
	provider := &v1alpha1.CredentialProvider{Metadata: &v1alpha1.ObjectMeta{Name: "github", Atespace: "team"}, Spec: &v1alpha1.CredentialProviderSpec{GithubApp: &v1alpha1.GitHubAppCredential{AppId: 1, InstallationId: 2, PrivateKeySecret: &v1alpha1.SecretKeyRef{Name: "app-key", Key: "pem"}, Repositories: []string{"repo"}, Permissions: map[string]string{"contents": "read"}}}}
	task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "job", Atespace: "team"}, Spec: &v1alpha1.TaskSpec{CredentialProvider: &v1alpha1.CredentialProviderRef{Name: "github"}}}

	got, err := r.ReconcileWithProvider(ctx, task, provider)
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStatus().GetPhase() != "Suspended" {
		t.Fatalf("phase = %q, want Suspended", got.GetStatus().GetPhase())
	}
	if fake.minted != 1 {
		t.Fatalf("minted %d tokens, want 1", fake.minted)
	}
	if len(fake.revoked) != 1 {
		t.Fatalf("revoked %v, want the one minted token revoked", fake.revoked)
	}
	if len(mock.suspendedActors) != 1 {
		t.Fatalf("suspendedActors=%v, want the actor suspended once", mock.suspendedActors)
	}

	// A repeat reconcile in the same suspended state revokes again; that's
	// harmless (Revoke is idempotent) and cheaper than trying to track
	// "already revoked" in status that a failed write could lose.
	if _, err := r.ReconcileWithProvider(ctx, task, provider); err != nil {
		t.Fatal(err)
	}
	if len(fake.revoked) != 2 {
		t.Fatalf("revoked=%v, want the repeat suspend to revoke again", fake.revoked)
	}
}

// revokeCtxCapturingTokens wraps fakeInstallationTokens to record the exact
// context.Context passed to each Revoke call, so a test can tell whether
// production code reused the caller's own context or, as it must for a
// cleanup revoke, built a fresh one instead.
type revokeCtxCapturingTokens struct {
	fakeInstallationTokens
	revokeCtxs []context.Context
}

func (c *revokeCtxCapturingTokens) Revoke(ctx context.Context, token string) error {
	c.revokeCtxs = append(c.revokeCtxs, ctx)
	return c.fakeInstallationTokens.Revoke(ctx, token)
}

type callerCtxKey struct{}

// TestCredentialedTaskCreatedSuspendedRevokesWithFreshContext covers a
// credentialed CreateTask that starts out suspended: the token minted for
// its (never-run) actor is revoked right after SuspendActor succeeds. If that
// revoke reused the caller's own request context -- which may be close to
// its deadline right after a real Substrate SuspendActor call, itself
// possibly slow -- losing it to a now-exhausted context would report the
// whole call failed, with the task record never created, even though the
// actor really is suspended and the token is the only thing left live. The
// revoke must use its own fresh, independently bounded context instead, the
// same way suspendAndRevoke already does for its own revoke.
func TestCredentialedTaskCreatedSuspendedRevokesWithFreshContext(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	mock := &mockControlServer{}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mock)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()
	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	r := controller.NewTaskReconciler(client, "test-template", "ax-system")
	r.SecretResolver = func(_ context.Context, _, _, _ string) (string, error) { return "private-key", nil }
	tokens := &revokeCtxCapturingTokens{}
	r.InstallationTokens = tokens
	provider := &v1alpha1.CredentialProvider{Metadata: &v1alpha1.ObjectMeta{Name: "github", Atespace: "team"}, Spec: &v1alpha1.CredentialProviderSpec{GithubApp: &v1alpha1.GitHubAppCredential{AppId: 1, InstallationId: 2, PrivateKeySecret: &v1alpha1.SecretKeyRef{Name: "app-key", Key: "pem"}, Repositories: []string{"repo"}, Permissions: map[string]string{"contents": "read"}}}}
	task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "job", Atespace: "team"}, Spec: &v1alpha1.TaskSpec{CredentialProvider: &v1alpha1.CredentialProviderRef{Name: "github"}}}

	ctx := context.WithValue(context.Background(), callerCtxKey{}, "caller")
	got, err := r.ReconcileWithProvider(ctx, task, provider)
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStatus().GetPhase() != "Suspended" {
		t.Fatalf("phase = %q, want Suspended", got.GetStatus().GetPhase())
	}
	if len(tokens.revokeCtxs) != 1 {
		t.Fatalf("revoked %d times, want 1", len(tokens.revokeCtxs))
	}
	if tokens.revokeCtxs[0].Value(callerCtxKey{}) != nil {
		t.Fatal("revoke used the caller's own context instead of a fresh, independently bounded one")
	}
	if _, ok := tokens.revokeCtxs[0].Deadline(); !ok {
		t.Fatal("revoke's context has no deadline; want it bounded like suspendAndRevoke's cleanup context")
	}
}

// TestCredentialedCreateDoesNotRevokeOnFailedSuspend covers a fresh,
// credentialed actor whose initial SuspendActor call fails: the actor may
// still be running with the just-minted token, so it must not be revoked
// (and the task must not be reported safely Suspended) until suspension is
// confirmed to have actually happened.
func TestCredentialedCreateDoesNotRevokeOnFailedSuspend(t *testing.T) {
	ctx := context.Background()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	mock := &mockControlServer{suspendActorErr: errors.New("transient substrate error")}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mock)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()
	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	r := controller.NewTaskReconciler(client, "test-template", "ax-system")
	r.SecretResolver = func(_ context.Context, _, _, _ string) (string, error) { return "private-key", nil }
	fake := &fakeInstallationTokens{}
	r.InstallationTokens = fake
	provider := &v1alpha1.CredentialProvider{Metadata: &v1alpha1.ObjectMeta{Name: "github", Atespace: "team"}, Spec: &v1alpha1.CredentialProviderSpec{GithubApp: &v1alpha1.GitHubAppCredential{AppId: 1, InstallationId: 2, PrivateKeySecret: &v1alpha1.SecretKeyRef{Name: "app-key", Key: "pem"}, Repositories: []string{"repo"}, Permissions: map[string]string{"contents": "read"}}}}
	task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "job", Atespace: "team"}, Spec: &v1alpha1.TaskSpec{CredentialProvider: &v1alpha1.CredentialProviderRef{Name: "github"}}}

	got, err := r.ReconcileWithProvider(ctx, task, provider)
	if err == nil {
		t.Fatal("expected the failed suspend to be reported as an error")
	}
	if got.GetStatus().GetPhase() != "Failed" {
		t.Fatalf("phase = %q, want Failed", got.GetStatus().GetPhase())
	}
	if len(fake.revoked) != 0 {
		t.Fatalf("revoked=%v, want no revoke while suspension is unconfirmed", fake.revoked)
	}
}

// TestResumeRevokesStaleTokenFromFailedSuspend covers a suspend that reaches
// Substrate (the actor really is SUSPENDED) but whose token revoke fails
// transiently. A later resume must revoke that stale, still-live token before
// minting its replacement: GitHub allows multiple concurrent installation
// tokens, so simply minting a new one would leave the old one valid until its
// own expiry.
func TestResumeRevokesStaleTokenFromFailedSuspend(t *testing.T) {
	ctx := context.Background()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	mock := &mockControlServer{}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mock)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()
	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	r := controller.NewTaskReconciler(client, "test-template", "ax-system")
	r.SecretResolver = func(_ context.Context, _, _, _ string) (string, error) { return "private-key", nil }
	fake := &fakeInstallationTokens{}
	r.InstallationTokens = fake
	provider := &v1alpha1.CredentialProvider{Metadata: &v1alpha1.ObjectMeta{Name: "github", Atespace: "team"}, Spec: &v1alpha1.CredentialProviderSpec{GithubApp: &v1alpha1.GitHubAppCredential{AppId: 1, InstallationId: 2, PrivateKeySecret: &v1alpha1.SecretKeyRef{Name: "app-key", Key: "pem"}, Repositories: []string{"repo"}, Permissions: map[string]string{"contents": "read"}}}}
	task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "job", Atespace: "team"}, Spec: &v1alpha1.TaskSpec{CredentialProvider: &v1alpha1.CredentialProviderRef{Name: "github"}}, Status: &v1alpha1.TaskStatus{Conditions: []*v1alpha1.Condition{{Type: "WorkspaceReady", Status: "True"}}}}

	// Resume once so the actor is running with a live token, then suspend it:
	// the actor genuinely reaches SUSPENDED, but the revoke fails.
	task.Status.Phase = "Running"
	if _, err := r.ReconcileWithProvider(ctx, task, provider); err != nil {
		t.Fatal(err)
	}
	fake.revokeFails = 1
	task.Status.Phase = "Suspended"
	if _, err := r.ReconcileWithProvider(ctx, task, provider); err == nil {
		t.Fatal("expected the failed revoke to be reported as an error")
	}
	if len(mock.suspendedActors) != 1 {
		t.Fatalf("suspendedActors=%v, want the actor actually suspended despite the revoke failure", mock.suspendedActors)
	}
	if len(fake.revoked) != 0 {
		t.Fatalf("revoked=%v, want none recorded after a failed revoke", fake.revoked)
	}

	// Resume: must revoke the still-live token from the failed suspend before
	// minting the replacement.
	task.Status.Phase = "Running"
	if _, err := r.ReconcileWithProvider(ctx, task, provider); err != nil {
		t.Fatal(err)
	}
	if fake.minted != 2 || len(fake.revoked) != 1 || fake.revoked[0] != "ghs_test_token_1" {
		t.Fatalf("minted=%d revoked=%v, want the stale first token revoked before minting the second", fake.minted, fake.revoked)
	}
}

// TestAmbiguousResumeFailureSuspendsActorBeforeRevoking covers a ResumeActor
// call whose response is lost even though Substrate actually resumed the
// actor: the reconciler must force the actor back to SUSPENDED (not merely
// revoke the now-dead token it was left running with), so a subsequent
// resume attempt sees SUSPENDED and mints and applies a fresh token, instead
// of seeing RUNNING and skipping rotation forever.
func TestAmbiguousResumeFailureSuspendsActorBeforeRevoking(t *testing.T) {
	ctx := context.Background()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	mock := &mockControlServer{resumeActorErr: errors.New("deadline exceeded")}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mock)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()
	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	r := controller.NewTaskReconciler(client, "test-template", "ax-system")
	r.SecretResolver = func(_ context.Context, _, _, _ string) (string, error) { return "private-key", nil }
	fake := &fakeInstallationTokens{}
	r.InstallationTokens = fake
	provider := &v1alpha1.CredentialProvider{Metadata: &v1alpha1.ObjectMeta{Name: "github", Atespace: "team"}, Spec: &v1alpha1.CredentialProviderSpec{GithubApp: &v1alpha1.GitHubAppCredential{AppId: 1, InstallationId: 2, PrivateKeySecret: &v1alpha1.SecretKeyRef{Name: "app-key", Key: "pem"}, Repositories: []string{"repo"}, Permissions: map[string]string{"contents": "read"}}}}
	task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "job", Atespace: "team"}, Spec: &v1alpha1.TaskSpec{CredentialProvider: &v1alpha1.CredentialProviderRef{Name: "github"}}, Status: &v1alpha1.TaskStatus{Conditions: []*v1alpha1.Condition{{Type: "WorkspaceReady", Status: "True"}}}}

	// Create the actor (starts suspended), then attempt to resume it: the
	// mock flips it to RUNNING server-side but still reports an error.
	task.Status.Phase = "Running"
	if _, err := r.ReconcileWithProvider(ctx, task, provider); err == nil {
		t.Fatal("expected the ambiguous resume failure to be reported as an error")
	}
	if len(mock.suspendedActors) != 1 {
		t.Fatalf("suspendedActors=%v, want the actor forced back to SUSPENDED after the ambiguous resume failure", mock.suspendedActors)
	}
	if len(fake.revoked) != 1 || fake.revoked[0] != "ghs_test_token_1" {
		t.Fatalf("revoked=%v, want the token left on the actor revoked", fake.revoked)
	}

	// Retry the resume, now that Substrate genuinely resumes: it must rotate
	// to a fresh token rather than reusing the revoked one.
	mock.resumeActorErr = nil
	task.Status.Phase = "Suspended"
	if _, err := r.ReconcileWithProvider(ctx, task, provider); err != nil {
		t.Fatalf("re-suspend failed: %v", err)
	}
	task.Status.Phase = "Running"
	got, err := r.ReconcileWithProvider(ctx, task, provider)
	if err != nil {
		t.Fatalf("retry after ambiguous resume failure: %v", err)
	}
	if got.GetStatus().GetPhase() != "Running" {
		t.Fatalf("phase = %q, want Running", got.GetStatus().GetPhase())
	}
	if fake.minted != 2 {
		t.Fatalf("minted=%d, want a fresh token minted on the successful retry", fake.minted)
	}
}

// TestAmbiguousResumeFailureKeepsTokenWhenSuspendFails covers the case where
// the suspend attempted after an ambiguous resume failure itself fails (as it
// would if resume's own failure were a canceled/expired context, which the
// suspend must not reuse): the token must not be revoked in that case, since
// the actor's state was never confirmed and a running actor is better left
// with a still-valid token than a dead one.
func TestAmbiguousResumeFailureKeepsTokenWhenSuspendFails(t *testing.T) {
	ctx := context.Background()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	mock := &mockControlServer{resumeActorErr: errors.New("deadline exceeded"), suspendActorErr: errors.New("transient substrate error")}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mock)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()
	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	r := controller.NewTaskReconciler(client, "test-template", "ax-system")
	r.SecretResolver = func(_ context.Context, _, _, _ string) (string, error) { return "private-key", nil }
	fake := &fakeInstallationTokens{}
	r.InstallationTokens = fake
	provider := &v1alpha1.CredentialProvider{Metadata: &v1alpha1.ObjectMeta{Name: "github", Atespace: "team"}, Spec: &v1alpha1.CredentialProviderSpec{GithubApp: &v1alpha1.GitHubAppCredential{AppId: 1, InstallationId: 2, PrivateKeySecret: &v1alpha1.SecretKeyRef{Name: "app-key", Key: "pem"}, Repositories: []string{"repo"}, Permissions: map[string]string{"contents": "read"}}}}
	task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "job", Atespace: "team"}, Spec: &v1alpha1.TaskSpec{CredentialProvider: &v1alpha1.CredentialProviderRef{Name: "github"}}, Status: &v1alpha1.TaskStatus{Conditions: []*v1alpha1.Condition{{Type: "WorkspaceReady", Status: "True"}}}}

	task.Status.Phase = "Running"
	if _, err := r.ReconcileWithProvider(ctx, task, provider); err == nil {
		t.Fatal("expected the ambiguous resume failure to be reported as an error")
	}
	if len(mock.suspendedActors) != 0 {
		t.Fatalf("suspendedActors=%v, want none recorded since SuspendActor itself errored", mock.suspendedActors)
	}
	if len(fake.revoked) != 0 {
		t.Fatalf("revoked=%v, want nothing revoked while the actor's state is unconfirmed", fake.revoked)
	}
}

// TestSuspendExistingActorIgnoresInvalidatedProvider covers suspending an
// already-provisioned actor after its credential provider has been edited into
// something that would now fail validation (e.g. a dropped repository).
// Suspending only needs the token already baked into the actor, not a fresh
// mint or workspace access, so it must succeed regardless.
func TestSuspendExistingActorIgnoresInvalidatedProvider(t *testing.T) {
	ctx := context.Background()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	mock := &mockControlServer{}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mock)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()
	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	r := controller.NewTaskReconciler(client, "test-template", "ax-system")
	r.SecretResolver = func(_ context.Context, _, _, _ string) (string, error) { return "private-key", nil }
	fake := &fakeInstallationTokens{}
	r.InstallationTokens = fake
	provider := &v1alpha1.CredentialProvider{Metadata: &v1alpha1.ObjectMeta{Name: "github", Atespace: "team"}, Spec: &v1alpha1.CredentialProviderSpec{GithubApp: &v1alpha1.GitHubAppCredential{AppId: 1, InstallationId: 2, PrivateKeySecret: &v1alpha1.SecretKeyRef{Name: "app-key", Key: "pem"}, Repositories: []string{"repo"}, Permissions: map[string]string{"contents": "read"}}}}
	task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "job", Atespace: "team"}, Spec: &v1alpha1.TaskSpec{CredentialProvider: &v1alpha1.CredentialProviderRef{Name: "github"}}}

	// Create the actor first (starts suspended).
	if _, err := r.ReconcileWithProvider(ctx, task, provider); err != nil {
		t.Fatal(err)
	}

	// Edit the provider into something that would now fail ValidateCredentialProvider.
	provider.Spec.GithubApp.Repositories = nil

	task.Status.Phase = "Suspended"
	got, err := r.ReconcileWithProvider(ctx, task, provider)
	if err != nil {
		t.Fatalf("suspend failed against an invalidated provider: %v", err)
	}
	if got.GetStatus().GetPhase() != "Suspended" {
		t.Fatalf("phase = %q, want Suspended", got.GetStatus().GetPhase())
	}
}

// TestSuspendExistingActorWithDeletedProvider covers the server handing the
// reconciler a nil provider because the CredentialProvider was deleted while
// referenced by a task: suspending its already-provisioned actor must still
// go through, revoking the token already baked into it, rather than silently
// skipping the whole credentialed path because provider is nil.
func TestSuspendExistingActorWithDeletedProvider(t *testing.T) {
	ctx := context.Background()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	mock := &mockControlServer{}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mock)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()
	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	r := controller.NewTaskReconciler(client, "test-template", "ax-system")
	r.SecretResolver = func(_ context.Context, _, _, _ string) (string, error) { return "private-key", nil }
	fake := &fakeInstallationTokens{}
	r.InstallationTokens = fake
	provider := &v1alpha1.CredentialProvider{Metadata: &v1alpha1.ObjectMeta{Name: "github", Atespace: "team"}, Spec: &v1alpha1.CredentialProviderSpec{GithubApp: &v1alpha1.GitHubAppCredential{AppId: 1, InstallationId: 2, PrivateKeySecret: &v1alpha1.SecretKeyRef{Name: "app-key", Key: "pem"}, Repositories: []string{"repo"}, Permissions: map[string]string{"contents": "read"}}}}
	task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "job", Atespace: "team"}, Spec: &v1alpha1.TaskSpec{CredentialProvider: &v1alpha1.CredentialProviderRef{Name: "github"}}, Status: &v1alpha1.TaskStatus{Conditions: []*v1alpha1.Condition{{Type: "WorkspaceReady", Status: "True"}}}}

	// Create suspended, then resume so the actor is running with a live,
	// unrevoked token (mirrors the server's fetchCredentialProvider still
	// finding the provider at this point).
	if _, err := r.ReconcileWithProvider(ctx, task, provider); err != nil {
		t.Fatal(err)
	}
	task.Status.Phase = "Running"
	if _, err := r.ReconcileWithProvider(ctx, task, provider); err != nil {
		t.Fatal(err)
	}
	// minted=2: the initial suspended create, then the resume's rotation.
	// revoked=2: the create-suspend's own token, then the old token revoked
	// just before the resume minted its replacement.
	if fake.minted != 2 || len(fake.revoked) != 2 {
		t.Fatalf("minted=%d revoked=%v before deletion scenario", fake.minted, fake.revoked)
	}

	// The provider is now deleted: the server passes a nil provider through.
	task.Status.Phase = "Suspended"
	got, err := r.ReconcileWithProvider(ctx, task, nil)
	if err != nil {
		t.Fatalf("suspend failed with a deleted provider: %v", err)
	}
	if got.GetStatus().GetPhase() != "Suspended" {
		t.Fatalf("phase = %q, want Suspended", got.GetStatus().GetPhase())
	}
	if len(fake.revoked) != 3 {
		t.Fatalf("revoked=%v, want the actor's live token revoked despite the deleted provider", fake.revoked)
	}
}

func TestCredentialedWorkspaceFailureFailsTask(t *testing.T) {
	ready := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusFailedDependency) }))
	defer ready.Close()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	mock := &mockControlServer{workerIP: strings.TrimPrefix(ready.URL, "http://")}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mock)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()
	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	r := controller.NewTaskReconciler(client, "test-template", "ax-system")
	r.SecretResolver = func(_ context.Context, _, name, _ string) (string, error) {
		if name == "key" {
			return "private-key", nil
		}
		return "", nil
	}
	r.InstallationTokens = &fakeInstallationTokens{}
	provider := &v1alpha1.CredentialProvider{Metadata: &v1alpha1.ObjectMeta{Name: "github", Atespace: "team"}, Spec: &v1alpha1.CredentialProviderSpec{GithubApp: &v1alpha1.GitHubAppCredential{AppId: 1, InstallationId: 2, PrivateKeySecret: &v1alpha1.SecretKeyRef{Name: "key", Key: "pem"}, Repositories: []string{"one"}, Permissions: map[string]string{"contents": "read"}}}}
	task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "job", Atespace: "team"}, Spec: &v1alpha1.TaskSpec{CredentialProvider: &v1alpha1.CredentialProviderRef{Name: "github"}}, Status: &v1alpha1.TaskStatus{Phase: "Running"}}
	got, err := r.ReconcileWithProvider(context.Background(), task, provider)
	if err == nil || got.GetStatus().GetPhase() != "Failed" {
		t.Fatalf("failed workspace was not reported: task=%v err=%v", got, err)
	}
}

// TestCredentialedWorkspaceFailureRetriesCleanupOnTransientError covers a
// workspace that reports failure (HTTP 424) while the cleanup step itself
// (here, GetActor) fails transiently: with no background reconciliation to
// pick this up later, a single failed cleanup attempt would otherwise leave
// the actor running indefinitely with a live installation token. The cleanup
// must retry rather than give up on the first failure.
func TestCredentialedWorkspaceFailureRetriesCleanupOnTransientError(t *testing.T) {
	ready := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusFailedDependency) }))
	defer ready.Close()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	mock := &mockControlServer{workerIP: strings.TrimPrefix(ready.URL, "http://")}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mock)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()
	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	r := controller.NewTaskReconciler(client, "test-template", "ax-system")
	r.CleanupRetryDelay = time.Millisecond
	r.SecretResolver = func(_ context.Context, _, name, _ string) (string, error) {
		if name == "key" {
			return "private-key", nil
		}
		return "", nil
	}
	r.InstallationTokens = &fakeInstallationTokens{}
	provider := &v1alpha1.CredentialProvider{Metadata: &v1alpha1.ObjectMeta{Name: "github", Atespace: "team"}, Spec: &v1alpha1.CredentialProviderSpec{GithubApp: &v1alpha1.GitHubAppCredential{AppId: 1, InstallationId: 2, PrivateKeySecret: &v1alpha1.SecretKeyRef{Name: "key", Key: "pem"}, Repositories: []string{"one"}, Permissions: map[string]string{"contents": "read"}}}}

	// Create (suspended) first, with the mock's normal GetActor behavior, so
	// the actor genuinely exists before the resume below -- only the resume's
	// own cleanup-path GetActor calls should see the injected failures.
	createTask := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "job", Atespace: "team"}, Spec: &v1alpha1.TaskSpec{CredentialProvider: &v1alpha1.CredentialProviderRef{Name: "github"}}}
	if _, err := r.ReconcileWithProvider(context.Background(), createTask, provider); err != nil {
		t.Fatalf("create: %v", err)
	}

	// GetActor fails transiently on its first two calls from here on (the
	// cleanup path's own calls), then succeeds from the third attempt onward.
	// The resume's own reconcile logic makes two legitimate GetActor calls of
	// its own before ever reaching the 424-triggered cleanup path; only the
	// cleanup path's calls (the third GetActor call onward) are made to fail
	// transiently, twice, before succeeding.
	const legitimateCallsBeforeCleanup = 2
	getActorCalls := 0
	cleanupGetActorCalls := 0
	mock.getActorFunc = func(ctx context.Context, req *ateapipb.GetActorRequest) (*ateapipb.Actor, error) {
		getActorCalls++
		name := req.GetActor().GetName()
		fetch := func() (*ateapipb.Actor, error) {
			if mock.actor != nil && mock.actor.GetMetadata().GetName() == name {
				return mock.actor, nil
			}
			return nil, status.Errorf(codes.NotFound, "actor %q not found", name)
		}
		if getActorCalls <= legitimateCallsBeforeCleanup {
			return fetch()
		}
		cleanupGetActorCalls++
		if cleanupGetActorCalls < 3 {
			return nil, status.Errorf(codes.Unavailable, "transient control plane error")
		}
		return fetch()
	}

	task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "job", Atespace: "team"}, Spec: &v1alpha1.TaskSpec{CredentialProvider: &v1alpha1.CredentialProviderRef{Name: "github"}}, Status: &v1alpha1.TaskStatus{Phase: "Running"}}
	got, err := r.ReconcileWithProvider(context.Background(), task, provider)
	if err == nil || got.GetStatus().GetPhase() != "Failed" {
		t.Fatalf("failed workspace was not reported: task=%v err=%v", got, err)
	}
	if cleanupGetActorCalls < 3 {
		t.Fatalf("cleanup gave up after %d GetActor attempts, want it to retry through the transient failures", cleanupGetActorCalls)
	}
	// suspendedActors has one entry from the initial create-and-suspend round
	// plus one more from this round's cleanup succeeding on its third attempt;
	// what matters is that it recorded a second suspend at all, rather than
	// giving up after the first two cleanup attempts failed.
	if len(mock.suspendedActors) != 2 {
		t.Fatalf("suspendedActors=%v, want a second suspend recorded once cleanup succeeded on retry", mock.suspendedActors)
	}
}

// TestCredentialedWorkspaceRejectsCaseVariantSCPURL covers a valid Git SCP-style
// GitHub URL spelled with a different host case (git@GitHub.com:...): it must
// still be rejected as needing HTTPS, not silently pass validation by going
// unrecognized as GitHub (url.Parse treats an SCP-style remote as a bare path,
// with no hostname to match).
func TestCredentialedWorkspaceRejectsCaseVariantSCPURL(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	mock := &mockControlServer{}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mock)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()
	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	r := controller.NewTaskReconciler(client, "test-template", "ax-system")
	r.SecretResolver = func(_ context.Context, _, _, _ string) (string, error) { return "private-key", nil }
	r.InstallationTokens = &fakeInstallationTokens{}
	provider := &v1alpha1.CredentialProvider{Metadata: &v1alpha1.ObjectMeta{Name: "github", Atespace: "team"}, Spec: &v1alpha1.CredentialProviderSpec{GithubApp: &v1alpha1.GitHubAppCredential{AppId: 1, InstallationId: 2, PrivateKeySecret: &v1alpha1.SecretKeyRef{Name: "key", Key: "pem"}, Repositories: []string{"repo"}, Permissions: map[string]string{"contents": "read"}}}}
	task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "job", Atespace: "team"}, Spec: &v1alpha1.TaskSpec{CredentialProvider: &v1alpha1.CredentialProviderRef{Name: "github"}}}
	ws := &v1alpha1.Workspace{Metadata: &v1alpha1.ObjectMeta{Name: "one"}, Spec: &v1alpha1.WorkspaceSpec{Git: []*v1alpha1.GitRepo{{Repo: "git@GitHub.com:org/repo.git"}}}}

	_, err = r.ReconcileWithProvider(context.Background(), task, provider, ws)
	if err == nil || !strings.Contains(err.Error(), "HTTPS") {
		t.Fatalf("case-variant SCP URL was not rejected: %v", err)
	}
}

// TestCredentialedWorkspaceRejectsGenericSCPURL covers Git's generic scp-like
// remote syntax, [user@]host:path (recognized whenever a colon precedes the
// first slash with no "scheme://" present): only the conventional
// "git@github.com:" spelling was rejected outright, but a bare
// "github.com:org/repo.git" (no user) or one with a different user also
// targets GitHub over SSH and must be rejected the same way, not fall through
// url.Parse with no recognizable hostname and skip validation entirely.
func TestCredentialedWorkspaceRejectsGenericSCPURL(t *testing.T) {
	for _, remote := range []string{
		"github.com:org/repo.git",
		"alice@github.com:org/repo.git",
		"GitHub.com:org/repo.git",
	} {
		t.Run(remote, func(t *testing.T) {
			lis, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer lis.Close()
			mock := &mockControlServer{}
			grpcServer := grpc.NewServer()
			ateapipb.RegisterControlServer(grpcServer, mock)
			go grpcServer.Serve(lis)
			defer grpcServer.Stop()
			client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			r := controller.NewTaskReconciler(client, "test-template", "ax-system")
			r.SecretResolver = func(_ context.Context, _, _, _ string) (string, error) { return "private-key", nil }
			r.InstallationTokens = &fakeInstallationTokens{}
			provider := &v1alpha1.CredentialProvider{Metadata: &v1alpha1.ObjectMeta{Name: "github", Atespace: "team"}, Spec: &v1alpha1.CredentialProviderSpec{GithubApp: &v1alpha1.GitHubAppCredential{AppId: 1, InstallationId: 2, PrivateKeySecret: &v1alpha1.SecretKeyRef{Name: "key", Key: "pem"}, Repositories: []string{"repo"}, Permissions: map[string]string{"contents": "read"}}}}
			task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "job", Atespace: "team"}, Spec: &v1alpha1.TaskSpec{CredentialProvider: &v1alpha1.CredentialProviderRef{Name: "github"}}}
			ws := &v1alpha1.Workspace{Metadata: &v1alpha1.ObjectMeta{Name: "one"}, Spec: &v1alpha1.WorkspaceSpec{Git: []*v1alpha1.GitRepo{{Repo: remote}}}}

			_, err = r.ReconcileWithProvider(context.Background(), task, provider, ws)
			if err == nil || !strings.Contains(err.Error(), "HTTPS") {
				t.Fatalf("generic SCP-style remote %q was not rejected: %v", remote, err)
			}
		})
	}
}

// TestCredentialedWorkspaceRejectsTrailingDotHostname covers the absolute-DNS
// form "github.com." (a valid, distinct hostname string that Git and DNS both
// still treat as github.com): without recognizing it before comparison, a
// repository on it would skip GitHub-specific validation entirely, letting an
// unlisted repository or one with embedded credentials through. It is
// rejected outright rather than normalized and accepted, since the runner's
// credential helper matches the host Git supplies exactly and would not
// recognize it either, failing authentication at clone time instead.
func TestCredentialedWorkspaceRejectsTrailingDotHostname(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	mock := &mockControlServer{}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mock)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()
	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	r := controller.NewTaskReconciler(client, "test-template", "ax-system")
	r.SecretResolver = func(_ context.Context, _, _, _ string) (string, error) { return "private-key", nil }
	r.InstallationTokens = &fakeInstallationTokens{}
	provider := &v1alpha1.CredentialProvider{Metadata: &v1alpha1.ObjectMeta{Name: "github", Atespace: "team"}, Spec: &v1alpha1.CredentialProviderSpec{GithubApp: &v1alpha1.GitHubAppCredential{AppId: 1, InstallationId: 2, PrivateKeySecret: &v1alpha1.SecretKeyRef{Name: "key", Key: "pem"}, Repositories: []string{"repo"}, Permissions: map[string]string{"contents": "read"}}}}
	task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "job", Atespace: "team"}, Spec: &v1alpha1.TaskSpec{CredentialProvider: &v1alpha1.CredentialProviderRef{Name: "github"}}}
	ws := &v1alpha1.Workspace{Metadata: &v1alpha1.ObjectMeta{Name: "one"}, Spec: &v1alpha1.WorkspaceSpec{Git: []*v1alpha1.GitRepo{{Repo: "https://github.com./org/private.git"}}}}

	_, err = r.ReconcileWithProvider(context.Background(), task, provider, ws)
	if err == nil || !strings.Contains(err.Error(), "trailing dot") {
		t.Fatalf("trailing-dot GitHub hostname was not rejected: %v", err)
	}
}

type mockControlServer struct {
	ateapipb.UnimplementedControlServer
	workerIP         string
	createdAtespaces []string
	createdActors    []string
	resumedActors    []string
	suspendedActors  []string
	deletedActors    []string
	actorTemplates   map[string]bool
	deletedTemplates []string
	getActorFunc     func(ctx context.Context, req *ateapipb.GetActorRequest) (*ateapipb.Actor, error)
	crashedActor     string
	revertedActors   []string
	createdTemplates []*ateapipb.ActorTemplate
	actor            *ateapipb.Actor
	suspendActorErr  error
	// resumeActorErr, when set, is returned to the caller of ResumeActor to
	// simulate an ambiguous failure: the actor is still flipped to RUNNING
	// server-side (as a real Substrate resume that actually succeeded would
	// leave it), but the client never sees that success.
	resumeActorErr error
	// onSuspendActor, when set, is called synchronously after SuspendActor
	// records success but before it returns, so a test can simulate the
	// caller's context expiring in the narrow window right after a real
	// Substrate suspend succeeds.
	onSuspendActor func()
}

// noSecrets is a SecretResolver for tests: it never finds a key and never touches a cluster.
func noSecrets(context.Context, string, string, string) (string, error) {
	return "", nil
}

func (m *mockControlServer) GetActorTemplate(_ context.Context, req *ateapipb.GetActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	ref := req.GetActorTemplate()
	for _, tmpl := range m.createdTemplates {
		if tmpl.GetMetadata().GetName() == ref.GetName() {
			return tmpl, nil
		}
	}
	if !m.actorTemplates[ref.GetName()] {
		return nil, status.Error(codes.NotFound, "template not found")
	}
	return &ateapipb.ActorTemplate{Metadata: &ateapipb.ResourceMetadata{Name: ref.GetName(), Atespace: ref.GetAtespace()}}, nil
}

func (m *mockControlServer) CreateActorTemplate(_ context.Context, req *ateapipb.CreateActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	if m.actorTemplates == nil {
		m.actorTemplates = make(map[string]bool)
	}
	tmpl := req.GetActorTemplate()
	m.actorTemplates[tmpl.GetMetadata().GetName()] = true
	m.createdTemplates = append(m.createdTemplates, tmpl)
	return tmpl, nil
}

func (m *mockControlServer) CreateAtespace(ctx context.Context, req *ateapipb.CreateAtespaceRequest) (*ateapipb.Atespace, error) {
	name := ""
	if req.Atespace != nil && req.Atespace.Metadata != nil {
		name = req.Atespace.Metadata.Name
	}
	m.createdAtespaces = append(m.createdAtespaces, name)
	return &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: name}}, nil
}

func (m *mockControlServer) CreateActor(ctx context.Context, req *ateapipb.CreateActorRequest) (*ateapipb.Actor, error) {
	if m.actor != nil {
		return nil, status.Error(codes.AlreadyExists, "exists")
	}
	name := ""
	if req.Actor != nil && req.Actor.Metadata != nil {
		name = req.Actor.Metadata.Name
	}
	if name == m.crashedActor {
		return nil, status.Error(codes.AlreadyExists, "actor exists")
	}
	m.createdActors = append(m.createdActors, name)
	m.actor = &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Name: name, Atespace: req.Actor.GetMetadata().GetAtespace()}, ActorTemplate: req.Actor.GetActorTemplate(),
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
		},
	}
	return m.actor, nil
}

func (m *mockControlServer) UpdateActor(_ context.Context, req *ateapipb.UpdateActorRequest) (*ateapipb.Actor, error) {
	m.actor = req.Actor
	return m.actor, nil
}

func (m *mockControlServer) ResumeActor(ctx context.Context, req *ateapipb.ResumeActorRequest) (*ateapipb.ResumeActorResponse, error) {
	name := ""
	if req.Actor != nil {
		name = req.Actor.Name
	}
	m.resumedActors = append(m.resumedActors, name)
	if m.actor != nil {
		m.actor.Status.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
	}
	if m.resumeActorErr != nil {
		return nil, m.resumeActorErr
	}
	wIP := "10.244.1.42"
	if m.workerIP != "" {
		wIP = m.workerIP
	}
	return &ateapipb.ResumeActorResponse{
		Actor: &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Name: name},
			Status: &ateapipb.ActorStatus{
				State: ateapipb.ActorState_ACTOR_STATE_RUNNING,
				WorkerAssignment: &ateapipb.WorkerAssignment{
					WorkerPod:   "worker-pod-1",
					WorkerPodIp: wIP,
				},
			},
		},
		Resumed: true,
	}, nil
}

func (m *mockControlServer) SuspendActor(ctx context.Context, req *ateapipb.SuspendActorRequest) (*ateapipb.SuspendActorResponse, error) {
	if m.suspendActorErr != nil {
		return nil, m.suspendActorErr
	}
	name := ""
	if req.Actor != nil {
		name = req.Actor.Name
	}
	if name == m.crashedActor {
		// Matches real Substrate: a crashed actor must be reverted (back to
		// SUSPENDED) before it will accept a suspend.
		return nil, status.Errorf(codes.FailedPrecondition, "actor %q is crashed; revert it first", name)
	}
	m.suspendedActors = append(m.suspendedActors, name)
	if m.actor != nil {
		m.actor.Status.State = ateapipb.ActorState_ACTOR_STATE_SUSPENDED
	}
	if m.onSuspendActor != nil {
		m.onSuspendActor()
	}
	return &ateapipb.SuspendActorResponse{}, nil
}

func (m *mockControlServer) RevertActor(_ context.Context, req *ateapipb.RevertActorRequest) (*ateapipb.RevertActorResponse, error) {
	name := req.GetActor().GetName()
	m.revertedActors = append(m.revertedActors, name)
	return &ateapipb.RevertActorResponse{Actor: &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Name: name},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	}}, nil
}

func (m *mockControlServer) DeleteActor(ctx context.Context, req *ateapipb.DeleteActorRequest) (*ateapipb.Actor, error) {
	name := req.GetActor().GetName()
	m.deletedActors = append(m.deletedActors, name)
	m.actor = nil
	return &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: name}}, nil
}

func (m *mockControlServer) GetActor(ctx context.Context, req *ateapipb.GetActorRequest) (*ateapipb.Actor, error) {
	if m.getActorFunc != nil {
		return m.getActorFunc(ctx, req)
	}
	name := req.GetActor().GetName()
	if name == m.crashedActor {
		return &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Name: name},
			Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_CRASHED},
		}, nil
	}
	for _, del := range m.deletedActors {
		if del == name {
			return nil, status.Errorf(codes.NotFound, "actor %q not found", name)
		}
	}
	if m.actor != nil && m.actor.GetMetadata().GetName() == name {
		return m.actor, nil
	}
	for _, a := range m.createdActors {
		if a == name {
			return &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Name: name},
				Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
			}, nil
		}
	}
	return nil, status.Errorf(codes.NotFound, "actor %q not found", name)
}

func (m *mockControlServer) ListActorTemplates(ctx context.Context, req *ateapipb.ListActorTemplatesRequest) (*ateapipb.ListActorTemplatesResponse, error) {
	resp := &ateapipb.ListActorTemplatesResponse{}
	for name := range m.actorTemplates {
		resp.ActorTemplates = append(resp.ActorTemplates, &ateapipb.ActorTemplate{
			Metadata: &ateapipb.ResourceMetadata{Name: name, Atespace: req.GetAtespace()},
		})
	}
	return resp, nil
}

func (m *mockControlServer) DeleteActorTemplate(ctx context.Context, req *ateapipb.DeleteActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	name := req.GetActorTemplate().GetName()
	delete(m.actorTemplates, name)
	m.deletedTemplates = append(m.deletedTemplates, name)
	return &ateapipb.ActorTemplate{Metadata: &ateapipb.ResourceMetadata{Name: name}}, nil
}

func TestTaskReconciler(t *testing.T) {
	ctx := context.Background()

	// 1. Start in-process mock gRPC Substrate server
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	mockSrv := &mockControlServer{}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mockSrv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	// 2. Initialize Substrate client
	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create substrate client: %v", err)
	}
	defer client.Close()

	// 3. Reconcile Task
	reconciler := controller.NewTaskReconciler(client, "test-template", "ax-system")
	reconciler.SecretResolver = noSecrets
	reconciler.WorkspaceReadyTimeout = 200 * time.Millisecond

	task := &v1alpha1.Task{
		ApiVersion: v1alpha1.APIVersion,
		Kind:       v1alpha1.KindTask,
		Metadata: &v1alpha1.ObjectMeta{
			Name:     "test-task",
			Atespace: "default",
		},
		Spec: &v1alpha1.TaskSpec{
			Image:   "ghrc.io/my-org/my-image",
			Command: []string{"/bin/task-runner"},
		},
		// A client-supplied actor name must not survive: the actor is always
		// named after the task.
		Status: &v1alpha1.TaskStatus{Actor: "not-the-task", Phase: "Running"},
	}

	reconciled, err := reconciler.Reconcile(ctx, task)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	// 4. Validate reconciliation results
	if reconciled.Status.Phase != "Running" {
		t.Errorf("expected phase 'Running', got %q", reconciled.Status.Phase)
	}
	if reconciled.Status.Actor != "test-task" {
		t.Errorf("expected actor 'test-task', got %q", reconciled.Status.Actor)
	}
	if reconciled.Status.WorkerIp != "10.244.1.42" {
		t.Errorf("expected worker IP '10.244.1.42', got %q", reconciled.Status.WorkerIp)
	}

	// Verify mock was called
	if len(mockSrv.createdAtespaces) != 1 || mockSrv.createdAtespaces[0] != "default" {
		t.Errorf("expected atespace 'default' created, got %v", mockSrv.createdAtespaces)
	}
	if len(mockSrv.createdActors) != 1 || mockSrv.createdActors[0] != "test-task" {
		t.Errorf("expected actor 'test-task' created, got %v", mockSrv.createdActors)
	}
	if len(mockSrv.resumedActors) != 1 || mockSrv.resumedActors[0] != "test-task" {
		t.Errorf("expected actor 'test-task' resumed, got %v", mockSrv.resumedActors)
	}
}

func TestTaskReconciler_Suspend(t *testing.T) {
	ctx := context.Background()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	mockSrv := &mockControlServer{}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mockSrv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create substrate client: %v", err)
	}
	defer client.Close()

	reconciler := controller.NewTaskReconciler(client, "test-template", "ax-system")
	reconciler.SecretResolver = noSecrets
	reconciler.WorkspaceReadyTimeout = 200 * time.Millisecond

	task := &v1alpha1.Task{
		ApiVersion: v1alpha1.APIVersion,
		Kind:       v1alpha1.KindTask,
		Metadata: &v1alpha1.ObjectMeta{
			Name:     "suspend-task",
			Atespace: "default",
		},
		Spec: &v1alpha1.TaskSpec{
			Image: "ghrc.io/my-org/my-image",
		},
	}

	reconciled, err := reconciler.Reconcile(ctx, task, nil)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	if reconciled.Status.Phase != "Suspended" {
		t.Errorf("expected phase 'Suspended', got %q", reconciled.Status.Phase)
	}
	if reconciled.Status.WorkerIp != "" {
		t.Errorf("expected empty worker IP, got %q", reconciled.Status.WorkerIp)
	}
	if len(mockSrv.suspendedActors) != 1 || mockSrv.suspendedActors[0] != "suspend-task" {
		t.Errorf("expected actor 'suspend-task' suspended, got %v", mockSrv.suspendedActors)
	}
}

func TestTaskReconciler_WorkspaceReady(t *testing.T) {
	ctx := context.Background()

	// 1. Mock Substrate Control Server
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	// 2. Mock Worker readyz HTTP server
	httpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen http: %v", err)
	}
	defer httpLis.Close()

	httpMux := http.NewServeMux()
	httpMux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	httpServer := &http.Server{Handler: httpMux}
	go httpServer.Serve(httpLis)
	defer httpServer.Close()

	workerHost, workerPortStr, _ := net.SplitHostPort(httpLis.Addr().String())
	// In our mock, the worker IP returned by ResumeActor will have our mock ready server listening.
	// But our reconciler connects to port 9999 by default: fmt.Sprintf("http://%s:9999/readyz", workerIP).
	// If workerIP includes a port or is a host, let's verify how it handles it.
	_ = workerHost
	_ = workerPortStr

	mockSrv := &mockControlServer{}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mockSrv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create substrate client: %v", err)
	}
	defer client.Close()

	reconciler := controller.NewTaskReconciler(client, "test-template", "ax-system")
	reconciler.SecretResolver = noSecrets
	reconciler.WorkspaceReadyTimeout = 200 * time.Millisecond

	task := &v1alpha1.Task{
		ApiVersion: v1alpha1.APIVersion,
		Kind:       v1alpha1.KindTask,
		Metadata: &v1alpha1.ObjectMeta{
			Name:     "ready-task",
			Atespace: "default",
		},
		Spec: &v1alpha1.TaskSpec{},
		Status: &v1alpha1.TaskStatus{
			Phase: "Running",
		},
	}

	// Case 1: Worker not responding on readyz -> WorkspaceReady=False and Ready=False.
	reconciled, err := reconciler.Reconcile(ctx, task, nil)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	assertCondition(t, reconciled, "WorkspaceReady", "False", "Initializing")
	assertCondition(t, reconciled, "Ready", "False", "WorkspaceInitializing")

	// Case 2: Worker readyz endpoint succeeds -> WorkspaceReady=True and Ready=True.
	mockSrv.workerIP = httpLis.Addr().String()
	reconciledReady, err := reconciler.Reconcile(ctx, task, nil)
	if err != nil {
		t.Fatalf("Reconcile with ready worker failed: %v", err)
	}
	assertCondition(t, reconciledReady, "WorkspaceReady", "True", "SetupComplete")
	assertCondition(t, reconciledReady, "Ready", "True", "TaskRunning")

	// Case 3: Suspending the task -> Ready=False (TaskSuspended), but the workspace was
	// already initialized so WorkspaceReady stays True.
	task = reconciledReady
	task.Status.Phase = "Suspended"
	reconciledSuspended, err := reconciler.Reconcile(ctx, task, nil)
	if err != nil {
		t.Fatalf("Reconcile with suspend failed: %v", err)
	}
	if reconciledSuspended.Status.Phase != "Suspended" {
		t.Errorf("expected phase Suspended, got %s", reconciledSuspended.Status.Phase)
	}
	assertCondition(t, reconciledSuspended, "Ready", "False", "TaskSuspended")
	assertCondition(t, reconciledSuspended, "WorkspaceReady", "True", "SetupComplete")

	// Case 4: Resuming with the worker unreachable -> the reconciler trusts the recorded
	// WorkspaceReady instead of re-polling, so the task is Ready again immediately.
	mockSrv.workerIP = "127.0.0.1:1"
	task = reconciledSuspended
	task.Status.Phase = "Running"
	reconciledResumed, err := reconciler.Reconcile(ctx, task, nil)
	if err != nil {
		t.Fatalf("Reconcile with resume failed: %v", err)
	}
	assertCondition(t, reconciledResumed, "WorkspaceReady", "True", "SetupComplete")
	assertCondition(t, reconciledResumed, "Ready", "True", "TaskRunning")
	if got := len(mockSrv.actorTemplates); got != 1 {
		t.Fatalf("readiness updates and suspend/resume created %d templates, want 1", got)
	}

	// A launch configuration change must still get a distinct template.
	task.Spec.Command = []string{"python3", "agent.py"}
	if _, err := reconciler.Reconcile(ctx, task, nil); err != nil {
		t.Fatalf("Reconcile with changed command failed: %v", err)
	}
	if got := len(mockSrv.actorTemplates); got != 2 {
		t.Errorf("command change left %d templates, want 2", got)
	}
}

// assertCondition fails the test unless the task has a condition of the given type with
// the expected status and reason.
func assertCondition(t *testing.T, task *v1alpha1.Task, condType, wantStatus, wantReason string) {
	t.Helper()
	for _, c := range task.Status.Conditions {
		if c.Type != condType {
			continue
		}
		if c.Status != wantStatus || c.Reason != wantReason {
			t.Errorf("expected %s=%s (%s), got Status=%s Reason=%s", condType, wantStatus, wantReason, c.Status, c.Reason)
		}
		return
	}
	t.Errorf("expected %s condition to be set", condType)
}

func TestReconcileDelete_RemovesActorAndTemplates(t *testing.T) {
	ctx := context.Background()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	mockSrv := &mockControlServer{actorTemplates: map[string]bool{
		"job-tmpl-0a1b2c3d":               true, // current revision of task "job"
		"job-tmpl-deadbeef":               true, // stale revision of task "job"
		"job-tmpl-deadbeef-tmpl-01234567": true, // belongs to a task literally named "job-tmpl-deadbeef"
		"jobs-tmpl-0a1b2c3d":              true, // belongs to task "jobs"
		"default-template":                true,
	}}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mockSrv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create substrate client: %v", err)
	}
	defer client.Close()

	reconciler := controller.NewTaskReconciler(client, "test-template", "ax-system")
	reconciler.SecretResolver = noSecrets
	reconciler.WorkspaceReadyTimeout = 200 * time.Millisecond

	if err := reconciler.ReconcileDelete(ctx, "default", "job", false); err != nil {
		t.Fatalf("ReconcileDelete failed: %v", err)
	}

	if len(mockSrv.deletedActors) != 1 || mockSrv.deletedActors[0] != "job" {
		t.Errorf("expected actor 'job' to be deleted, got %v", mockSrv.deletedActors)
	}

	wantDeleted := map[string]bool{"job-tmpl-0a1b2c3d": true, "job-tmpl-deadbeef": true}
	if len(mockSrv.deletedTemplates) != len(wantDeleted) {
		t.Errorf("expected %d templates deleted, got %v", len(wantDeleted), mockSrv.deletedTemplates)
	}
	for _, name := range mockSrv.deletedTemplates {
		if !wantDeleted[name] {
			t.Errorf("unexpected template deleted: %s", name)
		}
	}
	for _, keep := range []string{"job-tmpl-deadbeef-tmpl-01234567", "jobs-tmpl-0a1b2c3d", "default-template"} {
		if !mockSrv.actorTemplates[keep] {
			t.Errorf("template %s should not have been deleted", keep)
		}
	}
}

// TestReconcileDelete_RevokesOnlyForCredentialedTasks covers a GITHUB_TOKEN
// baked into an actor's template: it must be revoked when the deleted task
// had a CredentialProvider, so its controller-minted token doesn't outlive
// the task, but never touched otherwise, since a non-provider task may have
// set GITHUB_TOKEN itself to a shared or externally managed token.
func TestReconcileDelete_RevokesOnlyForCredentialedTasks(t *testing.T) {
	ctx := context.Background()
	newReconciler := func(t *testing.T) (*controller.TaskReconciler, *mockControlServer, *fakeInstallationTokens) {
		t.Helper()
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("failed to listen: %v", err)
		}
		t.Cleanup(func() { lis.Close() })
		mockSrv := &mockControlServer{
			actor: &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Name: "job", Atespace: "default"},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: "default", Name: "job-tmpl"},
				Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
			},
			createdTemplates: []*ateapipb.ActorTemplate{{
				Metadata:   &ateapipb.ResourceMetadata{Name: "job-tmpl", Atespace: "default"},
				Containers: []*ateapipb.Container{{Env: []*ateapipb.EnvVar{{Name: "GITHUB_TOKEN", Value: "ghs_secret"}}}},
			}},
		}
		grpcServer := grpc.NewServer()
		ateapipb.RegisterControlServer(grpcServer, mockSrv)
		go grpcServer.Serve(lis)
		t.Cleanup(grpcServer.Stop)
		client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("failed to create substrate client: %v", err)
		}
		t.Cleanup(func() { client.Close() })
		r := controller.NewTaskReconciler(client, "test-template", "ax-system")
		r.SecretResolver = noSecrets
		fake := &fakeInstallationTokens{}
		r.InstallationTokens = fake
		return r, mockSrv, fake
	}

	t.Run("non-provider task", func(t *testing.T) {
		r, _, fake := newReconciler(t)
		if err := r.ReconcileDelete(ctx, "default", "job", false); err != nil {
			t.Fatalf("ReconcileDelete failed: %v", err)
		}
		if len(fake.revoked) != 0 {
			t.Fatalf("revoked a token for a task without a credential provider: %v", fake.revoked)
		}
	})

	t.Run("credentialed task", func(t *testing.T) {
		r, mockSrv, fake := newReconciler(t)
		if err := r.ReconcileDelete(ctx, "default", "job", true); err != nil {
			t.Fatalf("ReconcileDelete failed: %v", err)
		}
		if len(fake.revoked) != 1 || fake.revoked[0] != "ghs_secret" {
			t.Fatalf("revoked = %v, want the actor's token revoked", fake.revoked)
		}
		if len(mockSrv.deletedActors) != 1 {
			t.Fatalf("expected the actor to still be deleted, got %v", mockSrv.deletedActors)
		}
	})
}

// TestReconcileDelete_RevokesOrphanedTemplateTokens covers a task with more
// than one ActorTemplate left behind across spec revisions: the actor only
// ever references its newest template, so a GITHUB_TOKEN baked into an
// older, orphaned one (e.g. left over from a revoke that failed or was
// skipped on an earlier revision) would otherwise be deleted along with its
// template without ever being revoked.
func TestReconcileDelete_RevokesOrphanedTemplateTokens(t *testing.T) {
	ctx := context.Background()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()
	mockSrv := &mockControlServer{
		actor: &ateapipb.Actor{
			Metadata:      &ateapipb.ResourceMetadata{Name: "job", Atespace: "default"},
			ActorTemplate: &ateapipb.ObjectRef{Atespace: "default", Name: "job-tmpl-aaaaaaaa"},
			Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
		},
		actorTemplates: map[string]bool{"job-tmpl-aaaaaaaa": true, "job-tmpl-bbbbbbbb": true},
		createdTemplates: []*ateapipb.ActorTemplate{
			{
				Metadata:   &ateapipb.ResourceMetadata{Name: "job-tmpl-aaaaaaaa", Atespace: "default"},
				Containers: []*ateapipb.Container{{Env: []*ateapipb.EnvVar{{Name: "GITHUB_TOKEN", Value: "ghs_current"}}}},
			},
			{
				Metadata:   &ateapipb.ResourceMetadata{Name: "job-tmpl-bbbbbbbb", Atespace: "default"},
				Containers: []*ateapipb.Container{{Env: []*ateapipb.EnvVar{{Name: "GITHUB_TOKEN", Value: "ghs_orphaned"}}}},
			},
		},
	}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mockSrv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()
	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create substrate client: %v", err)
	}
	defer client.Close()
	r := controller.NewTaskReconciler(client, "test-template", "ax-system")
	r.SecretResolver = noSecrets
	fake := &fakeInstallationTokens{}
	r.InstallationTokens = fake

	if err := r.ReconcileDelete(ctx, "default", "job", true); err != nil {
		t.Fatalf("ReconcileDelete failed: %v", err)
	}
	found := false
	for _, tok := range fake.revoked {
		if tok == "ghs_orphaned" {
			found = true
		}
	}
	if !found {
		t.Fatalf("revoked = %v, want the orphaned template's token revoked too", fake.revoked)
	}
}

// TestReconcileDelete_KeepsTemplateWhenRevokeFails covers a template whose
// token fails to revoke: deleting it anyway would destroy the only stored
// copy of that still-live token, with no later retry able to read it back
// out. It must be left in place instead.
func TestReconcileDelete_KeepsTemplateWhenRevokeFails(t *testing.T) {
	ctx := context.Background()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()
	mockSrv := &mockControlServer{
		actor: &ateapipb.Actor{
			Metadata:      &ateapipb.ResourceMetadata{Name: "job", Atespace: "default"},
			ActorTemplate: &ateapipb.ObjectRef{Atespace: "default", Name: "job-tmpl-aaaaaaaa"},
			Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
		},
		actorTemplates: map[string]bool{"job-tmpl-aaaaaaaa": true, "job-tmpl-bbbbbbbb": true},
		createdTemplates: []*ateapipb.ActorTemplate{
			{
				Metadata:   &ateapipb.ResourceMetadata{Name: "job-tmpl-aaaaaaaa", Atespace: "default"},
				Containers: []*ateapipb.Container{{Env: []*ateapipb.EnvVar{{Name: "GITHUB_TOKEN", Value: "ghs_current"}}}},
			},
			{
				Metadata:   &ateapipb.ResourceMetadata{Name: "job-tmpl-bbbbbbbb", Atespace: "default"},
				Containers: []*ateapipb.Container{{Env: []*ateapipb.EnvVar{{Name: "GITHUB_TOKEN", Value: "ghs_orphaned"}}}},
			},
		},
	}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mockSrv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()
	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create substrate client: %v", err)
	}
	defer client.Close()
	r := controller.NewTaskReconciler(client, "test-template", "ax-system")
	r.SecretResolver = noSecrets
	fake := &fakeInstallationTokens{revokeErrFor: map[string]bool{"ghs_orphaned": true}}
	r.InstallationTokens = fake

	// Propagated, not merely logged: the task record must not be deleted
	// while an orphaned template's token is still unrevoked, or that
	// template becomes unreachable for any later retry.
	if err := r.ReconcileDelete(ctx, "default", "job", true); err == nil {
		t.Fatal("expected the orphaned template's failed revoke to fail ReconcileDelete")
	}
	for _, name := range mockSrv.deletedTemplates {
		if name == "job-tmpl-bbbbbbbb" {
			t.Fatalf("deletedTemplates=%v, want the orphaned template (whose revoke failed) left in place for a retry", mockSrv.deletedTemplates)
		}
	}
	found := false
	for _, name := range mockSrv.deletedTemplates {
		if name == "job-tmpl-aaaaaaaa" {
			found = true
		}
	}
	if !found {
		t.Fatalf("deletedTemplates=%v, want the current template (whose revoke succeeded) still deleted", mockSrv.deletedTemplates)
	}
}

func TestReconcileDelete_BlocksUntilActorDeleted(t *testing.T) {
	ctx := context.Background()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	getCalls := 0
	mockSrv := &mockControlServer{
		getActorFunc: func(ctx context.Context, req *ateapipb.GetActorRequest) (*ateapipb.Actor, error) {
			getCalls++
			if getCalls < 3 {
				// Simulate actor in deleting state during the first two poll checks
				return &ateapipb.Actor{
					Metadata: &ateapipb.ResourceMetadata{Name: req.GetActor().GetName()},
					Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_DELETING},
				}, nil
			}
			// Once actor is completely torn down
			return nil, status.Errorf(codes.NotFound, "actor %q not found", req.GetActor().GetName())
		},
	}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mockSrv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create substrate client: %v", err)
	}
	defer client.Close()

	reconciler := controller.NewTaskReconciler(client, "test-template", "ax-system")
	reconciler.SecretResolver = noSecrets

	if err := reconciler.ReconcileDelete(ctx, "default", "slow-delete-task", false); err != nil {
		t.Fatalf("ReconcileDelete failed: %v", err)
	}

	if getCalls < 3 {
		t.Errorf("expected at least 3 GetActor calls before deletion completes, got %d", getCalls)
	}
}

func TestReconcileDelete_ActorDeletionTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	mockSrv := &mockControlServer{
		getActorFunc: func(ctx context.Context, req *ateapipb.GetActorRequest) (*ateapipb.Actor, error) {
			// Actor remains in deleting state indefinitely
			return &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Name: req.GetActor().GetName()},
				Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_DELETING},
			}, nil
		},
	}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mockSrv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create substrate client: %v", err)
	}
	defer client.Close()

	reconciler := controller.NewTaskReconciler(client, "test-template", "ax-system")
	reconciler.SecretResolver = noSecrets

	err = reconciler.ReconcileDelete(ctx, "default", "stuck-task", false)
	if err == nil {
		t.Fatal("expected error due to timeout, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Errorf("expected deadline exceeded error, got %v", err)
	}
}

func TestEnsureActor_RevertsCrashedActor(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	mockSrv := &mockControlServer{crashedActor: "job"}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mockSrv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create substrate client: %v", err)
	}
	defer client.Close()

	actor, err := client.EnsureActor(context.Background(), "default", "job", "default", "job-tmpl-0a1b2c3d")
	if err != nil {
		t.Fatalf("EnsureActor failed: %v", err)
	}
	if got := actor.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("actor state = %v, want SUSPENDED", got)
	}
	if len(mockSrv.revertedActors) != 1 || len(mockSrv.deletedActors) != 0 {
		t.Errorf("crashed actor: reverted %v, deleted %v; want reverted only", mockSrv.revertedActors, mockSrv.deletedActors)
	}
}

// TestCredentialedResumeSwitchesTemplateAfterCrashRevert covers a credentialed
// actor found CRASHED on resume: SetActorTemplate only works on a suspended
// actor, and only EnsureActor's own crash recovery (RevertActor) gets it
// there, so the token-bearing template switch must happen after EnsureActor,
// against the actor it actually returns -- not be attempted first against the
// still-crashed actor, which would reject it and strand the task.
func TestCredentialedResumeSwitchesTemplateAfterCrashRevert(t *testing.T) {
	ctx := context.Background()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	mockSrv := &mockControlServer{crashedActor: "job"}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mockSrv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()
	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	r := controller.NewTaskReconciler(client, "test-template", "ax-system")
	r.SecretResolver = func(_ context.Context, _, _, _ string) (string, error) { return "private-key", nil }
	r.InstallationTokens = &fakeInstallationTokens{}
	provider := &v1alpha1.CredentialProvider{Metadata: &v1alpha1.ObjectMeta{Name: "github", Atespace: "team"}, Spec: &v1alpha1.CredentialProviderSpec{GithubApp: &v1alpha1.GitHubAppCredential{AppId: 1, InstallationId: 2, PrivateKeySecret: &v1alpha1.SecretKeyRef{Name: "app-key", Key: "pem"}, Repositories: []string{"repo"}, Permissions: map[string]string{"contents": "read"}}}}
	task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "job", Atespace: "team"}, Spec: &v1alpha1.TaskSpec{CredentialProvider: &v1alpha1.CredentialProviderRef{Name: "github"}}, Status: &v1alpha1.TaskStatus{Phase: "Running", Conditions: []*v1alpha1.Condition{{Type: "WorkspaceReady", Status: "True"}}}}

	got, err := r.ReconcileWithProvider(ctx, task, provider)
	if err != nil {
		t.Fatalf("resume after crash failed: %v", err)
	}
	if got.GetStatus().GetPhase() != "Running" {
		t.Fatalf("phase = %q, want Running", got.GetStatus().GetPhase())
	}
	if len(mockSrv.revertedActors) != 1 {
		t.Fatalf("revertedActors=%v, want the crashed actor reverted once", mockSrv.revertedActors)
	}
	if mockSrv.actor.GetActorTemplate().GetName() == "" {
		t.Fatal("reverted actor never had its template switched to the credentialed one")
	}
}

// TestSuspendCrashedActorRevokesWithoutCallingSuspendActor covers suspending
// a credentialed actor that has crashed: real Substrate rejects SuspendActor
// on a crashed actor until it's been reverted, so suspendAndRevoke must not
// call it there -- a crashed actor is already stopped, so it only needs its
// token cleaned up.
func TestSuspendCrashedActorRevokesWithoutCallingSuspendActor(t *testing.T) {
	ctx := context.Background()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	mockSrv := &mockControlServer{crashedActor: "job"}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mockSrv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()
	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	r := controller.NewTaskReconciler(client, "test-template", "ax-system")
	r.SecretResolver = func(_ context.Context, _, _, _ string) (string, error) { return "private-key", nil }
	r.InstallationTokens = &fakeInstallationTokens{}
	provider := &v1alpha1.CredentialProvider{Metadata: &v1alpha1.ObjectMeta{Name: "github", Atespace: "team"}, Spec: &v1alpha1.CredentialProviderSpec{GithubApp: &v1alpha1.GitHubAppCredential{AppId: 1, InstallationId: 2, PrivateKeySecret: &v1alpha1.SecretKeyRef{Name: "app-key", Key: "pem"}, Repositories: []string{"repo"}, Permissions: map[string]string{"contents": "read"}}}}
	task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "job", Atespace: "team"}, Spec: &v1alpha1.TaskSpec{CredentialProvider: &v1alpha1.CredentialProviderRef{Name: "github"}}, Status: &v1alpha1.TaskStatus{Phase: "Suspended", Conditions: []*v1alpha1.Condition{{Type: "WorkspaceReady", Status: "True"}}}}

	got, err := r.ReconcileWithProvider(ctx, task, provider)
	if err != nil {
		t.Fatalf("suspending a crashed actor failed: %v", err)
	}
	if got.GetStatus().GetPhase() != "Suspended" {
		t.Fatalf("phase = %q, want Suspended", got.GetStatus().GetPhase())
	}
	if len(mockSrv.suspendedActors) != 0 {
		t.Fatalf("suspendedActors=%v, want SuspendActor never called on a crashed actor", mockSrv.suspendedActors)
	}
}

func TestTaskReconcilerClaudeCredential(t *testing.T) {
	t.Setenv("GEMINI_API_KEY", "gemini-must-not-be-injected")
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	mockSrv := &mockControlServer{}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mockSrv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()
	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	reconciler := controller.NewTaskReconciler(client, "test-template", "ax-system")
	reconciler.WorkspaceReadyTimeout = 50 * time.Millisecond
	reconciler.SecretResolver = func(_ context.Context, atespace, name, key string) (string, error) {
		if atespace != "default" || name != "anthropic-api-secret" || key != "ANTHROPIC_API_KEY" {
			t.Errorf("unexpected credential lookup: %s/%s key %s", atespace, name, key)
			return "", nil
		}
		return "test-anthropic-key", nil
	}
	task := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "claude-task", Atespace: "default"},
		Spec: &v1alpha1.TaskSpec{
			Image: "example.invalid/runner",
			Env:   []*v1alpha1.EnvVar{{Name: "AX_GOAL_AGENT", Value: "claude"}},
		},
	}
	if _, err := reconciler.Reconcile(context.Background(), task, nil); err != nil {
		t.Fatal(err)
	}
	if len(mockSrv.createdTemplates) != 1 {
		t.Fatalf("created %d templates, want 1", len(mockSrv.createdTemplates))
	}
	env := map[string]string{}
	for _, v := range mockSrv.createdTemplates[0].Containers[0].Env {
		env[v.Name] = v.Value
	}
	if env["ANTHROPIC_API_KEY"] != "test-anthropic-key" {
		t.Error("Anthropic key missing from Claude actor template")
	}
	if _, ok := env["GEMINI_API_KEY"]; ok {
		t.Error("Gemini key was included in Claude actor template")
	}
}

// TestClaudeSecretRotationAppliesToSuspendedActor covers a suspended,
// non-credential-provider Claude task whose Anthropic secret is rotated:
// EnsureActorTemplateWithImage creates a new template for the new secret
// regardless of credential-provider status, but only switching the actor to
// it was previously gated on provider != nil, so a suspended actor kept
// resuming with its old, stale secret.
func TestClaudeSecretRotationAppliesToSuspendedActor(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	mockSrv := &mockControlServer{}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mockSrv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()
	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	reconciler := controller.NewTaskReconciler(client, "test-template", "ax-system")
	reconciler.WorkspaceReadyTimeout = 50 * time.Millisecond
	secretValue := "key-v1"
	reconciler.SecretResolver = func(_ context.Context, _, _, _ string) (string, error) {
		return secretValue, nil
	}
	task := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "claude-task", Atespace: "default"},
		Spec: &v1alpha1.TaskSpec{
			Image: "example.invalid/runner",
			Env:   []*v1alpha1.EnvVar{{Name: "AX_GOAL_AGENT", Value: "claude"}},
		},
	}
	if _, err := reconciler.Reconcile(context.Background(), task, nil); err != nil {
		t.Fatal(err)
	}
	if len(mockSrv.createdTemplates) != 1 {
		t.Fatalf("created %d templates, want 1", len(mockSrv.createdTemplates))
	}

	secretValue = "key-v2"
	task.Status.Phase = "Suspended"
	if _, err := reconciler.Reconcile(context.Background(), task, nil); err != nil {
		t.Fatal(err)
	}
	if len(mockSrv.createdTemplates) != 2 {
		t.Fatalf("created %d templates after secret rotation, want 2 (one per secret value)", len(mockSrv.createdTemplates))
	}
	wantTemplate := mockSrv.createdTemplates[1].GetMetadata().GetName()
	if got := mockSrv.actor.GetActorTemplate().GetName(); got != wantTemplate {
		t.Fatalf("suspended actor's template = %q, want switched to the rotated secret's template %q", got, wantTemplate)
	}
}

// TestClaudeSecretRotationDeferredWhileActorRunning covers a running (not
// suspended) Claude task whose secret rotates: SetActorTemplate's contract
// only supports a suspended actor, so attempting the switch against a
// running one would fail and, unguarded, incorrectly mark an otherwise
// healthy running task Failed. The switch must be skipped instead, deferred
// until the actor is next actually suspended and resumed.
func TestClaudeSecretRotationDeferredWhileActorRunning(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	mockSrv := &mockControlServer{}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mockSrv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()
	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	reconciler := controller.NewTaskReconciler(client, "test-template", "ax-system")
	reconciler.WorkspaceReadyTimeout = 50 * time.Millisecond
	secretValue := "key-v1"
	reconciler.SecretResolver = func(_ context.Context, _, _, _ string) (string, error) {
		return secretValue, nil
	}
	task := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "claude-task", Atespace: "default"},
		Spec: &v1alpha1.TaskSpec{
			Image: "example.invalid/runner",
			Env:   []*v1alpha1.EnvVar{{Name: "AX_GOAL_AGENT", Value: "claude"}},
		},
	}
	// Create (suspended), then resume so the actor is RUNNING.
	if _, err := reconciler.Reconcile(context.Background(), task, nil); err != nil {
		t.Fatal(err)
	}
	task.Status.Phase = "Running"
	if _, err := reconciler.Reconcile(context.Background(), task, nil); err != nil {
		t.Fatal(err)
	}
	runningTemplate := mockSrv.actor.GetActorTemplate().GetName()

	secretValue = "key-v2"
	got, err := reconciler.Reconcile(context.Background(), task, nil)
	if err != nil {
		t.Fatalf("Reconcile failed (switch should be deferred, not errored): %v", err)
	}
	if got.GetStatus().GetPhase() != "Running" {
		t.Fatalf("phase = %q, want Running (must not be marked Failed)", got.GetStatus().GetPhase())
	}
	if got := mockSrv.actor.GetActorTemplate().GetName(); got != runningTemplate {
		t.Fatalf("running actor's template = %q, want unchanged (%q) until it's actually suspended", got, runningTemplate)
	}
}

// TestClaudeSecretTransientLookupFailurePreservesTemplate covers a suspended
// Claude task resumed while its secret lookup transiently fails (as opposed
// to the secret being authoritatively unconfigured): the reconciler must not
// treat that failure as "no credential" and compute+switch to a new template
// that lacks it, replacing a previously working one.
func TestClaudeSecretTransientLookupFailurePreservesTemplate(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	mockSrv := &mockControlServer{}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mockSrv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()
	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	reconciler := controller.NewTaskReconciler(client, "test-template", "ax-system")
	reconciler.WorkspaceReadyTimeout = 50 * time.Millisecond
	calls := 0
	reconciler.SecretResolver = func(_ context.Context, _, _, _ string) (string, error) {
		calls++
		if calls == 1 {
			return "key-v1", nil
		}
		return "", errors.New("transient secret store error")
	}
	task := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "claude-task", Atespace: "default"},
		Spec: &v1alpha1.TaskSpec{
			Image: "example.invalid/runner",
			Env:   []*v1alpha1.EnvVar{{Name: "AX_GOAL_AGENT", Value: "claude"}},
		},
	}
	// Create (suspended) with the secret lookup succeeding.
	if _, err := reconciler.Reconcile(context.Background(), task, nil); err != nil {
		t.Fatal(err)
	}
	workingTemplate := mockSrv.actor.GetActorTemplate().GetName()
	if len(mockSrv.createdTemplates) != 1 {
		t.Fatalf("created %d templates, want 1", len(mockSrv.createdTemplates))
	}

	// Reconcile again (still suspended) while the lookup now fails
	// transiently.
	got, err := reconciler.Reconcile(context.Background(), task, nil)
	if err != nil {
		t.Fatalf("Reconcile failed on a transient secret lookup error: %v", err)
	}
	if got.GetStatus().GetPhase() != "Suspended" {
		t.Fatalf("phase = %q, want Suspended", got.GetStatus().GetPhase())
	}
	if got := mockSrv.actor.GetActorTemplate().GetName(); got != workingTemplate {
		t.Fatalf("actor's template = %q, want unchanged (%q) despite the transient lookup failure", got, workingTemplate)
	}
}

// TestCredentialedActorSwitchesTemplateDespiteTransientModelSecretFailure
// covers a suspended credential-provider task whose GitHub installation
// token is rotated (old one revoked, new one minted and baked into a new
// template) in the same round that the unrelated Claude/Gemini model-secret
// lookup transiently fails. Skipping the template switch in that case -- as
// if preserving a previously-working template -- would actually leave the
// actor on its OLD template, whose GitHub token was already revoked above:
// worse than switching, not safer. The switch must go through regardless of
// the model-secret lookup outcome whenever a fresh GitHub token was minted
// this round.
func TestCredentialedActorSwitchesTemplateDespiteTransientModelSecretFailure(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	mockSrv := &mockControlServer{}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mockSrv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()
	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	reconciler := controller.NewTaskReconciler(client, "test-template", "ax-system")
	reconciler.WorkspaceReadyTimeout = 50 * time.Millisecond
	modelSecretFails := false
	reconciler.SecretResolver = func(_ context.Context, _, name, _ string) (string, error) {
		if name == "app-key" {
			return "private-key", nil
		}
		if modelSecretFails {
			return "", errors.New("transient secret store error")
		}
		return "claude-key-v1", nil
	}
	fake := &fakeInstallationTokens{}
	reconciler.InstallationTokens = fake
	provider := &v1alpha1.CredentialProvider{Metadata: &v1alpha1.ObjectMeta{Name: "github", Atespace: "team"}, Spec: &v1alpha1.CredentialProviderSpec{GithubApp: &v1alpha1.GitHubAppCredential{AppId: 1, InstallationId: 2, PrivateKeySecret: &v1alpha1.SecretKeyRef{Name: "app-key", Key: "pem"}, Repositories: []string{"repo"}, Permissions: map[string]string{"contents": "read"}}}}
	task := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "job", Atespace: "team"},
		Spec: &v1alpha1.TaskSpec{
			CredentialProvider: &v1alpha1.CredentialProviderRef{Name: "github"},
			Env:                []*v1alpha1.EnvVar{{Name: "AX_GOAL_AGENT", Value: "claude"}},
		},
	}

	// Create (suspended): mints token 1, builds template 1, model secret
	// lookup succeeds.
	if _, err := reconciler.ReconcileWithProvider(context.Background(), task, provider); err != nil {
		t.Fatal(err)
	}
	if fake.minted != 1 {
		t.Fatalf("minted = %d, want 1", fake.minted)
	}
	firstTemplate := mockSrv.actor.GetActorTemplate().GetName()

	// Suspend (revokes token 1), then resume: resuming a credentialed actor
	// mints a fresh token (token 2) and rotates the template, which is where
	// the model-secret lookup now fails transiently.
	task.Status.Phase = "Suspended"
	if _, err := reconciler.ReconcileWithProvider(context.Background(), task, provider); err != nil {
		t.Fatal(err)
	}
	modelSecretFails = true
	task.Status.Phase = "Running"
	got, err := reconciler.ReconcileWithProvider(context.Background(), task, provider)
	if err != nil {
		t.Fatalf("Reconcile failed on a transient model-secret lookup error: %v", err)
	}
	if fake.minted != 2 {
		t.Fatalf("minted = %d, want 2", fake.minted)
	}
	// Token 1 is revoked repeatedly along the way (it starts out suspended,
	// so the create call itself immediately revokes its own freshly minted
	// token; revoke is also idempotent across suspend/resume calls) -- what
	// matters here is that token 2 was never revoked.
	for _, tok := range fake.revoked {
		if tok == "ghs_test_token_2" {
			t.Fatalf("revoked = %v, want the freshly minted token 2 to remain live", fake.revoked)
		}
	}
	secondTemplate := mockSrv.actor.GetActorTemplate().GetName()
	if secondTemplate == firstTemplate {
		t.Fatalf("actor's template did not switch away from the revoked-token template %q", firstTemplate)
	}
	if got.GetStatus().GetPhase() != "Running" {
		t.Fatalf("phase = %q, want Running", got.GetStatus().GetPhase())
	}
	// The actor's current template must carry the new (unrevoked) token, not
	// the old one that was just revoked above.
	var tmpl *ateapipb.ActorTemplate
	for _, c := range mockSrv.createdTemplates {
		if c.GetMetadata().GetName() == secondTemplate {
			tmpl = c
			break
		}
	}
	if tmpl == nil {
		t.Fatalf("could not find actor's current template %q among created templates", secondTemplate)
	}
	var gotToken string
	for _, e := range tmpl.GetContainers()[0].GetEnv() {
		if e.GetName() == "GITHUB_TOKEN" {
			gotToken = e.GetValue()
		}
	}
	if gotToken != "ghs_test_token_2" {
		t.Fatalf("actor's current template carries GITHUB_TOKEN=%q, want the freshly minted ghs_test_token_2 (not the revoked token)", gotToken)
	}
	// The new template must also carry forward the last successfully-resolved
	// model credential: this round's own lookup failed, so without carrying
	// it forward the actor would resume into a template with a valid GitHub
	// token but no Claude credential at all, and the workspace goal would
	// then be skipped for lack of credentials.
	var gotModelKey string
	for _, e := range tmpl.GetContainers()[0].GetEnv() {
		if e.GetName() == "ANTHROPIC_API_KEY" {
			gotModelKey = e.GetValue()
		}
	}
	if gotModelKey != "claude-key-v1" {
		t.Fatalf("actor's current template carries ANTHROPIC_API_KEY=%q, want the last successfully-resolved %q carried forward", gotModelKey, "claude-key-v1")
	}
}

// TestCredentialedTemplateCarryForwardOverwritesStaleTaskSuppliedSecret covers
// the same transient-model-secret-lookup-during-token-rotation scenario as
// TestCredentialedActorSwitchesTemplateDespiteTransientModelSecretFailure, but
// with the task's own spec.env also setting a stale ANTHROPIC_API_KEY: since
// extraEnv is seeded from spec.env before the secret lookup even runs, that
// stale value is already non-empty by the time the carry-forward logic runs.
// Carrying the old template's value forward only when extraEnv is still empty
// would leave the stale task-supplied value in place; it must overwrite it
// instead, the same way a successful lookup would.
func TestCredentialedTemplateCarryForwardOverwritesStaleTaskSuppliedSecret(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	mockSrv := &mockControlServer{}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mockSrv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()
	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	reconciler := controller.NewTaskReconciler(client, "test-template", "ax-system")
	reconciler.WorkspaceReadyTimeout = 50 * time.Millisecond
	modelSecretFails := false
	reconciler.SecretResolver = func(_ context.Context, _, name, _ string) (string, error) {
		if name == "app-key" {
			return "private-key", nil
		}
		if modelSecretFails {
			return "", errors.New("transient secret store error")
		}
		return "claude-key-v1", nil
	}
	fake := &fakeInstallationTokens{}
	reconciler.InstallationTokens = fake
	provider := &v1alpha1.CredentialProvider{Metadata: &v1alpha1.ObjectMeta{Name: "github", Atespace: "team"}, Spec: &v1alpha1.CredentialProviderSpec{GithubApp: &v1alpha1.GitHubAppCredential{AppId: 1, InstallationId: 2, PrivateKeySecret: &v1alpha1.SecretKeyRef{Name: "app-key", Key: "pem"}, Repositories: []string{"repo"}, Permissions: map[string]string{"contents": "read"}}}}
	task := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "job", Atespace: "team"},
		Spec: &v1alpha1.TaskSpec{
			CredentialProvider: &v1alpha1.CredentialProviderRef{Name: "github"},
			Env:                []*v1alpha1.EnvVar{{Name: "AX_GOAL_AGENT", Value: "claude"}},
		},
	}

	if _, err := reconciler.ReconcileWithProvider(context.Background(), task, provider); err != nil {
		t.Fatal(err)
	}
	task.Status.Phase = "Suspended"
	if _, err := reconciler.ReconcileWithProvider(context.Background(), task, provider); err != nil {
		t.Fatal(err)
	}

	// The task now supplies its own, stale ANTHROPIC_API_KEY, and the
	// model-secret lookup fails transiently on the resume that rotates the
	// GitHub token and forces the template switch.
	task.Spec.Env = append(task.Spec.Env, &v1alpha1.EnvVar{Name: "ANTHROPIC_API_KEY", Value: "stale-task-supplied-key"})
	modelSecretFails = true
	task.Status.Phase = "Running"
	if _, err := reconciler.ReconcileWithProvider(context.Background(), task, provider); err != nil {
		t.Fatalf("Reconcile failed on a transient model-secret lookup error: %v", err)
	}

	secondTemplate := mockSrv.actor.GetActorTemplate().GetName()
	var tmpl *ateapipb.ActorTemplate
	for _, c := range mockSrv.createdTemplates {
		if c.GetMetadata().GetName() == secondTemplate {
			tmpl = c
			break
		}
	}
	if tmpl == nil {
		t.Fatalf("could not find actor's current template %q among created templates", secondTemplate)
	}
	var gotModelKey string
	for _, e := range tmpl.GetContainers()[0].GetEnv() {
		if e.GetName() == "ANTHROPIC_API_KEY" {
			gotModelKey = e.GetValue()
		}
	}
	if gotModelKey != "claude-key-v1" {
		t.Fatalf("actor's current template carries ANTHROPIC_API_KEY=%q, want the last successfully-resolved %q to overwrite the stale task-supplied value", gotModelKey, "claude-key-v1")
	}
}

func TestTaskReconcilerOpenRouterCredential(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	mockSrv := &mockControlServer{}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mockSrv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()
	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	reconciler := controller.NewTaskReconciler(client, "test-template", "ax-system")
	reconciler.WorkspaceReadyTimeout = 50 * time.Millisecond
	reconciler.SecretResolver = func(_ context.Context, atespace, name, key string) (string, error) {
		if atespace != "default" || name != "openrouter-api-secret" || key != "OPENROUTER_API_KEY" {
			t.Errorf("unexpected credential lookup: %s/%s key %s", atespace, name, key)
			return "", nil
		}
		return "test-openrouter-key", nil
	}
	task := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "openrouter-task", Atespace: "default"},
		Spec: &v1alpha1.TaskSpec{
			Image: "example.invalid/runner",
			Env: []*v1alpha1.EnvVar{
				{Name: "AX_GOAL_AGENT", Value: "claude"},
				{Name: "AX_CLAUDE_PROVIDER", Value: "openrouter"},
			},
		},
	}
	if _, err := reconciler.Reconcile(context.Background(), task, nil); err != nil {
		t.Fatal(err)
	}
	if len(mockSrv.createdTemplates) != 1 {
		t.Fatalf("created %d templates, want 1", len(mockSrv.createdTemplates))
	}
	env := map[string]string{}
	for _, v := range mockSrv.createdTemplates[0].Containers[0].Env {
		env[v.Name] = v.Value
	}
	if env["ANTHROPIC_AUTH_TOKEN"] != "test-openrouter-key" || env["ANTHROPIC_BASE_URL"] != "https://openrouter.ai/api" {
		t.Error("OpenRouter configuration missing from Claude actor template")
	}
	if key, ok := env["ANTHROPIC_API_KEY"]; !ok || key != "" {
		t.Error("Anthropic API key must be explicitly empty for OpenRouter")
	}
}

// TestOpenRouterCredentialSurvivesTaskSuppliedEnv covers a task whose own
// spec.env happens to set ANTHROPIC_API_KEY: the runner re-applies every
// AX_TASK_YAML spec.env entry over the container's real environment at
// startup, so if that stale key stayed in AX_TASK_YAML it would silently
// override the OpenRouter credentials just resolved for the container.
func TestOpenRouterCredentialSurvivesTaskSuppliedEnv(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	mockSrv := &mockControlServer{}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mockSrv)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()
	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	reconciler := controller.NewTaskReconciler(client, "test-template", "ax-system")
	reconciler.WorkspaceReadyTimeout = 50 * time.Millisecond
	reconciler.SecretResolver = func(_ context.Context, _, _, _ string) (string, error) {
		return "test-openrouter-key", nil
	}
	task := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "openrouter-task", Atespace: "default"},
		Spec: &v1alpha1.TaskSpec{
			Image: "example.invalid/runner",
			Env: []*v1alpha1.EnvVar{
				{Name: "AX_GOAL_AGENT", Value: "claude"},
				{Name: "AX_CLAUDE_PROVIDER", Value: "openrouter"},
				{Name: "ANTHROPIC_API_KEY", Value: "stale-task-supplied-key"},
			},
		},
	}
	if _, err := reconciler.Reconcile(context.Background(), task, nil); err != nil {
		t.Fatal(err)
	}
	if len(mockSrv.createdTemplates) != 1 {
		t.Fatalf("created %d templates, want 1", len(mockSrv.createdTemplates))
	}
	var taskYAML string
	env := map[string]string{}
	for _, v := range mockSrv.createdTemplates[0].Containers[0].Env {
		env[v.Name] = v.Value
		if v.Name == "AX_TASK_YAML" {
			taskYAML = v.Value
		}
	}
	if env["ANTHROPIC_AUTH_TOKEN"] != "test-openrouter-key" {
		t.Error("OpenRouter token missing from container environment")
	}
	if strings.Contains(taskYAML, "stale-task-supplied-key") || strings.Contains(taskYAML, "ANTHROPIC_API_KEY") {
		t.Errorf("AX_TASK_YAML still carries the task-supplied ANTHROPIC_API_KEY, which the runner would reapply over the container env: %s", taskYAML)
	}
}
