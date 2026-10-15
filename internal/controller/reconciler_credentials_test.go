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
	"crypto/sha256"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/ax/internal/controller"
	"github.com/google/ax/internal/credentials"
	"github.com/google/ax/internal/lock"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/internal/substrate"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// issuer is an OAuth provider that records the access tokens it hands out and
// the ones it is asked to revoke.
type issuer struct {
	srv *httptest.Server

	mu      sync.Mutex
	issued  []string
	revoked []string
}

func newIssuer(t *testing.T) *issuer {
	t.Helper()
	i := &issuer{}
	i.srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		i.mu.Lock()
		defer i.mu.Unlock()
		switch r.URL.Path {
		case "/token":
			tok := fmt.Sprintf("at-%d", len(i.issued)+1)
			i.issued = append(i.issued, tok)
			_, _ = fmt.Fprintf(w, `{"access_token":%q,"expires_in":3600}`, tok)
		case "/revoke":
			i.revoked = append(i.revoked, r.PostForm.Get("token"))
		}
	}))
	t.Cleanup(i.srv.Close)
	return i
}

func (i *issuer) issuedTokens() []string {
	i.mu.Lock()
	defer i.mu.Unlock()
	return append([]string(nil), i.issued...)
}

func (i *issuer) revokedTokens() []string {
	i.mu.Lock()
	defer i.mu.Unlock()
	return append([]string(nil), i.revoked...)
}

// credential returns an oauthRefreshToken credential backed by the issuer, whose
// tokens can be revoked.
func (i *issuer) credential(name string) *v1alpha1.Credential {
	return &v1alpha1.Credential{
		Metadata: &v1alpha1.ObjectMeta{Name: name, Atespace: "default"},
		Spec: &v1alpha1.CredentialSpec{Type: &v1alpha1.CredentialSpec_OauthRefreshToken{OauthRefreshToken: &v1alpha1.OAuthRefreshTokenCredential{
			TokenUrl:           i.srv.URL + "/token",
			RevocationUrl:      i.srv.URL + "/revoke",
			ClientId:           "client",
			RefreshTokenSecret: &v1alpha1.SecretKeyRef{Name: "idp", Key: "refresh"},
		}}},
	}
}

// service issues tokens from the issuer, with every Secret resolving to a fixed
// seed. The returned counter reports how many Secrets were read, which is one per
// token issued.
func (i *issuer) service() *credentials.Service {
	return &credentials.Service{
		Secrets:    func(context.Context, string, string, string) (string, error) { return "seed", nil },
		State:      memory.NewStore(),
		Locker:     lock.NewMemoryLocker(),
		HTTPClient: i.srv.Client(),
	}
}

// accessTokenCredential is a credential whose token is the given Secret value.
func accessTokenCredential(name, secret string) *v1alpha1.Credential {
	return &v1alpha1.Credential{
		Metadata: &v1alpha1.ObjectMeta{Name: name, Atespace: "default"},
		Spec: &v1alpha1.CredentialSpec{Type: &v1alpha1.CredentialSpec_OauthAccessToken{OauthAccessToken: &v1alpha1.OAuthAccessTokenCredential{
			TokenSecret: &v1alpha1.SecretKeyRef{Name: secret, Key: "token"},
		}}},
	}
}

// secretMap is a credentials.SecretResolver over fixed values that counts reads.
type secretMap struct {
	mu     sync.Mutex
	values map[string]string
	reads  int
}

func (s *secretMap) resolve(_ context.Context, _, name, _ string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads++
	v, ok := s.values[name]
	if !ok {
		return "", fmt.Errorf("secret %q not found", name)
	}
	return v, nil
}

func (s *secretMap) set(name, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[name] = value
}

func (s *secretMap) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads
}

func newCredentialReconciler(t *testing.T, svc *credentials.Service) (*controller.TaskReconciler, *mockControlServer) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	mock := &mockControlServer{}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, mock)
	go grpcServer.Serve(lis)
	t.Cleanup(func() { grpcServer.Stop(); lis.Close() })

	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create substrate client: %v", err)
	}
	t.Cleanup(func() { client.Close() })

	r := controller.NewTaskReconciler(client, "test-template", "ax-system")
	r.SecretResolver = noSecrets
	r.WorkspaceReadyTimeout = 100 * time.Millisecond
	r.Credentials = svc
	return r, mock
}

func credentialTask(name, phase string, bindings ...*v1alpha1.CredentialBinding) *v1alpha1.Task {
	return &v1alpha1.Task{
		ApiVersion: v1alpha1.APIVersion,
		Kind:       v1alpha1.KindTask,
		Metadata:   &v1alpha1.ObjectMeta{Name: name, Atespace: "default"},
		Spec:       &v1alpha1.TaskSpec{Image: "example.com/img", Credentials: bindings},
		Status:     &v1alpha1.TaskStatus{Phase: phase},
	}
}

func bind(name, env string) *v1alpha1.CredentialBinding {
	return &v1alpha1.CredentialBinding{Name: name, Env: env}
}

// templates returns the ActorTemplates created so far, as name -> env.
func (m *mockControlServer) templates() map[string]map[string]string {
	m.credMu.Lock()
	defer m.credMu.Unlock()
	out := map[string]map[string]string{}
	for _, tmpl := range m.createdTemplates {
		env := map[string]string{}
		for _, c := range tmpl.GetContainers() {
			for _, e := range c.GetEnv() {
				env[e.GetName()] = e.GetValue()
			}
		}
		out[tmpl.GetMetadata().GetName()] = env
	}
	return out
}

// failTemplateCreation makes CreateActorTemplate run hook, then fail with err.
func (m *mockControlServer) failTemplateCreation(err error, hook func()) {
	m.credMu.Lock()
	defer m.credMu.Unlock()
	m.createTemplateErr, m.onCreateTemplate = err, hook
}

func (m *mockControlServer) onlyTemplate(t *testing.T) (string, map[string]string) {
	t.Helper()
	all := m.templates()
	if len(all) != 1 {
		t.Fatalf("created %d templates, want 1: %v", len(all), keys(all))
	}
	for name, env := range all {
		return name, env
	}
	return "", nil
}

func keys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func readyCondition(task *v1alpha1.Task) *v1alpha1.Condition {
	for _, c := range task.GetStatus().GetConditions() {
		if c.GetType() == "Ready" {
			return c
		}
	}
	return nil
}

func TestReconcile_CredentialTokenLandsInTheTemplateEnv(t *testing.T) {
	secrets := &secretMap{values: map[string]string{"claude-oauth": "sk-ant-oat01-TOKEN"}}
	r, mock := newCredentialReconciler(t, &credentials.Service{Secrets: secrets.resolve})
	task := credentialTask("t", "Suspended", bind("claude", "CLAUDE_CODE_OAUTH_TOKEN"))

	if _, err := r.Reconcile(context.Background(), task, nil, []*v1alpha1.Credential{accessTokenCredential("claude", "claude-oauth")}); err != nil {
		t.Fatal(err)
	}
	_, env := mock.onlyTemplate(t)
	if env["CLAUDE_CODE_OAUTH_TOKEN"] != "sk-ant-oat01-TOKEN" {
		t.Errorf("template env = %v", keys(env))
	}
	// The task spec the runner reads back names the binding but never the token.
	if yaml := env["AX_TASK_YAML"]; !strings.Contains(yaml, "CLAUDE_CODE_OAUTH_TOKEN") || strings.Contains(yaml, "sk-ant-oat01-TOKEN") {
		t.Errorf("AX_TASK_YAML = %q", yaml)
	}
}

func TestReconcile_BoundTokenWinsOverOtherInjectedEnv(t *testing.T) {
	secrets := &secretMap{values: map[string]string{"gemini": "from-credential"}}
	r, mock := newCredentialReconciler(t, &credentials.Service{Secrets: secrets.resolve})
	r.SecretResolver = func(context.Context, string, string, string) (string, error) { return "ambient-gemini-key", nil }
	task := credentialTask("t", "Suspended", bind("gem", "GEMINI_API_KEY"))

	if _, err := r.Reconcile(context.Background(), task, nil, []*v1alpha1.Credential{accessTokenCredential("gem", "gemini")}); err != nil {
		t.Fatal(err)
	}
	if _, env := mock.onlyTemplate(t); env["GEMINI_API_KEY"] != "from-credential" {
		t.Errorf("GEMINI_API_KEY = %q, want the bound credential's token", env["GEMINI_API_KEY"])
	}
}

func TestReconcile_ResumeReusesTheTemplateAndIssuesNothing(t *testing.T) {
	iss := newIssuer(t)
	r, mock := newCredentialReconciler(t, iss.service())
	creds := []*v1alpha1.Credential{iss.credential("idp")}
	task := credentialTask("t", "Suspended", bind("idp", "IDP_TOKEN"))

	if _, err := r.Reconcile(context.Background(), task, nil, creds); err != nil {
		t.Fatal(err)
	}
	if got := iss.issuedTokens(); len(got) != 1 {
		t.Fatalf("issued %v on create, want 1 token", got)
	}
	name, env := mock.onlyTemplate(t)
	if env["IDP_TOKEN"] != "at-1" {
		t.Fatalf("IDP_TOKEN = %q", env["IDP_TOKEN"])
	}

	// Resume, suspend, resume again: the template is already there and already
	// holds a token, so no further token is minted. Neither is one needed when
	// the credential has since been deleted.
	for _, phase := range []string{"Running", "Suspended", "Running"} {
		task.Status.Phase = phase
		if _, err := r.Reconcile(context.Background(), task, nil, nil); err != nil {
			t.Fatalf("Reconcile(%s): %v", phase, err)
		}
	}
	if got := iss.issuedTokens(); len(got) != 1 {
		t.Errorf("issued %v after resume cycles, want still 1 token", got)
	}
	if got := mock.templates(); len(got) != 1 || got[name] == nil {
		t.Errorf("templates = %v, want only %s", keys(got), name)
	}
	if got := iss.revokedTokens(); len(got) != 0 {
		t.Errorf("revoked %v, but the template still holds the token", got)
	}
}

func TestReconcile_TokenValueDoesNotChangeTheTemplateName(t *testing.T) {
	// The template name is derived from the spec. If it also depended on token
	// values, every reconcile would mint a token and a template.
	secrets := &secretMap{values: map[string]string{"claude-oauth": "token-v1"}}
	r, mock := newCredentialReconciler(t, &credentials.Service{Secrets: secrets.resolve})
	creds := []*v1alpha1.Credential{accessTokenCredential("claude", "claude-oauth")}
	task := credentialTask("t", "Suspended", bind("claude", "TOKEN"))

	if _, err := r.Reconcile(context.Background(), task, nil, creds); err != nil {
		t.Fatal(err)
	}
	secrets.set("claude-oauth", "token-v2")
	task.Status.Phase = "Running"
	if _, err := r.Reconcile(context.Background(), task, nil, creds); err != nil {
		t.Fatal(err)
	}
	if _, env := mock.onlyTemplate(t); env["TOKEN"] != "token-v1" {
		t.Errorf("TOKEN = %q, want the first token kept", env["TOKEN"])
	}
	if secrets.count() != 1 {
		t.Errorf("secret read %d times, want 1", secrets.count())
	}
}

func TestReconcile_BindingsChangeTheTemplateName(t *testing.T) {
	secrets := &secretMap{values: map[string]string{"s": "tok"}}
	r, mock := newCredentialReconciler(t, &credentials.Service{Secrets: secrets.resolve})
	creds := []*v1alpha1.Credential{accessTokenCredential("a", "s")}
	for _, env := range []string{"ONE", "TWO"} {
		if _, err := r.Reconcile(context.Background(), credentialTask("t", "Suspended", bind("a", env)), nil, creds); err != nil {
			t.Fatal(err)
		}
	}
	if got := mock.templates(); len(got) != 2 {
		t.Errorf("templates = %v, want one per binding set", keys(got))
	}
}

func TestReconcile_OneCredentialBoundToTwoVariablesIsIssuedOnce(t *testing.T) {
	iss := newIssuer(t)
	r, mock := newCredentialReconciler(t, iss.service())
	task := credentialTask("t", "Suspended", bind("idp", "GH_TOKEN"), bind("idp", "GITHUB_TOKEN"))

	if _, err := r.Reconcile(context.Background(), task, nil, []*v1alpha1.Credential{iss.credential("idp")}); err != nil {
		t.Fatal(err)
	}
	_, env := mock.onlyTemplate(t)
	if env["GH_TOKEN"] != "at-1" || env["GITHUB_TOKEN"] != "at-1" || len(iss.issuedTokens()) != 1 {
		t.Errorf("GH_TOKEN=%q GITHUB_TOKEN=%q issued=%v", env["GH_TOKEN"], env["GITHUB_TOKEN"], iss.issuedTokens())
	}
}

// legacyTemplateName is the name the reconciler produced before credentials
// existed: a digest of the image and the sorted container environment.
func legacyTemplateName(task, image string, env map[string]string) string {
	h := sha256.New()
	h.Write([]byte(image))
	for _, k := range keys(env) {
		h.Write([]byte(k + "=" + env[k] + ";"))
	}
	return fmt.Sprintf("%s-tmpl-%x", task, h.Sum(nil)[:4])
}

func TestReconcile_WithoutBindingsTheTemplateNameIsUnchanged(t *testing.T) {
	// A changed name would create a new template, with a new golden snapshot,
	// for every existing task the first time it is reconciled after an upgrade.
	r, mock := newCredentialReconciler(t, nil)
	task := credentialTask("legacy", "Suspended")
	task.Spec.Env = []*v1alpha1.EnvVar{{Name: "FOO", Value: "bar"}}

	if _, err := r.Reconcile(context.Background(), task, nil, nil); err != nil {
		t.Fatal(err)
	}
	name, env := mock.onlyTemplate(t)
	if want := legacyTemplateName("legacy", "example.com/img", env); name != want {
		t.Errorf("template name = %s, want %s", name, want)
	}
	if strings.Contains(env["AX_TASK_YAML"], "credentials") {
		t.Errorf("a task without bindings must serialize without a credentials field:\n%s", env["AX_TASK_YAML"])
	}
}

func TestReconcile_TemplateCreateFailureRevokesAndDoesNotFallBack(t *testing.T) {
	iss := newIssuer(t)
	r, mock := newCredentialReconciler(t, iss.service())
	// The caller gives up while the template is being created. Tokens that were
	// already minted must still be revoked, on a context that outlives it.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mock.failTemplateCreation(status.Error(codes.Internal, "substrate is down"), cancel)

	task := credentialTask("t", "Suspended", bind("idp", "IDP_TOKEN"))
	got, err := r.Reconcile(ctx, task, nil, []*v1alpha1.Credential{iss.credential("idp")})
	if err == nil {
		t.Fatal("expected an error")
	}
	if got.Status.Phase != "Failed" {
		t.Errorf("phase = %q, want Failed", got.Status.Phase)
	}
	if c := readyCondition(got); c == nil || c.Status != "False" || c.Reason != "CredentialIssueFailed" {
		t.Errorf("Ready condition = %v", c)
	}
	// No fallback to the default template: that would start the task without its credentials.
	if len(mock.createdActors) != 0 || len(mock.resumedActors) != 0 {
		t.Errorf("an actor was created (%v) or resumed (%v) despite the failure", mock.createdActors, mock.resumedActors)
	}
	if issued, revoked := iss.issuedTokens(), iss.revokedTokens(); len(issued) != 1 || len(revoked) != 1 || revoked[0] != issued[0] {
		t.Errorf("issued %v, revoked %v: the orphaned token must be revoked", issued, revoked)
	}
	if strings.Contains(err.Error(), "at-1") {
		t.Errorf("error leaks the token: %v", err)
	}
}

func TestReconcile_TransientFailureLeavesTheTaskRetryable(t *testing.T) {
	iss := newIssuer(t)
	r, mock := newCredentialReconciler(t, iss.service())
	// DeadlineExceeded is transient but, unlike Unavailable, not retried inside
	// the Substrate client, which would only slow this test down.
	mock.failTemplateCreation(status.Error(codes.DeadlineExceeded, "substrate is slow"), nil)

	task := credentialTask("t", "Suspended", bind("idp", "IDP_TOKEN"))
	got, err := r.Reconcile(context.Background(), task, nil, []*v1alpha1.Credential{iss.credential("idp")})
	if err == nil || !substrate.IsTransientError(err) {
		t.Fatalf("error = %v, want one that still reads as transient so the server answers Unavailable", err)
	}
	// Pending, not Failed: the server lets the client retry a Pending task.
	if c := readyCondition(got); got.Status.Phase != "Pending" || c == nil || c.Reason != "CredentialIssueFailed" {
		t.Errorf("phase = %q, Ready = %v", got.Status.Phase, c)
	}
	if len(mock.createdActors) != 0 {
		t.Errorf("an actor was created: %v", mock.createdActors)
	}
	if issued, revoked := iss.issuedTokens(), iss.revokedTokens(); len(issued) != 1 || len(revoked) != 1 {
		t.Errorf("issued %v, revoked %v: the unused token must still be revoked", issued, revoked)
	}

	// Once Substrate is back, the retry succeeds with a fresh token. The server
	// puts a retried task back in Suspended before it reconciles it.
	mock.failTemplateCreation(nil, nil)
	task.Status.Phase = "Suspended"
	got, err = r.Reconcile(context.Background(), task, nil, []*v1alpha1.Credential{iss.credential("idp")})
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if _, env := mock.onlyTemplate(t); env["IDP_TOKEN"] != "at-2" || got.Status.Phase != "Suspended" {
		t.Errorf("IDP_TOKEN = %q, phase = %q", env["IDP_TOKEN"], got.Status.Phase)
	}
}

func TestReconcile_IssueFailureRevokesTokensIssuedBefore(t *testing.T) {
	iss := newIssuer(t)
	r, mock := newCredentialReconciler(t, iss.service())
	broken := iss.credential("broken")
	broken.Spec.GetOauthRefreshToken().TokenUrl = iss.srv.URL + "/missing" // answers 200 with an empty body

	task := credentialTask("t", "Suspended", bind("idp", "GOOD"), bind("broken", "BAD"))
	got, err := r.Reconcile(context.Background(), task, nil, []*v1alpha1.Credential{iss.credential("idp"), broken})
	if err == nil {
		t.Fatal("expected an error")
	}
	if c := readyCondition(got); c == nil || c.Reason != "CredentialIssueFailed" || got.Status.Phase != "Failed" {
		t.Errorf("phase = %q, Ready = %v", got.Status.Phase, c)
	}
	if len(mock.templates()) != 0 || len(mock.createdActors) != 0 {
		t.Errorf("templates %v / actors %v created despite the failure", keys(mock.templates()), mock.createdActors)
	}
	if issued, revoked := iss.issuedTokens(), iss.revokedTokens(); len(issued) != 1 || len(revoked) != 1 || revoked[0] != issued[0] {
		t.Errorf("issued %v, revoked %v", issued, revoked)
	}
}

func TestReconcile_MissingCredentialFailsWithoutFallback(t *testing.T) {
	r, mock := newCredentialReconciler(t, &credentials.Service{})
	task := credentialTask("t", "Suspended", bind("gone", "TOKEN"))

	got, err := r.Reconcile(context.Background(), task, nil, nil)
	if err == nil || !strings.Contains(err.Error(), `"gone"`) {
		t.Fatalf("error = %v, want one naming the credential", err)
	}
	if c := readyCondition(got); c == nil || c.Reason != "CredentialIssueFailed" || got.Status.Phase != "Failed" {
		t.Errorf("phase = %q, Ready = %v", got.Status.Phase, c)
	}
	if len(mock.templates()) != 0 || len(mock.createdActors) != 0 {
		t.Errorf("templates %v / actors %v created despite the failure", keys(mock.templates()), mock.createdActors)
	}
}

func TestReconcile_BindingsWithoutAServiceFail(t *testing.T) {
	r, mock := newCredentialReconciler(t, nil)
	got, err := r.Reconcile(context.Background(), credentialTask("t", "Suspended", bind("a", "TOKEN")), nil, []*v1alpha1.Credential{accessTokenCredential("a", "s")})
	if err == nil {
		t.Fatal("expected an error")
	}
	if c := readyCondition(got); c == nil || c.Reason != "CredentialIssueFailed" {
		t.Errorf("Ready = %v", c)
	}
	if len(mock.createdActors) != 0 {
		t.Errorf("actors created: %v", mock.createdActors)
	}
}

func TestReconcile_ExistingTemplateNeedsNoService(t *testing.T) {
	// A server restarted without credentials support can still resume tasks
	// whose templates already hold their tokens.
	secrets := &secretMap{values: map[string]string{"s": "tok"}}
	r, mock := newCredentialReconciler(t, &credentials.Service{Secrets: secrets.resolve})
	creds := []*v1alpha1.Credential{accessTokenCredential("a", "s")}
	task := credentialTask("t", "Suspended", bind("a", "TOKEN"))
	if _, err := r.Reconcile(context.Background(), task, nil, creds); err != nil {
		t.Fatal(err)
	}

	r.Credentials = nil
	task.Status.Phase = "Running"
	if _, err := r.Reconcile(context.Background(), task, nil, nil); err != nil {
		t.Fatalf("resume without a service: %v", err)
	}
	if len(mock.resumedActors) != 1 {
		t.Errorf("resumed actors = %v", mock.resumedActors)
	}
}
