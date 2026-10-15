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

package server_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/ax/internal/credentials"
	"github.com/google/ax/internal/lock"
	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func oauthAccessCredential(atespace, name, secretName string) *v1alpha1.Credential {
	return &v1alpha1.Credential{
		Metadata: &v1alpha1.ObjectMeta{Name: name, Atespace: atespace},
		Spec: &v1alpha1.CredentialSpec{Type: &v1alpha1.CredentialSpec_OauthAccessToken{OauthAccessToken: &v1alpha1.OAuthAccessTokenCredential{
			TokenSecret: &v1alpha1.SecretKeyRef{Name: secretName, Key: "token"},
		}}},
	}
}

func oauthRefreshCredential(name, scope string) *v1alpha1.Credential {
	return &v1alpha1.Credential{
		Metadata: &v1alpha1.ObjectMeta{Name: name},
		Spec: &v1alpha1.CredentialSpec{Type: &v1alpha1.CredentialSpec_OauthRefreshToken{OauthRefreshToken: &v1alpha1.OAuthRefreshTokenCredential{
			TokenUrl:           "https://idp.example.com/token",
			ClientId:           "client",
			Scopes:             []string{scope},
			RefreshTokenSecret: &v1alpha1.SecretKeyRef{Name: "idp", Key: "refresh"},
		}}},
	}
}

func updateCredential(t *testing.T, srv *server.Server, c *v1alpha1.Credential) *v1alpha1.Credential {
	t.Helper()
	got, err := srv.UpdateCredential(context.Background(), &v1alpha1.UpdateCredentialRequest{Credential: c})
	if err != nil {
		t.Fatalf("UpdateCredential: %v", err)
	}
	return got
}

func TestCredential_CRUD(t *testing.T) {
	srv := server.NewServer(memory.NewStore())
	ctx := context.Background()

	// Metadata is left partly empty to exercise server-side defaulting.
	created := updateCredential(t, srv, oauthAccessCredential("", "claude", "claude-oauth"))
	if created.GetMetadata().GetAtespace() != "default" || created.GetMetadata().GetCreationTimestamp() == nil {
		t.Errorf("metadata not defaulted: %v", created.GetMetadata())
	}

	got, err := srv.GetCredential(ctx, &v1alpha1.GetCredentialRequest{Name: "claude"})
	if err != nil || got.GetSpec().GetOauthAccessToken().GetTokenSecret().GetName() != "claude-oauth" {
		t.Fatalf("GetCredential = %v, %v", got, err)
	}

	updateCredential(t, srv, oauthAccessCredential("team-a", "other", "s"))
	all, err := srv.ListCredentials(ctx, &v1alpha1.ListCredentialsRequest{})
	if err != nil || len(all.GetCredentials()) != 2 {
		t.Fatalf("list all = %v, %v", all, err)
	}
	scoped, _ := srv.ListCredentials(ctx, &v1alpha1.ListCredentialsRequest{Atespace: "team-a"})
	if len(scoped.GetCredentials()) != 1 || scoped.GetCredentials()[0].GetMetadata().GetName() != "other" {
		t.Errorf("list team-a = %v", scoped)
	}

	// Re-applying keeps the original creation timestamp.
	reapplied := updateCredential(t, srv, oauthAccessCredential("", "claude", "claude-oauth-2"))
	if !reapplied.GetMetadata().GetCreationTimestamp().AsTime().Equal(created.GetMetadata().GetCreationTimestamp().AsTime()) {
		t.Errorf("creation timestamp changed on update")
	}

	if _, err := srv.DeleteCredential(ctx, &v1alpha1.DeleteCredentialRequest{Name: "claude"}); err != nil {
		t.Fatal(err)
	}
	if _, err := srv.GetCredential(ctx, &v1alpha1.GetCredentialRequest{Name: "claude"}); status.Code(err) != codes.NotFound {
		t.Errorf("GetCredential after delete = %v, want NotFound", err)
	}
}

func TestUpdateCredential_RejectsInvalid(t *testing.T) {
	srv := server.NewServer(memory.NewStore())
	ctx := context.Background()

	httpURL := oauthRefreshCredential("idp", "repo")
	httpURL.Spec.GetOauthRefreshToken().TokenUrl = "http://idp.example.com/token"
	for name, c := range map[string]*v1alpha1.Credential{
		"bad name": oauthAccessCredential("", "Not_Valid", "s"),
		"no type":  {Metadata: &v1alpha1.ObjectMeta{Name: "x"}, Spec: &v1alpha1.CredentialSpec{}},
		"http url": httpURL,
	} {
		if _, err := srv.UpdateCredential(ctx, &v1alpha1.UpdateCredentialRequest{Credential: c}); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: got %v, want InvalidArgument", name, err)
		}
	}
	if _, err := srv.UpdateCredential(ctx, nil); status.Code(err) != codes.InvalidArgument {
		t.Errorf("nil request: got %v", err)
	}
	if _, err := srv.UpdateCredential(ctx, &v1alpha1.UpdateCredentialRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("empty request: got %v", err)
	}
	if resp, _ := srv.ListCredentials(ctx, &v1alpha1.ListCredentialsRequest{}); len(resp.GetCredentials()) != 0 {
		t.Errorf("rejected credentials were persisted: %v", resp)
	}
}

func hasState(t *testing.T, st store.Store, name string) bool {
	t.Helper()
	_, err := st.GetCredentialState(context.Background(), "default", name)
	return err == nil
}

func TestUpdateCredential_KeepsStateUnlessTheTokenChangesHands(t *testing.T) {
	ctx := context.Background()
	edit := func(f func(*v1alpha1.OAuthRefreshTokenCredential)) *v1alpha1.Credential {
		c := oauthRefreshCredential("idp", "repo")
		f(c.Spec.GetOauthRefreshToken())
		return c
	}
	for _, tt := range []struct {
		name     string
		updated  *v1alpha1.Credential
		wantKept bool
	}{
		// The provider has already retired the seed, so throwing the rotated
		// refresh token away over an unrelated edit would break the credential.
		{"identical manifest", oauthRefreshCredential("idp", "repo"), true},
		{"scopes", oauthRefreshCredential("idp", "repo read:user"), true},
		{"revocation url", edit(func(o *v1alpha1.OAuthRefreshTokenCredential) { o.RevocationUrl = "https://idp.example.com/revoke" }), true},
		{"client secret", edit(func(o *v1alpha1.OAuthRefreshTokenCredential) {
			o.ClientSecret = &v1alpha1.SecretKeyRef{Name: "idp", Key: "client-secret"}
		}), true},

		{"token url", edit(func(o *v1alpha1.OAuthRefreshTokenCredential) { o.TokenUrl = "https://other.example.com/token" }), false},
		{"client id", edit(func(o *v1alpha1.OAuthRefreshTokenCredential) { o.ClientId = "other" }), false},
		{"seed secret", edit(func(o *v1alpha1.OAuthRefreshTokenCredential) { o.RefreshTokenSecret.Name = "other" }), false},
		{"type", oauthAccessCredential("", "idp", "s"), false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			st := memory.NewStore()
			srv := server.NewServer(st)
			updateCredential(t, srv, oauthRefreshCredential("idp", "repo"))
			if err := st.SaveCredentialState(ctx, "default", "idp", store.CredentialState{RefreshToken: "rt", SeedDigest: "d"}); err != nil {
				t.Fatal(err)
			}

			updateCredential(t, srv, tt.updated)
			if got := hasState(t, st, "idp"); got != tt.wantKept {
				t.Errorf("state kept = %v, want %v", got, tt.wantKept)
			}
		})
	}
}

func TestDeleteCredential_DropsState(t *testing.T) {
	st := memory.NewStore()
	srv := server.NewServer(st)
	updateCredential(t, srv, oauthRefreshCredential("idp", "repo"))
	_ = st.SaveCredentialState(context.Background(), "default", "idp", store.CredentialState{RefreshToken: "rt", SeedDigest: "d"})

	if _, err := srv.DeleteCredential(context.Background(), &v1alpha1.DeleteCredentialRequest{Name: "idp"}); err != nil {
		t.Fatal(err)
	}
	if hasState(t, st, "idp") {
		t.Error("state outlived its credential")
	}
}

func TestGetCredential_NeverReturnsState(t *testing.T) {
	st := memory.NewStore()
	srv := server.NewServer(st)
	updateCredential(t, srv, oauthRefreshCredential("idp", "repo"))
	_ = st.SaveCredentialState(context.Background(), "default", "idp", store.CredentialState{RefreshToken: "super-secret-refresh", SeedDigest: "d"})

	got, err := srv.GetCredential(context.Background(), &v1alpha1.GetCredentialRequest{Name: "idp"})
	if err != nil {
		t.Fatal(err)
	}
	list, _ := srv.ListCredentials(context.Background(), &v1alpha1.ListCredentialsRequest{})
	for _, m := range []string{prototext.Format(got), prototext.Format(list)} {
		if strings.Contains(m, "super-secret-refresh") {
			t.Errorf("state leaked through the API:\n%s", m)
		}
	}
}

// credentialsService builds a credentials.Service over secrets, sharing the store
// and locker with the server the way ax-server wires them.
func credentialsService(st store.Store, secrets map[string]string) *credentials.Service {
	return &credentials.Service{
		Secrets: func(_ context.Context, namespace, name, key string) (string, error) {
			if v, ok := secrets[namespace+"/"+name+"/"+key]; ok {
				return v, nil
			}
			return "", fmt.Errorf("secret %s/%s/%s not found", namespace, name, key)
		},
		State:  st,
		Locker: lock.NewMemoryLocker(),
	}
}

func TestTestCredential_AccessTokenIsMaskedAndNotRevoked(t *testing.T) {
	const raw = "sk-ant-oat01-SECRETSECRETSECRET-zzzz"
	st := memory.NewStore()
	srv := server.NewServer(st, server.Options{Credentials: credentialsService(st, map[string]string{"default/claude-oauth/token": raw})})
	updateCredential(t, srv, oauthAccessCredential("", "claude", "claude-oauth"))

	resp, err := srv.TestCredential(context.Background(), &v1alpha1.TestCredentialRequest{Name: "claude"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetType() != "oauthAccessToken" || resp.GetMaskedToken() != credentials.Mask(raw) {
		t.Errorf("response = %v", resp)
	}
	if resp.GetRevoked() {
		t.Error("an access token must not be reported revoked")
	}
	if resp.GetExpireTime() != nil {
		t.Errorf("expire time = %v, want none", resp.GetExpireTime())
	}
	if strings.Contains(prototext.Format(resp), raw) || strings.Contains(prototext.Format(resp), "SECRETSECRET") {
		t.Errorf("response carries the raw token: %v", resp)
	}
}

func githubFixture(t *testing.T, expires time.Time) (*httptest.Server, *atomic.Int32, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pemKey := string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
	var revokes atomic.Int32
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/access_tokens"):
			w.WriteHeader(http.StatusCreated)
			_, _ = fmt.Fprintf(w, `{"token":"ghs_RAWTOKENVALUE_a1b2","expires_at":%q}`, expires.Format(time.RFC3339))
		case r.Method == http.MethodDelete && r.URL.Path == "/installation/token":
			revokes.Add(1)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(ts.Close)
	return ts, &revokes, pemKey
}

func githubCredential() *v1alpha1.Credential {
	return &v1alpha1.Credential{
		Metadata: &v1alpha1.ObjectMeta{Name: "gh"},
		Spec: &v1alpha1.CredentialSpec{Type: &v1alpha1.CredentialSpec_GithubApp{GithubApp: &v1alpha1.GitHubAppCredential{
			AppId: 1, InstallationId: 2,
			PrivateKeySecret: &v1alpha1.SecretKeyRef{Name: "app-key", Key: "pem"},
			Repositories:     []string{"repo"},
			Permissions:      map[string]string{"contents": "read"},
		}}},
	}
}

func TestTestCredential_GitHubAppReportsExpiryAndRevokes(t *testing.T) {
	expires := time.Date(2026, 10, 15, 14, 3, 0, 0, time.UTC)
	ts, revokes, pemKey := githubFixture(t, expires)
	st := memory.NewStore()
	svc := credentialsService(st, map[string]string{"default/app-key/pem": pemKey})
	svc.HTTPClient, svc.GitHubAPIURL = ts.Client(), ts.URL
	srv := server.NewServer(st, server.Options{Credentials: svc})
	updateCredential(t, srv, githubCredential())

	resp, err := srv.TestCredential(context.Background(), &v1alpha1.TestCredentialRequest{Name: "gh"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetType() != "githubApp" || !resp.GetRevoked() || revokes.Load() != 1 {
		t.Errorf("response = %v, revokes = %d", resp, revokes.Load())
	}
	if !resp.GetExpireTime().AsTime().Equal(expires) {
		t.Errorf("expire time = %v, want %v", resp.GetExpireTime().AsTime(), expires)
	}
	if resp.GetMaskedToken() != "ghs_…a1b2" {
		t.Errorf("masked token = %q", resp.GetMaskedToken())
	}
	if strings.Contains(prototext.Format(resp), "RAWTOKENVALUE") {
		t.Errorf("response carries the raw token: %v", resp)
	}
}

// cancelAfterBody is an http.RoundTripper that cancels a context once the body
// of a matching response has been closed. That is the moment between the
// service reading the issued token and revoking it.
type cancelAfterBody struct {
	base   http.RoundTripper
	cancel context.CancelFunc
	match  func(*http.Request) bool
	// revokeCtxErr is the context error seen by the revocation request.
	revokeCtxErr error
	revoked      bool
}

type closeHook struct {
	io.ReadCloser
	onClose func()
}

func (c closeHook) Close() error { err := c.ReadCloser.Close(); c.onClose(); return err }

func (c *cancelAfterBody) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method == http.MethodDelete {
		c.revoked, c.revokeCtxErr = true, req.Context().Err()
	}
	resp, err := c.base.RoundTrip(req)
	if err == nil && c.match(req) {
		resp.Body = closeHook{ReadCloser: resp.Body, onClose: c.cancel}
	}
	return resp, err
}

func TestTestCredential_RevokesEvenIfTheClientGivesUp(t *testing.T) {
	ts, revokes, pemKey := githubFixture(t, time.Now().Add(time.Hour))
	st := memory.NewStore()
	svc := credentialsService(st, map[string]string{"default/app-key/pem": pemKey})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt := &cancelAfterBody{base: ts.Client().Transport, cancel: cancel, match: func(r *http.Request) bool { return r.Method == http.MethodPost }}
	svc.HTTPClient, svc.GitHubAPIURL = &http.Client{Transport: rt}, ts.URL
	srv := server.NewServer(st, server.Options{Credentials: svc})
	updateCredential(t, srv, githubCredential())

	resp, err := srv.TestCredential(ctx, &v1alpha1.TestCredentialRequest{Name: "gh"})
	if err != nil {
		t.Fatalf("TestCredential: %v", err)
	}
	if ctx.Err() == nil {
		t.Fatal("test setup: the request context was never canceled")
	}
	// A token left valid because the caller disconnected would outlive the test.
	if !resp.GetRevoked() || revokes.Load() != 1 || rt.revokeCtxErr != nil {
		t.Errorf("revoked=%v, server revokes=%d, revoke ctx err=%v", resp.GetRevoked(), revokes.Load(), rt.revokeCtxErr)
	}
}

func TestTestCredential_Errors(t *testing.T) {
	st := memory.NewStore()
	ctx := context.Background()

	t.Run("not configured", func(t *testing.T) {
		srv := server.NewServer(st)
		updateCredential(t, srv, oauthAccessCredential("", "claude", "claude-oauth"))
		if _, err := srv.TestCredential(ctx, &v1alpha1.TestCredentialRequest{Name: "claude"}); status.Code(err) != codes.FailedPrecondition {
			t.Errorf("got %v, want FailedPrecondition", err)
		}
	})
	t.Run("unknown credential", func(t *testing.T) {
		srv := server.NewServer(st, server.Options{Credentials: credentialsService(st, nil)})
		if _, err := srv.TestCredential(ctx, &v1alpha1.TestCredentialRequest{Name: "nope"}); status.Code(err) != codes.NotFound {
			t.Errorf("got %v, want NotFound", err)
		}
	})
	t.Run("issuing fails", func(t *testing.T) {
		srv := server.NewServer(st, server.Options{Credentials: credentialsService(st, nil)})
		updateCredential(t, srv, oauthAccessCredential("", "claude", "claude-oauth"))
		_, err := srv.TestCredential(ctx, &v1alpha1.TestCredentialRequest{Name: "claude"})
		if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "claude-oauth") {
			t.Errorf("got %v, want FailedPrecondition naming the missing secret", err)
		}
	})
	t.Run("nil request", func(t *testing.T) {
		srv := server.NewServer(st, server.Options{Credentials: credentialsService(st, nil)})
		if _, err := srv.TestCredential(ctx, nil); status.Code(err) != codes.InvalidArgument {
			t.Errorf("got %v, want InvalidArgument", err)
		}
	})
}

func credentialTask(name string, bindings ...*v1alpha1.CredentialBinding) *v1alpha1.Task {
	return &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: name, CreationTimestamp: timestamppb.Now()},
		Spec:     &v1alpha1.TaskSpec{Image: "alpine", Credentials: bindings},
	}
}

func TestCreateTask_MissingCredentialIsRejectedBeforeAnythingIsSaved(t *testing.T) {
	st := memory.NewStore()
	rec := &fakeReconciler{}
	srv := server.NewServer(st, server.Options{Reconciler: rec})
	ctx := context.Background()

	_, err := srv.CreateTask(ctx, &v1alpha1.CreateTaskRequest{Task: credentialTask("t", &v1alpha1.CredentialBinding{Name: "gh", Env: "GITHUB_TOKEN"})})
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), `"gh"`) {
		t.Fatalf("got %v, want FailedPrecondition naming the credential", err)
	}
	if _, err := st.GetTask(ctx, "default", "t"); err != store.ErrNotFound {
		t.Errorf("the task was saved: %v", err)
	}
	if rec.reconcileCount != 0 {
		t.Errorf("reconciler ran %d times for a task that was rejected", rec.reconcileCount)
	}

	// A credential in another atespace does not satisfy the binding.
	updateCredential(t, srv, oauthAccessCredential("team-a", "gh", "s"))
	if _, err := srv.CreateTask(ctx, &v1alpha1.CreateTaskRequest{Task: credentialTask("t", &v1alpha1.CredentialBinding{Name: "gh", Env: "GITHUB_TOKEN"})}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("cross-atespace binding: got %v, want FailedPrecondition", err)
	}
}

func TestCreateTask_PassesBoundCredentialsToTheReconciler(t *testing.T) {
	st := memory.NewStore()
	rec := &fakeReconciler{}
	srv := server.NewServer(st, server.Options{Reconciler: rec})
	ctx := context.Background()
	updateCredential(t, srv, oauthAccessCredential("", "gh", "s"))
	updateCredential(t, srv, oauthAccessCredential("", "unbound", "s"))

	task := credentialTask("t",
		&v1alpha1.CredentialBinding{Name: "gh", Env: "GITHUB_TOKEN"},
		&v1alpha1.CredentialBinding{Name: "gh", Env: "GH_TOKEN"},
	)
	if _, err := srv.CreateTask(ctx, &v1alpha1.CreateTaskRequest{Task: task}); err != nil {
		t.Fatal(err)
	}
	if len(rec.lastCredentials) != 1 || rec.lastCredentials[0].GetMetadata().GetName() != "gh" {
		t.Errorf("reconciler got credentials %v, want just gh, once", rec.lastCredentials)
	}

	// Suspend and resume hand the same credentials over.
	for name, call := range map[string]func() error{
		"resume":  func() error { _, err := srv.ResumeTask(ctx, &v1alpha1.ResumeTaskRequest{Name: "t"}); return err },
		"suspend": func() error { _, err := srv.SuspendTask(ctx, &v1alpha1.SuspendTaskRequest{Name: "t"}); return err },
	} {
		rec.lastCredentials = nil
		if err := call(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(rec.lastCredentials) != 1 {
			t.Errorf("%s: reconciler got credentials %v", name, rec.lastCredentials)
		}
	}
}

func TestResumeTask_ToleratesADeletedCredential(t *testing.T) {
	// The actor template already holds the token, so a task must keep working
	// after its credential is deleted. Whether a template needs the credential
	// is the reconciler's call, not the server's.
	st := memory.NewStore()
	rec := &fakeReconciler{}
	srv := server.NewServer(st, server.Options{Reconciler: rec})
	ctx := context.Background()
	updateCredential(t, srv, oauthAccessCredential("", "gh", "s"))
	if _, err := srv.CreateTask(ctx, &v1alpha1.CreateTaskRequest{Task: credentialTask("t", &v1alpha1.CredentialBinding{Name: "gh", Env: "GITHUB_TOKEN"})}); err != nil {
		t.Fatal(err)
	}
	if _, err := srv.DeleteCredential(ctx, &v1alpha1.DeleteCredentialRequest{Name: "gh"}); err != nil {
		t.Fatal(err)
	}

	if _, err := srv.ResumeTask(ctx, &v1alpha1.ResumeTaskRequest{Name: "t"}); err != nil {
		t.Fatalf("ResumeTask: %v", err)
	}
	if len(rec.lastCredentials) != 0 {
		t.Errorf("reconciler got credentials %v for a deleted credential", rec.lastCredentials)
	}
}

func TestCreateTask_RejectsInvalidCredentialBindings(t *testing.T) {
	srv := server.NewServer(memory.NewStore())
	_, err := srv.CreateTask(context.Background(), &v1alpha1.CreateTaskRequest{Task: credentialTask("t", &v1alpha1.CredentialBinding{Name: "gh", Env: "AX_TASK_YAML"})})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("got %v, want InvalidArgument", err)
	}
}
