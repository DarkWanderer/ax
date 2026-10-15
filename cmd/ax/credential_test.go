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

package main

import (
	"context"
	"net"
	"os"
	"strings"
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
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v3"
)

func TestNormalizeKind(t *testing.T) {
	for in, want := range map[string]string{
		"task": "Task", "tasks": "Task", "Task": "Task",
		"workspace": "Workspace", "workspaces": "Workspace",
		"model": "Model", "models": "Model",
		"credential": "Credential", "credentials": "Credential", "Credential": "Credential",
	} {
		got, err := normalizeKind(in)
		if err != nil || got != want {
			t.Errorf("normalizeKind(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, in := range []string{"", "pods", "credentialprovider"} {
		if _, err := normalizeKind(in); err == nil {
			t.Errorf("normalizeKind(%q) should fail", in)
		}
	}
	if _, err := normalizeKind("nope"); err == nil || !strings.Contains(err.Error(), "credential") {
		t.Errorf("the error for an unknown kind should list credentials: %v", err)
	}
}

// startServer serves the API on a loopback port, with creds as the credentials
// service, and returns the address and the backing store.
func startServer(t *testing.T, creds *credentials.Service) (string, *memory.MemoryStore) {
	t.Helper()
	s := memory.NewStore()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := server.NewServer(s, server.Options{Credentials: creds}).GRPCServer()
	t.Cleanup(srv.Stop)
	go func() { _ = srv.Serve(listener) }()
	return listener.Addr().String(), s
}

// captureStdout redirects os.Stdout to a file for the rest of the test and
// returns a function that reads what was written. Tests using it must not run in parallel.
func captureStdout(t *testing.T) func() string {
	t.Helper()
	output, err := os.CreateTemp(t.TempDir(), "stdout")
	if err != nil {
		t.Fatal(err)
	}
	stdout := os.Stdout
	os.Stdout = output
	t.Cleanup(func() {
		os.Stdout = stdout
		_ = output.Close()
	})
	return func() string {
		data, err := os.ReadFile(output.Name())
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
}

func applyManifest(t *testing.T, addr, manifest string) error {
	t.Helper()
	var node yaml.Node
	if err := yaml.Unmarshal([]byte(manifest), &node); err != nil {
		t.Fatal(err)
	}
	_, _, _, err := applyDocument(context.Background(), mustClient(t, addr), node.Content[0])
	return err
}

func mustClient(t *testing.T, addr string) v1alpha1.AXClient {
	t.Helper()
	client, conn, err := getAXClient(addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return client
}

const claudeCredentialManifest = `
apiVersion: ax.io/v1alpha1
kind: Credential
metadata:
  name: claude
spec:
  oauthAccessToken:
    tokenSecret:
      name: claude-oauth
      key: token
`

func TestApplyDocument_Credential(t *testing.T) {
	addr, s := startServer(t, nil)
	client := mustClient(t, addr)
	apply := func(manifest string) (string, string, string) {
		var node yaml.Node
		if err := yaml.Unmarshal([]byte(manifest), &node); err != nil {
			t.Fatal(err)
		}
		kind, name, outcome, err := applyDocument(context.Background(), client, node.Content[0])
		if err != nil {
			t.Fatalf("applyDocument: %v", err)
		}
		return kind, name, outcome
	}

	if kind, name, outcome := apply(claudeCredentialManifest); kind != "Credential" || name != "claude" || outcome != "created" {
		t.Errorf("first apply = %s/%s %s", kind, name, outcome)
	}
	if _, _, outcome := apply(claudeCredentialManifest); outcome != "unchanged" {
		t.Errorf("second apply = %s, want unchanged", outcome)
	}
	if _, _, outcome := apply(strings.Replace(claudeCredentialManifest, "claude-oauth", "claude-oauth-2", 1)); outcome != "configured" {
		t.Errorf("changed apply = %s, want configured", outcome)
	}
	got, err := s.GetCredential(context.Background(), "default", "claude")
	if err != nil || got.GetSpec().GetOauthAccessToken().GetTokenSecret().GetName() != "claude-oauth-2" {
		t.Errorf("stored credential = %v, %v", got, err)
	}
}

func TestApplyDocument_CredentialErrors(t *testing.T) {
	addr, _ := startServer(t, nil)
	for name, tc := range map[string]struct {
		manifest string
		wantErr  string
	}{
		"two types": {
			manifest: "kind: Credential\nmetadata: {name: x}\nspec:\n  oauthAccessToken: {tokenSecret: {name: a, key: b}}\n  githubApp: {appId: 1}\n",
			wantErr:  "oneof",
		},
		"no type": {
			manifest: "kind: Credential\nmetadata: {name: x}\nspec: {}\n",
			wantErr:  "exactly one credential type",
		},
		"plain http token url": {
			manifest: "kind: Credential\nmetadata: {name: x}\nspec:\n  oauthRefreshToken: {tokenUrl: 'http://idp/token', clientId: c, refreshTokenSecret: {name: a, key: b}}\n",
			wantErr:  "https",
		},
		"unknown field": {
			manifest: "kind: Credential\nmetadata: {name: x}\nspec:\n  oauthAccessToken: {tokenSecert: {name: a, key: b}}\n",
			wantErr:  "tokenSecert",
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := applyManifest(t, addr, tc.manifest)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestRunApply_UnsupportedKindListsCredential(t *testing.T) {
	addr, _ := startServer(t, nil)
	err := applyManifest(t, addr, "kind: Pod\nmetadata: {name: x}\n")
	if err == nil || !strings.Contains(err.Error(), "Credential") {
		t.Fatalf("error = %v, want one that lists Credential among the supported kinds", err)
	}
}

func TestRunDelete_Credential(t *testing.T) {
	addr, s := startServer(t, nil)
	if err := s.SaveCredential(context.Background(), &v1alpha1.Credential{Metadata: &v1alpha1.ObjectMeta{Name: "gone", Atespace: "default"}}); err != nil {
		t.Fatal(err)
	}
	out := captureStdout(t)

	for _, kind := range []string{"credential", "credentials"} {
		if err := runDelete(addr, "default", []string{kind, "gone"}); err != nil {
			t.Fatalf("runDelete(%s): %v", kind, err)
		}
	}
	if got := out(); got != "credential.ax.io/gone deleted\ncredential.ax.io/gone deleted\n" {
		t.Errorf("output = %q", got)
	}
	if _, err := s.GetCredential(context.Background(), "default", "gone"); err == nil {
		t.Error("credential still stored")
	}
}

func TestRunGet_CredentialListShowsType(t *testing.T) {
	addr, s := startServer(t, nil)
	ctx := context.Background()
	for _, c := range []*v1alpha1.Credential{
		{Metadata: &v1alpha1.ObjectMeta{Name: "gh", Atespace: "default"}, Spec: &v1alpha1.CredentialSpec{Type: &v1alpha1.CredentialSpec_GithubApp{GithubApp: &v1alpha1.GitHubAppCredential{}}}},
		{Metadata: &v1alpha1.ObjectMeta{Name: "claude", Atespace: "default"}, Spec: &v1alpha1.CredentialSpec{Type: &v1alpha1.CredentialSpec_OauthAccessToken{OauthAccessToken: &v1alpha1.OAuthAccessTokenCredential{}}}},
		{Metadata: &v1alpha1.ObjectMeta{Name: "idp", Atespace: "default"}, Spec: &v1alpha1.CredentialSpec{Type: &v1alpha1.CredentialSpec_OauthRefreshToken{OauthRefreshToken: &v1alpha1.OAuthRefreshTokenCredential{}}}},
	} {
		if err := s.SaveCredential(ctx, c); err != nil {
			t.Fatal(err)
		}
	}
	out := captureStdout(t)
	if err := runGet(addr, "default", []string{"credentials"}); err != nil {
		t.Fatal(err)
	}
	rows := strings.Split(strings.TrimSpace(out()), "\n")
	if len(rows) != 4 || strings.Fields(rows[0])[0] != "NAME" || strings.Fields(rows[0])[2] != "TYPE" {
		t.Fatalf("unexpected table:\n%s", strings.Join(rows, "\n"))
	}
	types := map[string]string{}
	for _, row := range rows[1:] {
		f := strings.Fields(row)
		types[f[0]] = f[2]
	}
	if types["gh"] != "githubApp" || types["claude"] != "oauthAccessToken" || types["idp"] != "oauthRefreshToken" {
		t.Errorf("types = %v", types)
	}
}

func TestRunGet_CredentialYAMLHasOnlySecretReferences(t *testing.T) {
	addr, s := startServer(t, nil)
	cred := &v1alpha1.Credential{
		ApiVersion: v1alpha1.APIVersion,
		Kind:       v1alpha1.KindCredential,
		Metadata:   &v1alpha1.ObjectMeta{Name: "idp", Atespace: "default"},
		Spec: &v1alpha1.CredentialSpec{Type: &v1alpha1.CredentialSpec_OauthRefreshToken{OauthRefreshToken: &v1alpha1.OAuthRefreshTokenCredential{
			TokenUrl: "https://idp/token", ClientId: "c",
			RefreshTokenSecret: &v1alpha1.SecretKeyRef{Name: "idp", Key: "refresh"},
		}}},
	}
	if err := s.SaveCredential(context.Background(), cred); err != nil {
		t.Fatal(err)
	}
	// Rotated refresh tokens live in state, which no read path may return.
	if err := s.SaveCredentialState(context.Background(), "default", "idp", storeState("rotated-secret-token")); err != nil {
		t.Fatal(err)
	}
	out := captureStdout(t)
	if err := runGet(addr, "default", []string{"credential", "idp"}); err != nil {
		t.Fatal(err)
	}
	got := out()
	if strings.Contains(got, "rotated-secret-token") {
		t.Errorf("state leaked into get output:\n%s", got)
	}
	for _, want := range []string{"kind: Credential", "oauthRefreshToken:", "refreshTokenSecret:", "name: idp", "key: refresh"} {
		if !strings.Contains(got, want) {
			t.Errorf("output lacks %q:\n%s", want, got)
		}
	}
}

func TestFormatTestResult(t *testing.T) {
	now := time.Date(2026, 10, 15, 13, 4, 0, 0, time.UTC)
	for name, tc := range map[string]struct {
		resp *v1alpha1.TestCredentialResponse
		want string
	}{
		"github app": {
			resp: &v1alpha1.TestCredentialResponse{
				Type: "githubApp", MaskedToken: "ghs_…a1b2", Revoked: true,
				ExpireTime: timestamppb.New(time.Date(2026, 10, 15, 14, 3, 0, 0, time.UTC)),
			},
			want: "credential.ax.io/gh ok\n" +
				"  type:    githubApp\n" +
				"  token:   ghs_…a1b2\n" +
				"  expires: 2026-10-15T14:03:00Z (in 59m)\n" +
				"  revoked: yes\n",
		},
		"no expiry, not revoked": {
			resp: &v1alpha1.TestCredentialResponse{Type: "oauthAccessToken", MaskedToken: "sk-a…mnop"},
			want: "credential.ax.io/gh ok\n" +
				"  type:    oauthAccessToken\n" +
				"  token:   sk-a…mnop\n" +
				"  expires: unknown\n" +
				"  revoked: no\n",
		},
		"already expired": {
			resp: &v1alpha1.TestCredentialResponse{
				Type: "oauthRefreshToken", MaskedToken: "****",
				ExpireTime: timestamppb.New(now.Add(-3 * time.Minute)),
			},
			want: "credential.ax.io/gh ok\n" +
				"  type:    oauthRefreshToken\n" +
				"  token:   ****\n" +
				"  expires: 2026-10-15T13:01:00Z (3m ago)\n" +
				"  revoked: no\n",
		},
	} {
		t.Run(name, func(t *testing.T) {
			if got := formatTestResult("gh", tc.resp, now); got != tc.want {
				t.Errorf("formatTestResult:\n got: %q\nwant: %q", got, tc.want)
			}
		})
	}
}

func TestRunTest_Credential(t *testing.T) {
	const raw = "sk-ant-oat01-NOT-FOR-DISPLAY-0123456789-zzzz"
	svc := &credentials.Service{
		Secrets: func(context.Context, string, string, string) (string, error) { return raw, nil },
		State:   memory.NewStore(),
		Locker:  lock.NewMemoryLocker(),
	}
	addr, _ := startServer(t, svc)
	if err := applyManifest(t, addr, claudeCredentialManifest); err != nil {
		t.Fatal(err)
	}
	out := captureStdout(t)

	if err := runTest(addr, "default", []string{"credential", "claude"}); err != nil {
		t.Fatalf("runTest: %v", err)
	}
	got := out()
	if strings.Contains(got, raw) || strings.Contains(got, "NOT-FOR-DISPLAY") {
		t.Errorf("output shows the raw token:\n%s", got)
	}
	for _, want := range []string{"credential.ax.io/claude ok", "type:    oauthAccessToken", "token:   " + credentials.Mask(raw), "revoked: no"} {
		if !strings.Contains(got, want) {
			t.Errorf("output lacks %q:\n%s", want, got)
		}
	}
}

func TestRunTest_Errors(t *testing.T) {
	addr, _ := startServer(t, nil)

	if err := runTest(addr, "default", nil); err == nil || !strings.Contains(err.Error(), "usage") {
		t.Errorf("no args: %v", err)
	}
	if err := runTest(addr, "default", []string{"credential"}); err == nil || !strings.Contains(err.Error(), "usage") {
		t.Errorf("no name: %v", err)
	}
	if err := runTest(addr, "default", []string{"task", "x"}); err == nil || !strings.Contains(err.Error(), "credential") {
		t.Errorf("task kind: %v", err)
	}
	// Without a credentials service the server cannot test anything.
	if err := runTest(addr, "default", []string{"credential", "x"}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("no service: %v", err)
	}

	withService, _ := startServer(t, &credentials.Service{})
	if err := runTest(withService, "default", []string{"credentials", "missing"}); status.Code(err) != codes.NotFound {
		t.Errorf("missing credential: %v", err)
	}
}

func storeState(refresh string) store.CredentialState {
	return store.CredentialState{RefreshToken: refresh, SeedDigest: "d"}
}
