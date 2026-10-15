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

package credentials_test

import (
	"context"
	"strings"
	"testing"

	"github.com/google/ax/internal/credentials"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

func TestMask(t *testing.T) {
	tests := []struct{ in, want string }{
		{"", "****"},
		{"short", "****"},
		{"123456789012345", "****"}, // 15 characters
		{"1234567890123456", "1234…3456"},
		{"ghs_abcdefghijklmnopqrstuvwxyz0123a1b2", "ghs_…a1b2"},
		{"sk-ant-oat01-abcdefghijklmnop", "sk-a…mnop"},
		// Masking must never cut a multi-byte character in half: the result
		// travels in a protobuf string, which must be valid UTF-8.
		{"ééééééééééééééééé", "éééé…éééé"},
	}
	for _, tt := range tests {
		if got := credentials.Mask(tt.in); got != tt.want {
			t.Errorf("Mask(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestMask_NeverRevealsTheMiddle(t *testing.T) {
	token := "ghs_" + strings.Repeat("x", 30) + "SECRETMIDDLE" + strings.Repeat("y", 30) + "a1b2"
	if got := credentials.Mask(token); strings.Contains(got, "SECRETMIDDLE") || len(got) > 12 {
		t.Errorf("Mask leaked or kept too much: %q", got)
	}
}

func TestOAuthAccessToken_IssuePassesTheSecretThrough(t *testing.T) {
	secrets := newSecrets(map[string]string{secretID("team-a", "claude-oauth", "token"): "sk-ant-oat01-abc"})
	svc, _ := newService(secrets)
	cred := accessTokenCredential()

	tok, err := svc.Issue(context.Background(), cred)
	if err != nil {
		t.Fatal(err)
	}
	if tok.Value != "sk-ant-oat01-abc" {
		t.Errorf("token = %q", tok.Value)
	}
	if !tok.ExpiresAt.IsZero() {
		t.Errorf("an access token has no known expiry, got %v", tok.ExpiresAt)
	}
	// The Secret is looked up in the namespace named after the credential's atespace.
	if len(secrets.calls) != 1 || secrets.calls[0] != "team-a/claude-oauth/token" {
		t.Errorf("secret lookups = %v", secrets.calls)
	}
}

func TestOAuthAccessToken_RevokeIsANoOp(t *testing.T) {
	secrets := newSecrets(map[string]string{secretID("team-a", "claude-oauth", "token"): "sk-ant-oat01-abc"})
	svc, _ := newService(secrets)
	cred := accessTokenCredential()
	tok, _ := svc.Issue(context.Background(), cred)

	// Revoking the stored token would invalidate the secret itself, so it is left alone.
	revoked, err := svc.Revoke(context.Background(), cred, tok)
	if err != nil || revoked {
		t.Errorf("Revoke = %v, %v; want false, nil", revoked, err)
	}
}

func TestOAuthAccessToken_ErrorsWithoutAUsableSecret(t *testing.T) {
	svc, _ := newService(newSecrets(map[string]string{}))
	if _, err := svc.Issue(context.Background(), accessTokenCredential()); err == nil {
		t.Error("expected an error for a missing Secret")
	}
	svc, _ = newService(newSecrets(map[string]string{secretID("team-a", "claude-oauth", "token"): "  \n"}))
	if _, err := svc.Issue(context.Background(), accessTokenCredential()); err == nil {
		t.Error("expected an error for a blank token")
	}
}

func TestOAuthAccessToken_TrimsTrailingNewline(t *testing.T) {
	// `kubectl create secret --from-file` keeps the trailing newline of the file.
	svc, _ := newService(newSecrets(map[string]string{secretID("team-a", "claude-oauth", "token"): "sk-ant-oat01-abc\n"}))
	tok, err := svc.Issue(context.Background(), accessTokenCredential())
	if err != nil || tok.Value != "sk-ant-oat01-abc" {
		t.Errorf("Issue = %q, %v", tok.Value, err)
	}
}

func TestService_RejectsCredentialsWithoutAType(t *testing.T) {
	svc, _ := newService(newSecrets(nil))
	for name, cred := range map[string]*v1alpha1.Credential{
		"nil":      nil,
		"no spec":  {Metadata: &v1alpha1.ObjectMeta{Name: "x"}},
		"no type":  {Metadata: &v1alpha1.ObjectMeta{Name: "x"}, Spec: &v1alpha1.CredentialSpec{}},
		"nil type": {Metadata: &v1alpha1.ObjectMeta{Name: "x"}, Spec: &v1alpha1.CredentialSpec{Type: &v1alpha1.CredentialSpec_GithubApp{}}},
	} {
		if _, err := svc.Issue(context.Background(), cred); err == nil {
			t.Errorf("%s: Issue should fail", name)
		}
		if _, err := svc.Revoke(context.Background(), cred, credentials.Token{Value: "t"}); err == nil {
			t.Errorf("%s: Revoke should fail", name)
		}
	}
}

func TestService_NilReceiverFails(t *testing.T) {
	var svc *credentials.Service
	if _, err := svc.Issue(context.Background(), accessTokenCredential()); err == nil {
		t.Error("Issue on a nil service should fail, not panic")
	}
}
