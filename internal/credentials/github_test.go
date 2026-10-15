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
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/ax/internal/credentials"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

func newPEM(t *testing.T) (*rsa.PrivateKey, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return key, string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
}

func githubCredential() *v1alpha1.Credential {
	return &v1alpha1.Credential{
		Metadata: &v1alpha1.ObjectMeta{Name: "gh", Atespace: "team-a"},
		Spec: &v1alpha1.CredentialSpec{Type: &v1alpha1.CredentialSpec_GithubApp{GithubApp: &v1alpha1.GitHubAppCredential{
			AppId: 7, InstallationId: 42,
			PrivateKeySecret: &v1alpha1.SecretKeyRef{Name: "app-key", Key: "pem"},
			Repositories:     []string{"first", "second", "third"},
			Permissions:      map[string]string{"contents": "read"},
		}}},
	}
}

// verifyAppJWT checks that the bearer value is an RS256 JWT signed by key and
// issued by the app, expiring no more than ten minutes from now (GitHub's maximum).
func verifyAppJWT(t *testing.T, bearer string, key *rsa.PrivateKey, appID string) {
	t.Helper()
	parts := strings.Split(strings.TrimPrefix(bearer, "Bearer "), ".")
	if len(parts) != 3 {
		t.Errorf("not a JWT: %q", bearer)
		return
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		t.Errorf("signature: %v", err)
		return
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(&key.PublicKey, crypto.SHA256, digest[:], sig); err != nil {
		t.Errorf("JWT signature does not verify: %v", err)
	}
	raw, _ := base64.RawURLEncoding.DecodeString(parts[1])
	var claims struct {
		Iss string `json:"iss"`
		Iat int64  `json:"iat"`
		Exp int64  `json:"exp"`
	}
	if err := json.Unmarshal(raw, &claims); err != nil {
		t.Errorf("claims: %v", err)
		return
	}
	if claims.Iss != appID {
		t.Errorf("iss = %q, want %q", claims.Iss, appID)
	}
	if now := time.Now().Unix(); claims.Exp > now+600 || claims.Exp <= now || claims.Iat > now {
		t.Errorf("JWT lifetime out of bounds: iat=%d exp=%d", claims.Iat, claims.Exp)
	}
}

func TestGitHubApp_IssueAndRevokeScopedInstallationToken(t *testing.T) {
	key, pemKey := newPEM(t)
	expires := time.Date(2026, 10, 15, 14, 3, 0, 0, time.UTC)

	var mu sync.Mutex
	var issued, revoked bool
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.URL.Path {
		case "/app/installations/42/access_tokens":
			issued = true
			if r.Method != http.MethodPost {
				t.Errorf("method = %s", r.Method)
			}
			verifyAppJWT(t, r.Header.Get("Authorization"), key, "7")
			var body struct {
				Repositories []string          `json:"repositories"`
				Permissions  map[string]string `json:"permissions"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if !reflect.DeepEqual(body.Repositories, []string{"first", "second", "third"}) {
				t.Errorf("repository scope = %v", body.Repositories)
			}
			if !reflect.DeepEqual(body.Permissions, map[string]string{"contents": "read"}) {
				t.Errorf("permissions = %v", body.Permissions)
			}
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"token":"ghs_secret","expires_at":"` + expires.Format(time.RFC3339) + `"}`))
		case "/installation/token":
			revoked = true
			if r.Method != http.MethodDelete {
				t.Errorf("method = %s", r.Method)
			}
			if got := r.Header.Get("Authorization"); got != "Bearer ghs_secret" {
				t.Errorf("revoke auth = %s", got)
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected endpoint %s", r.URL.Path)
		}
	}))
	defer srv.Close()

	secrets := newSecrets(map[string]string{secretID("team-a", "app-key", "pem"): pemKey})
	svc, _ := newService(secrets)
	svc.GitHubAPIURL = srv.URL
	svc.HTTPClient = srv.Client()
	logs := captureLogs(t)

	cred := githubCredential()
	tok, err := svc.Issue(context.Background(), cred)
	if err != nil {
		t.Fatal(err)
	}
	if tok.Value != "ghs_secret" {
		t.Errorf("token = %q", tok.Value)
	}
	if !tok.ExpiresAt.Equal(expires) {
		t.Errorf("expiry = %v, want %v", tok.ExpiresAt, expires)
	}
	if len(secrets.calls) != 1 || secrets.calls[0] != "team-a/app-key/pem" {
		t.Errorf("secret lookups = %v", secrets.calls)
	}

	ok, err := svc.Revoke(context.Background(), cred, tok)
	if err != nil || !ok {
		t.Fatalf("Revoke = %v, %v", ok, err)
	}
	if !issued || !revoked {
		t.Fatalf("issued=%t revoked=%t", issued, revoked)
	}
	assertNotLogged(t, logs, "ghs_secret", pemKey)
}

func TestGitHubApp_IssueWithoutExpiryLeavesItZero(t *testing.T) {
	_, pemKey := newPEM(t)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"token":"ghs_x"}`))
	}))
	defer srv.Close()
	svc, _ := newService(newSecrets(map[string]string{secretID("team-a", "app-key", "pem"): pemKey}))
	svc.GitHubAPIURL, svc.HTTPClient = srv.URL, srv.Client()

	tok, err := svc.Issue(context.Background(), githubCredential())
	if err != nil || tok.Value != "ghs_x" || !tok.ExpiresAt.IsZero() {
		t.Errorf("Issue = %+v, %v", tok, err)
	}
}

func TestGitHubApp_IssueErrors(t *testing.T) {
	_, pemKey := newPEM(t)
	tests := []struct {
		name    string
		handler http.HandlerFunc
		key     string
		wantErr string
	}{
		{
			name: "http error does not echo the body",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"message":"Bad credentials ghs_leak"}`))
			},
			key:     pemKey,
			wantErr: "HTTP 401",
		},
		{
			name:    "empty token in response",
			handler: func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"token":""}`)) },
			key:     pemKey,
			wantErr: "no token",
		},
		{
			name:    "malformed response",
			handler: func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`not json`)) },
			key:     pemKey,
			wantErr: "decoding",
		},
		{
			name:    "private key is not PEM",
			handler: func(w http.ResponseWriter, r *http.Request) { t.Error("must not call GitHub with an unusable key") },
			key:     "not a key",
			wantErr: "PEM",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewTLSServer(tt.handler)
			defer srv.Close()
			svc, _ := newService(newSecrets(map[string]string{secretID("team-a", "app-key", "pem"): tt.key}))
			svc.GitHubAPIURL, svc.HTTPClient = srv.URL, srv.Client()
			_, err := svc.Issue(context.Background(), githubCredential())
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want containing %q", err, tt.wantErr)
			}
			if strings.Contains(err.Error(), "ghs_leak") || strings.Contains(err.Error(), "not a key") {
				t.Errorf("error leaks secret material: %v", err)
			}
		})
	}
}

func TestGitHubApp_RevokeStatuses(t *testing.T) {
	for _, tt := range []struct {
		status      int
		wantRevoked bool
		wantErr     bool
	}{
		{http.StatusNoContent, true, false},
		// The token is already unusable; the caller's goal is met.
		{http.StatusUnauthorized, true, false},
		{http.StatusNotFound, true, false},
		{http.StatusInternalServerError, false, true},
		{http.StatusForbidden, false, true},
	} {
		srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(tt.status) }))
		svc, _ := newService(newSecrets(nil))
		svc.GitHubAPIURL, svc.HTTPClient = srv.URL, srv.Client()
		revoked, err := svc.Revoke(context.Background(), githubCredential(), credentials.Token{Value: "ghs_x"})
		srv.Close()
		if revoked != tt.wantRevoked || (err != nil) != tt.wantErr {
			t.Errorf("status %d: Revoke = %v, %v", tt.status, revoked, err)
		}
	}
}

func TestGitHubApp_RevokeEmptyTokenMakesNoRequest(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("unexpected request") }))
	defer srv.Close()
	svc, _ := newService(newSecrets(nil))
	svc.GitHubAPIURL, svc.HTTPClient = srv.URL, srv.Client()
	revoked, err := svc.Revoke(context.Background(), githubCredential(), credentials.Token{})
	if revoked || err != nil {
		t.Errorf("Revoke = %v, %v", revoked, err)
	}
}
