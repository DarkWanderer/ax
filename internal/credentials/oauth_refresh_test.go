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
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/ax/internal/credentials"
	"github.com/google/ax/internal/lock"
	"github.com/google/ax/internal/store"
	axredis "github.com/google/ax/internal/store/redis"
	"github.com/google/ax/pkg/apis/v1alpha1"
	goredis "github.com/redis/go-redis/v9"
)

// idpRequest is one request the fake identity provider received.
type idpRequest struct {
	path string
	auth string
	form url.Values
}

// fakeIdP is an OAuth authorization server. When rotating, it behaves like the
// strictest real providers: every refresh token works exactly once, and using
// anything but the latest one fails with invalid_grant.
type fakeIdP struct {
	srv *httptest.Server

	mu       sync.Mutex
	valid    string // the only refresh token currently accepted
	rotate   bool
	issued   int
	requests []idpRequest
}

func newFakeIdP(t *testing.T, validRefresh string, rotate bool) *fakeIdP {
	t.Helper()
	f := &fakeIdP{valid: validRefresh, rotate: rotate}
	f.srv = httptest.NewTLSServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeIdP) handle(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, idpRequest{path: r.URL.Path, auth: r.Header.Get("Authorization"), form: r.PostForm})
	w.Header().Set("Content-Type", "application/json")

	switch r.URL.Path {
	case "/token":
		if r.PostForm.Get("grant_type") != "refresh_token" || r.PostForm.Get("refresh_token") != f.valid {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"invalid_grant","error_description":"refresh token ` + r.PostForm.Get("refresh_token") + ` is not valid"}`))
			return
		}
		f.issued++
		if f.rotate {
			f.valid = fmt.Sprintf("rt-%d", f.issued)
			_, _ = fmt.Fprintf(w, `{"access_token":"at-%d","token_type":"Bearer","expires_in":3600,"refresh_token":"rt-%d"}`, f.issued, f.issued)
			return
		}
		_, _ = fmt.Fprintf(w, `{"access_token":"at-%d","token_type":"Bearer","expires_in":3600}`, f.issued)
	case "/revoke":
		w.WriteHeader(http.StatusOK)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (f *fakeIdP) last() idpRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests[len(f.requests)-1]
}

func (f *fakeIdP) count(path string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, r := range f.requests {
		if r.path == path {
			n++
		}
	}
	return n
}

func (f *fakeIdP) setValid(rt string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.valid = rt
}

// refreshCredential returns an oauthRefreshToken credential for the fake IdP.
// The client has a secret unless withClientSecret is false.
func refreshCredential(idp *fakeIdP, withClientSecret, withRevocation bool) *v1alpha1.Credential {
	o := &v1alpha1.OAuthRefreshTokenCredential{
		TokenUrl:           idp.srv.URL + "/token",
		ClientId:           "ax-client",
		Scopes:             []string{"repo", "read:user"},
		RefreshTokenSecret: &v1alpha1.SecretKeyRef{Name: "idp", Key: "refresh-token"},
	}
	if withClientSecret {
		o.ClientSecret = &v1alpha1.SecretKeyRef{Name: "idp", Key: "client-secret"}
	}
	if withRevocation {
		o.RevocationUrl = idp.srv.URL + "/revoke"
	}
	return &v1alpha1.Credential{
		Metadata: &v1alpha1.ObjectMeta{Name: "idp", Atespace: "team-a"},
		Spec:     &v1alpha1.CredentialSpec{Type: &v1alpha1.CredentialSpec_OauthRefreshToken{OauthRefreshToken: o}},
	}
}

func refreshSecrets(seed string) *fakeSecrets {
	return newSecrets(map[string]string{
		secretID("team-a", "idp", "refresh-token"): seed,
		secretID("team-a", "idp", "client-secret"): "s3cret",
	})
}

func serviceFor(idp *fakeIdP, secrets *fakeSecrets) (*credentials.Service, *storeHandle) {
	svc, st := newService(secrets)
	svc.HTTPClient = idp.srv.Client()
	return svc, &storeHandle{st}
}

// storeHandle reads back what the service persisted.
type storeHandle struct{ s store.Store }

func (h *storeHandle) state(t *testing.T) *store.CredentialState {
	t.Helper()
	st, err := h.s.GetCredentialState(context.Background(), "team-a", "idp")
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestOAuthRefresh_IssueWithoutRotation(t *testing.T) {
	idp := newFakeIdP(t, "seed-1", false)
	secrets := refreshSecrets("seed-1")
	svc, state := serviceFor(idp, secrets)
	logs := captureLogs(t)

	cred := refreshCredential(idp, true, false)
	before := time.Now()
	tok, err := svc.Issue(context.Background(), cred)
	if err != nil {
		t.Fatal(err)
	}
	after := time.Now()

	if tok.Value != "at-1" {
		t.Errorf("token = %q", tok.Value)
	}
	if tok.ExpiresAt.Before(before.Add(time.Hour)) || tok.ExpiresAt.After(after.Add(time.Hour)) {
		t.Errorf("expiry = %v, want about an hour from now", tok.ExpiresAt)
	}
	form := idp.last().form
	if form.Get("grant_type") != "refresh_token" || form.Get("refresh_token") != "seed-1" {
		t.Errorf("form = %v", form)
	}
	if form.Get("scope") != "repo read:user" {
		t.Errorf("scope = %q", form.Get("scope"))
	}
	// A provider that does not rotate leaves the seed valid, so nothing is stored.
	if st := state.state(t); st != nil {
		t.Errorf("state = %+v, want none", st)
	}

	// The seed keeps being used.
	if _, err := svc.Issue(context.Background(), cred); err != nil {
		t.Fatal(err)
	}
	if got := idp.last().form.Get("refresh_token"); got != "seed-1" {
		t.Errorf("second refresh used %q", got)
	}
	assertNotLogged(t, logs, "seed-1", "at-1", "s3cret")
}

func TestOAuthRefresh_NoScopeParameterWhenNoScopes(t *testing.T) {
	idp := newFakeIdP(t, "seed-1", false)
	svc, _ := serviceFor(idp, refreshSecrets("seed-1"))
	cred := refreshCredential(idp, true, false)
	cred.Spec.GetOauthRefreshToken().Scopes = nil
	if _, err := svc.Issue(context.Background(), cred); err != nil {
		t.Fatal(err)
	}
	if _, ok := idp.last().form["scope"]; ok {
		t.Errorf("scope sent: %v", idp.last().form)
	}
}

func TestOAuthRefresh_RotatedRefreshTokenIsStoredAndUsedNext(t *testing.T) {
	idp := newFakeIdP(t, "seed-1", true)
	svc, state := serviceFor(idp, refreshSecrets("seed-1"))
	logs := captureLogs(t)
	cred := refreshCredential(idp, true, false)

	tok, err := svc.Issue(context.Background(), cred)
	if err != nil || tok.Value != "at-1" {
		t.Fatalf("Issue = %+v, %v", tok, err)
	}
	// The new refresh token is persisted by the time Issue returns.
	st := state.state(t)
	if st == nil || st.RefreshToken != "rt-1" || st.SeedDigest == "" {
		t.Fatalf("state = %+v", st)
	}

	// The IdP now rejects the seed, so a second refresh only works from the stored token.
	tok, err = svc.Issue(context.Background(), cred)
	if err != nil || tok.Value != "at-2" {
		t.Fatalf("second Issue = %+v, %v", tok, err)
	}
	if got := idp.last().form.Get("refresh_token"); got != "rt-1" {
		t.Errorf("second refresh used %q, want rt-1", got)
	}
	if st := state.state(t); st.RefreshToken != "rt-2" {
		t.Errorf("state after second refresh = %+v", st)
	}
	assertNotLogged(t, logs, "seed-1", "rt-1", "rt-2", "at-1", "at-2", "s3cret")
}

func TestOAuthRefresh_FailsWithoutReturningATokenWhenRotationCannotBeSaved(t *testing.T) {
	idp := newFakeIdP(t, "seed-1", true)
	svc, _ := serviceFor(idp, refreshSecrets("seed-1"))
	svc.State = &failingState{StateStore: svc.State, failSave: true}

	tok, err := svc.Issue(context.Background(), refreshCredential(idp, true, false))
	if err == nil {
		t.Fatal("expected an error: handing out an access token whose rotated refresh token is lost strands the credential")
	}
	if tok.Value != "" {
		t.Errorf("token %q returned alongside the error", tok.Value)
	}
	if strings.Contains(err.Error(), "rt-1") || strings.Contains(err.Error(), "at-1") {
		t.Errorf("error leaks a token: %v", err)
	}
}

func TestOAuthRefresh_ReseedsWhenTheSecretChanges(t *testing.T) {
	idp := newFakeIdP(t, "seed-1", true)
	secrets := refreshSecrets("seed-1")
	svc, state := serviceFor(idp, secrets)
	cred := refreshCredential(idp, true, false)

	if _, err := svc.Issue(context.Background(), cred); err != nil {
		t.Fatal(err)
	}
	if st := state.state(t); st == nil || st.RefreshToken != "rt-1" {
		t.Fatalf("state = %+v", st)
	}

	// An operator re-authorizes and updates the Secret. The stored rt-1 is stale
	// now, and the new seed must win without anyone clearing state by hand.
	secrets.set("team-a", "idp", "refresh-token", "seed-2")
	idp.setValid("seed-2")
	tok, err := svc.Issue(context.Background(), cred)
	if err != nil {
		t.Fatalf("Issue after re-seeding: %v", err)
	}
	if got := idp.last().form.Get("refresh_token"); got != "seed-2" {
		t.Errorf("refresh used %q, want the new seed", got)
	}
	if st := state.state(t); st.RefreshToken != "rt-2" || tok.Value != "at-2" {
		t.Errorf("state = %+v, token = %q", st, tok.Value)
	}

	// And the chain continues from the new seed.
	if _, err := svc.Issue(context.Background(), cred); err != nil {
		t.Fatal(err)
	}
	if got := idp.last().form.Get("refresh_token"); got != "rt-2" {
		t.Errorf("refresh used %q, want rt-2", got)
	}
}

func TestOAuthRefresh_IgnoresStateFromADifferentClientOrEndpoint(t *testing.T) {
	idp := newFakeIdP(t, "seed-1", true)
	svc, _ := serviceFor(idp, refreshSecrets("seed-1"))
	cred := refreshCredential(idp, true, false)
	if _, err := svc.Issue(context.Background(), cred); err != nil {
		t.Fatal(err)
	}

	// The same credential name now points at another client. rt-1 belongs to the
	// old one and must not be sent to it, even though the seed Secret is unchanged.
	idp.setValid("seed-1")
	cred.Spec.GetOauthRefreshToken().ClientId = "another-client"
	if _, err := svc.Issue(context.Background(), cred); err != nil {
		t.Fatalf("Issue with another client: %v", err)
	}
	if got := idp.last().form.Get("refresh_token"); got != "seed-1" {
		t.Errorf("refresh used %q, want the seed", got)
	}
}

func TestOAuthRefresh_ClientAuthentication(t *testing.T) {
	t.Run("basic when there is a client secret", func(t *testing.T) {
		idp := newFakeIdP(t, "seed-1", false)
		secrets := refreshSecrets("seed-1")
		// Characters that must be form-encoded before Basic encoding (RFC 6749 section 2.3.1).
		secrets.set("team-a", "idp", "client-secret", "p@ss:w/rd+ x")
		svc, _ := serviceFor(idp, secrets)
		cred := refreshCredential(idp, true, false)
		cred.Spec.GetOauthRefreshToken().ClientId = "my client"

		if _, err := svc.Issue(context.Background(), cred); err != nil {
			t.Fatal(err)
		}
		req := idp.last()
		want := "Basic " + base64.StdEncoding.EncodeToString([]byte(url.QueryEscape("my client")+":"+url.QueryEscape("p@ss:w/rd+ x")))
		if req.auth != want {
			t.Errorf("Authorization = %q, want %q", req.auth, want)
		}
		if req.form.Has("client_id") || req.form.Has("client_secret") {
			t.Errorf("client credentials leaked into the body: %v", req.form)
		}
	})
	t.Run("client_id in the body without a client secret", func(t *testing.T) {
		idp := newFakeIdP(t, "seed-1", false)
		svc, _ := serviceFor(idp, refreshSecrets("seed-1"))
		if _, err := svc.Issue(context.Background(), refreshCredential(idp, false, false)); err != nil {
			t.Fatal(err)
		}
		req := idp.last()
		if req.auth != "" {
			t.Errorf("Authorization = %q, want none", req.auth)
		}
		if req.form.Get("client_id") != "ax-client" {
			t.Errorf("client_id = %q", req.form.Get("client_id"))
		}
	})
}

func TestOAuthRefresh_ConcurrentRefreshesAreSerialized(t *testing.T) {
	// With single-use rotation, two overlapping refreshes from the same token
	// make one of them fail and invalidate the chain. The lock prevents that.
	const workers = 20
	idp := newFakeIdP(t, "seed-1", true)
	svc, state := serviceFor(idp, refreshSecrets("seed-1"))
	cred := refreshCredential(idp, true, false)

	var wg sync.WaitGroup
	errs := make(chan error, workers)
	tokens := make(chan string, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tok, err := svc.Issue(context.Background(), cred)
			if err != nil {
				errs <- err
				return
			}
			tokens <- tok.Value
		}()
	}
	wg.Wait()
	close(errs)
	close(tokens)

	for err := range errs {
		t.Errorf("concurrent Issue failed: %v", err)
	}
	seen := map[string]bool{}
	for tok := range tokens {
		if seen[tok] {
			t.Errorf("token %q issued twice", tok)
		}
		seen[tok] = true
	}
	if st := state.state(t); st == nil || st.RefreshToken != fmt.Sprintf("rt-%d", workers) {
		t.Errorf("final state = %+v, want rt-%d", st, workers)
	}
}

func TestOAuthRefresh_GivesUpWhenTheLockCannotBeAcquired(t *testing.T) {
	idp := newFakeIdP(t, "seed-1", true)
	svc, _ := serviceFor(idp, refreshSecrets("seed-1"))
	cred := refreshCredential(idp, true, false)

	// Another replica is mid-refresh.
	unlock, err := svc.Locker.Lock(context.Background(), credentials.TokenLockKind, "team-a", "idp")
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := svc.Issue(ctx, cred); err == nil {
		t.Fatal("expected an error while the credential is locked")
	}
	if n := idp.count("/token"); n != 0 {
		t.Errorf("IdP received %d requests without holding the lock", n)
	}
}

func TestOAuthRefresh_IssueErrors(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
		wantErr string
	}{
		{
			name: "oauth error code is reported without the description",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error":"invalid_grant","error_description":"token seed-1 expired"}`))
			},
			wantErr: "invalid_grant",
		},
		{
			name: "non-json failure",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadGateway)
				_, _ = w.Write([]byte("<html>bad gateway seed-1</html>"))
			},
			wantErr: "HTTP 502",
		},
		{
			name:    "success without an access token",
			handler: func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"token_type":"Bearer"}`)) },
			wantErr: "no access_token",
		},
		{
			name:    "malformed success body",
			handler: func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`oops`)) },
			wantErr: "decoding",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewTLSServer(tt.handler)
			defer srv.Close()
			secrets := refreshSecrets("seed-1")
			svc, st := newService(secrets)
			svc.HTTPClient = srv.Client()
			cred := refreshCredential(&fakeIdP{srv: srv}, true, false)

			tok, err := svc.Issue(context.Background(), cred)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want containing %q", err, tt.wantErr)
			}
			if tok.Value != "" {
				t.Errorf("token %q returned with an error", tok.Value)
			}
			if strings.Contains(err.Error(), "seed-1") || strings.Contains(err.Error(), "s3cret") {
				t.Errorf("error leaks a secret: %v", err)
			}
			if _, err := st.GetCredentialState(context.Background(), "team-a", "idp"); !errors.Is(err, store.ErrNotFound) {
				t.Errorf("a failed refresh must not write state, got %v", err)
			}
		})
	}
}

func TestOAuthRefresh_ExpiresInAsStringOrMissing(t *testing.T) {
	for body, wantExpiry := range map[string]bool{
		`{"access_token":"at","expires_in":"7200"}`: true,
		`{"access_token":"at"}`:                     false,
		`{"access_token":"at","expires_in":0}`:      false,
	} {
		srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(body)) }))
		svc, _ := newService(refreshSecrets("seed-1"))
		svc.HTTPClient = srv.Client()
		tok, err := svc.Issue(context.Background(), refreshCredential(&fakeIdP{srv: srv}, true, false))
		srv.Close()
		if err != nil {
			t.Fatalf("%s: %v", body, err)
		}
		if has := !tok.ExpiresAt.IsZero(); has != wantExpiry {
			t.Errorf("%s: expiry set = %t, want %t (%v)", body, has, wantExpiry, tok.ExpiresAt)
		}
	}
}

func TestOAuthRefresh_DoesNotFollowRedirects(t *testing.T) {
	// A redirect would replay the POST body, which holds the refresh token, to
	// whatever host the redirect names.
	var hits int
	var mu sync.Mutex
	elsewhere := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits++
		mu.Unlock()
	}))
	defer elsewhere.Close()
	idp := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, elsewhere.URL, http.StatusTemporaryRedirect)
	}))
	defer idp.Close()

	svc, _ := newService(refreshSecrets("seed-1"))
	// Trust both test servers; they share the same test CA.
	svc.HTTPClient = idp.Client()
	if _, err := svc.Issue(context.Background(), refreshCredential(&fakeIdP{srv: idp}, true, false)); err == nil {
		t.Fatal("expected an error for a redirect")
	}
	mu.Lock()
	defer mu.Unlock()
	if hits != 0 {
		t.Errorf("redirect target received %d requests", hits)
	}
}

func TestOAuthRefresh_RequiresStateAndLocker(t *testing.T) {
	idp := newFakeIdP(t, "seed-1", true)
	cred := refreshCredential(idp, true, false)

	svc, _ := serviceFor(idp, refreshSecrets("seed-1"))
	svc.State = nil
	if _, err := svc.Issue(context.Background(), cred); err == nil {
		t.Error("Issue without a state store should fail, not rotate tokens it cannot keep")
	}
	svc, _ = serviceFor(idp, refreshSecrets("seed-1"))
	svc.Locker = nil
	if _, err := svc.Issue(context.Background(), cred); err == nil {
		t.Error("Issue without a locker should fail")
	}
	if n := idp.count("/token"); n != 0 {
		t.Errorf("IdP received %d requests from a misconfigured service", n)
	}
}

func TestOAuthRefresh_Revoke(t *testing.T) {
	t.Run("RFC 7009 request with client authentication", func(t *testing.T) {
		idp := newFakeIdP(t, "seed-1", false)
		svc, _ := serviceFor(idp, refreshSecrets("seed-1"))
		cred := refreshCredential(idp, true, true)

		revoked, err := svc.Revoke(context.Background(), cred, credentials.Token{Value: "at-1"})
		if err != nil || !revoked {
			t.Fatalf("Revoke = %v, %v", revoked, err)
		}
		req := idp.last()
		if req.path != "/revoke" || req.form.Get("token") != "at-1" || req.form.Get("token_type_hint") != "access_token" {
			t.Errorf("revocation request = %+v", req)
		}
		if !strings.HasPrefix(req.auth, "Basic ") {
			t.Errorf("Authorization = %q", req.auth)
		}
	})
	t.Run("public client sends client_id in the body", func(t *testing.T) {
		idp := newFakeIdP(t, "seed-1", false)
		svc, _ := serviceFor(idp, refreshSecrets("seed-1"))
		if _, err := svc.Revoke(context.Background(), refreshCredential(idp, false, true), credentials.Token{Value: "at-1"}); err != nil {
			t.Fatal(err)
		}
		if req := idp.last(); req.auth != "" || req.form.Get("client_id") != "ax-client" {
			t.Errorf("revocation request = %+v", req)
		}
	})
	t.Run("without a revocation url nothing is sent", func(t *testing.T) {
		idp := newFakeIdP(t, "seed-1", false)
		svc, _ := serviceFor(idp, refreshSecrets("seed-1"))
		revoked, err := svc.Revoke(context.Background(), refreshCredential(idp, true, false), credentials.Token{Value: "at-1"})
		if revoked || err != nil {
			t.Errorf("Revoke = %v, %v; want false, nil", revoked, err)
		}
		if n := idp.count("/revoke"); n != 0 {
			t.Errorf("%d revocation requests sent", n)
		}
	})
	t.Run("failure is an error and not a revocation", func(t *testing.T) {
		srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
		defer srv.Close()
		svc, _ := newService(refreshSecrets("seed-1"))
		svc.HTTPClient = srv.Client()
		cred := refreshCredential(&fakeIdP{srv: srv}, true, true)
		revoked, err := svc.Revoke(context.Background(), cred, credentials.Token{Value: "at-1"})
		if revoked || err == nil {
			t.Errorf("Revoke = %v, %v; want false and an error", revoked, err)
		}
	})
}

func TestOAuthRefresh_ReplicasSharingRedisStayConsistent(t *testing.T) {
	// ax-server runs as several replicas that share Redis for both the rotated
	// refresh token and the lock. With single-use rotation, any replica refreshing
	// from a stale token would invalidate the whole chain.
	const replicas, perReplica = 3, 5
	mr := miniredis.RunT(t)
	idp := newFakeIdP(t, "seed-1", true)
	secrets := refreshSecrets("seed-1")
	cred := refreshCredential(idp, true, false)

	var services []*credentials.Service
	for i := 0; i < replicas; i++ {
		client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { client.Close() })
		services = append(services, &credentials.Service{
			Secrets:    secrets.Resolve,
			State:      axredis.NewStore(client, axredis.Options{}),
			Locker:     lock.NewRedisLocker(client, lock.RedisLockerOptions{FallbackInterval: 10 * time.Millisecond}),
			HTTPClient: idp.srv.Client(),
		})
	}

	var wg sync.WaitGroup
	errs := make(chan error, replicas*perReplica)
	for _, svc := range services {
		for i := 0; i < perReplica; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := svc.Issue(context.Background(), cred); err != nil {
					errs <- err
				}
			}()
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("Issue: %v", err)
	}

	st, err := services[0].State.GetCredentialState(context.Background(), "team-a", "idp")
	if err != nil || st.RefreshToken != fmt.Sprintf("rt-%d", replicas*perReplica) {
		t.Errorf("final state = %+v, %v; want rt-%d", st, err, replicas*perReplica)
	}
}
