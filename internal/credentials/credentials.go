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

// Package credentials obtains and revokes the tokens that Credential resources
// describe. Token values pass through this package but are never logged and never
// appear in returned errors.
package credentials

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/ax/internal/lock"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

const (
	// TokenLockKind is the lock kind that serializes token operations on one
	// credential. Refreshing a rotating refresh token must not overlap with itself.
	TokenLockKind = "credential-token"

	defaultAtespace    = "default"
	defaultHTTPTimeout = 15 * time.Second
	// maxResponseBytes bounds how much of an issuer's response is read.
	maxResponseBytes = 1 << 20
	defaultGitHubAPI = "https://api.github.com"
)

// Token is an issued credential value.
type Token struct {
	Value string
	// ExpiresAt is the zero time when the issuer reports no expiry.
	ExpiresAt time.Time
}

// Mask shortens a token for display: the first and last four characters when
// the token is long enough that this reveals little, otherwise nothing at all.
func Mask(v string) string {
	r := []rune(v)
	if len(r) < 16 {
		return "****"
	}
	return string(r[:4]) + "…" + string(r[len(r)-4:])
}

// SecretResolver looks up a key from a Kubernetes Secret. It has the signature
// of model.GetKubernetesSecret.
type SecretResolver func(ctx context.Context, namespace, secretName, key string) (string, error)

// StateStore persists what the oauthRefreshToken type must remember between
// calls. store.Store implements it.
type StateStore interface {
	GetCredentialState(ctx context.Context, atespace, name string) (*store.CredentialState, error)
	SaveCredentialState(ctx context.Context, atespace, name string, state store.CredentialState) error
}

// Service issues and revokes tokens for Credentials.
type Service struct {
	// Secrets resolves Secret references. The namespace passed to it is the
	// credential's atespace.
	Secrets SecretResolver
	// State and Locker are required by the oauthRefreshToken type, whose refresh
	// tokens rotate: State keeps the latest one and Locker serializes refreshes
	// across replicas.
	State  StateStore
	Locker lock.Locker
	// HTTPClient performs the requests to issuers. Redirects are never followed
	// whatever the client says, because the requests carry secrets.
	HTTPClient *http.Client
	// GitHubAPIURL overrides https://api.github.com.
	GitHubAPIURL string
}

// Issue obtains a token for the credential.
func (s *Service) Issue(ctx context.Context, cred *v1alpha1.Credential) (Token, error) {
	if s == nil {
		return Token{}, errors.New("credentials are not configured on this server")
	}
	switch t := cred.GetSpec().GetType().(type) {
	case *v1alpha1.CredentialSpec_GithubApp:
		return s.issueGitHubApp(ctx, cred, t.GithubApp)
	case *v1alpha1.CredentialSpec_OauthAccessToken:
		return s.issueAccessToken(ctx, cred, t.OauthAccessToken)
	case *v1alpha1.CredentialSpec_OauthRefreshToken:
		return s.issueRefreshToken(ctx, cred, t.OauthRefreshToken)
	}
	return Token{}, fmt.Errorf("credential %s has no type", describe(cred))
}

// Revoke invalidates a token returned by Issue. It reports whether the token
// was revoked: some types cannot be revoked, and that is not an error.
func (s *Service) Revoke(ctx context.Context, cred *v1alpha1.Credential, tok Token) (bool, error) {
	if s == nil {
		return false, errors.New("credentials are not configured on this server")
	}
	switch t := cred.GetSpec().GetType().(type) {
	case *v1alpha1.CredentialSpec_GithubApp:
		if t.GithubApp == nil {
			return false, fmt.Errorf("credential %s: githubApp configuration is missing", describe(cred))
		}
		return s.revokeGitHubApp(ctx, tok)
	case *v1alpha1.CredentialSpec_OauthAccessToken:
		if t.OauthAccessToken == nil {
			return false, fmt.Errorf("credential %s: oauthAccessToken configuration is missing", describe(cred))
		}
		return revokeAccessToken()
	case *v1alpha1.CredentialSpec_OauthRefreshToken:
		if t.OauthRefreshToken == nil {
			return false, fmt.Errorf("credential %s: oauthRefreshToken configuration is missing", describe(cred))
		}
		return s.revokeRefreshToken(ctx, cred, t.OauthRefreshToken, tok)
	}
	return false, fmt.Errorf("credential %s has no type", describe(cred))
}

// atespaceOf returns the credential's atespace, which is also the namespace of
// the Secrets it references.
func atespaceOf(cred *v1alpha1.Credential) string {
	if a := cred.GetMetadata().GetAtespace(); a != "" {
		return a
	}
	return defaultAtespace
}

func describe(cred *v1alpha1.Credential) string {
	return atespaceOf(cred) + "/" + cred.GetMetadata().GetName()
}

// secret resolves a Secret reference in the credential's namespace. A token read
// from a file usually carries a trailing newline, so surrounding whitespace is
// dropped; a blank value is an error because it can never authenticate.
func (s *Service) secret(ctx context.Context, cred *v1alpha1.Credential, ref *v1alpha1.SecretKeyRef) (string, error) {
	if s.Secrets == nil {
		return "", errors.New("no secret resolver is configured")
	}
	v, err := s.Secrets(ctx, atespaceOf(cred), ref.GetName(), ref.GetKey())
	if err != nil {
		return "", fmt.Errorf("resolving secret %s/%s key %q: %w", atespaceOf(cred), ref.GetName(), ref.GetKey(), err)
	}
	v = strings.TrimSpace(v)
	if v == "" {
		return "", fmt.Errorf("secret %s/%s key %q is empty", atespaceOf(cred), ref.GetName(), ref.GetKey())
	}
	return v, nil
}

// client returns the HTTP client for issuer requests, with redirects disabled so
// a hostile or misconfigured endpoint cannot bounce a POST body holding a refresh
// token or client secret to another host.
func (s *Service) client() *http.Client {
	var c http.Client
	if s.HTTPClient != nil {
		c = *s.HTTPClient
	} else {
		c.Timeout = defaultHTTPTimeout
	}
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &c
}
