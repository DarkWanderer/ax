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

package credentials

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/ax/pkg/apis/v1alpha1"
)

// appJWT signs the short-lived JWT that authenticates as the GitHub App.
func appJWT(appID int64, privateKey string) (string, error) {
	block, _ := pem.Decode([]byte(privateKey))
	if block == nil {
		return "", errors.New("invalid GitHub App private key PEM")
	}
	var key *rsa.PrivateKey
	if parsed, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		key = parsed
	} else if parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		key, _ = parsed.(*rsa.PrivateKey)
	}
	if key == nil {
		return "", errors.New("GitHub App private key must be RSA")
	}
	// GitHub rejects an exp more than ten minutes ahead of its own clock, and iat
	// is backdated to tolerate drift between the two clocks.
	now := time.Now().Unix()
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))
	claims, _ := json.Marshal(map[string]any{"iat": now - 60, "exp": now + 540, "iss": strconv.FormatInt(appID, 10)})
	unsigned := header + "." + base64.RawURLEncoding.EncodeToString(claims)
	digest := sha256.Sum256([]byte(unsigned))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		return "", fmt.Errorf("signing GitHub App JWT: %w", err)
	}
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

func (s *Service) githubURL() string {
	if s.GitHubAPIURL != "" {
		return strings.TrimRight(s.GitHubAPIURL, "/")
	}
	return defaultGitHubAPI
}

// issueGitHubApp mints an installation token scoped to the credential's
// repositories and permissions.
func (s *Service) issueGitHubApp(ctx context.Context, cred *v1alpha1.Credential, app *v1alpha1.GitHubAppCredential) (Token, error) {
	if app == nil {
		return Token{}, fmt.Errorf("credential %s: githubApp configuration is missing", describe(cred))
	}
	privateKey, err := s.secret(ctx, cred, app.GetPrivateKeySecret())
	if err != nil {
		return Token{}, err
	}
	jwt, err := appJWT(app.GetAppId(), privateKey)
	if err != nil {
		return Token{}, err
	}
	body, err := json.Marshal(struct {
		Repositories []string          `json:"repositories"`
		Permissions  map[string]string `json:"permissions"`
	}{app.GetRepositories(), app.GetPermissions()})
	if err != nil {
		return Token{}, fmt.Errorf("encoding installation token scope: %w", err)
	}
	endpoint := fmt.Sprintf("%s/app/installations/%d/access_tokens", s.githubURL(), app.GetInstallationId())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return Token{}, err
	}
	req.Header.Set("Authorization", "Bearer "+jwt)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client().Do(req)
	if err != nil {
		return Token{}, fmt.Errorf("requesting GitHub installation token: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return Token{}, fmt.Errorf("GitHub installation token request failed (HTTP %d)", resp.StatusCode)
	}
	var result struct {
		Token     string `json:"token"`
		ExpiresAt string `json:"expires_at"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&result); err != nil {
		return Token{}, fmt.Errorf("decoding GitHub installation token response: %w", err)
	}
	if result.Token == "" {
		return Token{}, errors.New("GitHub installation token response contained no token")
	}
	tok := Token{Value: result.Token}
	// An unparsable expiry is treated as unknown rather than failing a token
	// that was in fact issued.
	if t, err := time.Parse(time.RFC3339, result.ExpiresAt); err == nil {
		tok.ExpiresAt = t
	}
	return tok, nil
}

// revokeGitHubApp deletes the installation token. A token GitHub no longer
// recognises (401, 404) counts as revoked: the caller's goal is already met.
func (s *Service) revokeGitHubApp(ctx context.Context, tok Token) (bool, error) {
	if tok.Value == "" {
		return false, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, s.githubURL()+"/installation/token", nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Authorization", "Bearer "+tok.Value)
	req.Header.Set("Accept", "application/vnd.github+json")
	resp, err := s.client().Do(req)
	if err != nil {
		return false, fmt.Errorf("revoking GitHub installation token: %w", err)
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusNoContent, http.StatusUnauthorized, http.StatusNotFound:
		return true, nil
	}
	return false, fmt.Errorf("GitHub installation token revoke failed (HTTP %d)", resp.StatusCode)
}
