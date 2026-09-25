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

// Package credentials issues and revokes GitHub App installation tokens.
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
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/ax/pkg/apis/v1alpha1"
)

type GitHubClient struct {
	BaseURL    string
	HTTPClient *http.Client
}

func (c *GitHubClient) client() *http.Client {
	if c != nil && c.HTTPClient != nil {
		return c.HTTPClient
	}
	return &http.Client{Timeout: 15 * time.Second}
}

func (c *GitHubClient) baseURL() string {
	if c != nil && c.BaseURL != "" {
		return strings.TrimRight(c.BaseURL, "/")
	}
	return "https://api.github.com"
}

func appJWT(appID int64, privateKey string) (string, error) {
	block, _ := pem.Decode([]byte(privateKey))
	if block == nil {
		return "", fmt.Errorf("invalid GitHub App private key PEM")
	}
	var key *rsa.PrivateKey
	if parsed, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		key = parsed
	} else if parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		key, _ = parsed.(*rsa.PrivateKey)
	}
	if key == nil {
		return "", fmt.Errorf("GitHub App private key must be RSA")
	}
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

func (c *GitHubClient) Mint(ctx context.Context, app *v1alpha1.GitHubAppCredential, privateKey string) (string, error) {
	if app == nil {
		return "", fmt.Errorf("GitHub App configuration is missing")
	}
	jwt, err := appJWT(app.GetAppId(), privateKey)
	if err != nil {
		return "", err
	}
	body, err := json.Marshal(struct {
		Repositories []string          `json:"repositories"`
		Permissions  map[string]string `json:"permissions"`
	}{app.GetRepositories(), app.GetPermissions()})
	if err != nil {
		return "", fmt.Errorf("encoding installation token scope: %w", err)
	}
	url := fmt.Sprintf("%s/app/installations/%d/access_tokens", c.baseURL(), app.GetInstallationId())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+jwt)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client().Do(req)
	if err != nil {
		return "", fmt.Errorf("requesting GitHub installation token: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GitHub installation token request failed (HTTP %d)", resp.StatusCode)
	}
	var result struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&result); err != nil {
		return "", fmt.Errorf("decoding GitHub installation token response: %w", err)
	}
	if result.Token == "" {
		return "", fmt.Errorf("GitHub installation token response contained no token")
	}
	return result.Token, nil
}

func (c *GitHubClient) Revoke(ctx context.Context, token string) error {
	if token == "" {
		return nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.baseURL()+"/installation/token", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/vnd.github+json")
	resp, err := c.client().Do(req)
	if err != nil {
		return fmt.Errorf("revoking GitHub installation token: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusUnauthorized && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("GitHub installation token revoke failed (HTTP %d)", resp.StatusCode)
	}
	return nil
}
