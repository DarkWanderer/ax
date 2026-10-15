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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/ax/internal/store"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/protobuf/proto"
)

// issueRefreshToken exchanges a refresh token for an access token.
//
// Providers that rotate refresh tokens invalidate the old one on use, so the new
// one must be stored before the access token leaves this function, and refreshes
// of one credential must never overlap. The Secret is only the seed; once the
// provider has rotated, the stored token supersedes it until the Secret changes.
func (s *Service) issueRefreshToken(ctx context.Context, cred *v1alpha1.Credential, o *v1alpha1.OAuthRefreshTokenCredential) (Token, error) {
	if o == nil {
		return Token{}, fmt.Errorf("credential %s: oauthRefreshToken configuration is missing", describe(cred))
	}
	if s.State == nil || s.Locker == nil {
		return Token{}, errors.New("oauthRefreshToken credentials need a state store and a locker, and this server has none")
	}
	atespace, name := atespaceOf(cred), cred.GetMetadata().GetName()

	unlock, err := s.Locker.Lock(ctx, TokenLockKind, atespace, name)
	if err != nil {
		return Token{}, fmt.Errorf("locking credential %s: %w", describe(cred), err)
	}
	defer unlock()

	seed, err := s.secret(ctx, cred, o.GetRefreshTokenSecret())
	if err != nil {
		return Token{}, err
	}
	digest := seedDigest(seed, o)

	refresh := seed
	switch st, err := s.State.GetCredentialState(ctx, atespace, name); {
	case err == nil:
		if st.SeedDigest == digest && st.RefreshToken != "" {
			refresh = st.RefreshToken
		}
	case errors.Is(err, store.ErrNotFound):
	default:
		return Token{}, fmt.Errorf("loading state of credential %s: %w", describe(cred), err)
	}

	clientSecret, err := s.clientSecret(ctx, cred, o)
	if err != nil {
		return Token{}, err
	}
	form := url.Values{"grant_type": {"refresh_token"}, "refresh_token": {refresh}}
	if len(o.GetScopes()) > 0 {
		form.Set("scope", strings.Join(o.GetScopes(), " "))
	}
	resp, err := s.postForm(ctx, o.GetTokenUrl(), form, o.GetClientId(), clientSecret)
	if err != nil {
		return Token{}, fmt.Errorf("requesting access token: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Token{}, tokenEndpointError("token request", resp)
	}
	var result struct {
		AccessToken  string      `json:"access_token"`
		RefreshToken string      `json:"refresh_token"`
		ExpiresIn    json.Number `json:"expires_in"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&result); err != nil {
		return Token{}, fmt.Errorf("decoding token response: %w", err)
	}
	if result.AccessToken == "" {
		return Token{}, errors.New("token response contained no access_token")
	}

	// The provider has now invalidated the token we sent. If the replacement
	// cannot be kept, handing out the access token would only postpone a
	// credential that can no longer refresh, so fail loudly instead.
	if result.RefreshToken != "" && result.RefreshToken != refresh {
		err := s.State.SaveCredentialState(ctx, atespace, name, store.CredentialState{RefreshToken: result.RefreshToken, SeedDigest: digest})
		if err != nil {
			return Token{}, fmt.Errorf("the provider rotated the refresh token of credential %s but it could not be stored, so the credential must be re-seeded: %w", describe(cred), err)
		}
		slog.Debug("stored rotated refresh token", "credential", describe(cred))
	}

	tok := Token{Value: result.AccessToken}
	if secs, err := result.ExpiresIn.Float64(); err == nil && secs > 0 {
		tok.ExpiresAt = time.Now().Add(time.Duration(secs * float64(time.Second)))
	}
	return tok, nil
}

// revokeRefreshToken revokes the access token at the RFC 7009 endpoint. Without
// one there is nothing to call.
func (s *Service) revokeRefreshToken(ctx context.Context, cred *v1alpha1.Credential, o *v1alpha1.OAuthRefreshTokenCredential, tok Token) (bool, error) {
	if o.GetRevocationUrl() == "" || tok.Value == "" {
		return false, nil
	}
	clientSecret, err := s.clientSecret(ctx, cred, o)
	if err != nil {
		return false, err
	}
	form := url.Values{"token": {tok.Value}, "token_type_hint": {"access_token"}}
	resp, err := s.postForm(ctx, o.GetRevocationUrl(), form, o.GetClientId(), clientSecret)
	if err != nil {
		return false, fmt.Errorf("revoking access token: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false, tokenEndpointError("revocation request", resp)
	}
	return true, nil
}

// clientSecret resolves the client secret, if the credential has one.
func (s *Service) clientSecret(ctx context.Context, cred *v1alpha1.Credential, o *v1alpha1.OAuthRefreshTokenCredential) (string, error) {
	if o.GetClientSecret() == nil {
		return "", nil
	}
	return s.secret(ctx, cred, o.GetClientSecret())
}

// postForm sends an OAuth endpoint request. A client with a secret authenticates
// with HTTP Basic, the method every server must support (RFC 6749 section 2.3.1);
// a public client identifies itself with client_id in the body.
func (s *Service) postForm(ctx context.Context, endpoint string, form url.Values, clientID, clientSecret string) (*http.Response, error) {
	if clientSecret == "" {
		form.Set("client_id", clientID)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	if clientSecret != "" {
		// The id and secret are form-encoded before Basic encoding.
		req.SetBasicAuth(url.QueryEscape(clientID), url.QueryEscape(clientSecret))
	}
	return s.client().Do(req)
}

// tokenEndpointError describes a failed response without echoing its body, which
// providers sometimes fill with the credentials they were sent. The OAuth error
// code (invalid_grant, ...) is a fixed vocabulary and is safe to show.
func tokenEndpointError(what string, resp *http.Response) error {
	var body struct {
		Error string `json:"error"`
	}
	_ = json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&body)
	if isOAuthErrorCode(body.Error) {
		return fmt.Errorf("%s failed (HTTP %d): %s", what, resp.StatusCode, body.Error)
	}
	return fmt.Errorf("%s failed (HTTP %d)", what, resp.StatusCode)
}

// isOAuthErrorCode reports whether s looks like an RFC 6749 error code, so that
// arbitrary provider text never reaches an error message.
func isOAuthErrorCode(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	for _, r := range s {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '_' {
			return false
		}
	}
	return true
}

// seedDigest identifies where a stored refresh token came from: the seed value
// and the client and endpoint it was issued by. Changing any of them makes the
// stored token stale, so the next refresh starts again from the seed. That is
// how an operator re-authorizes: by updating the Secret.
func seedDigest(seed string, o *v1alpha1.OAuthRefreshTokenCredential) string {
	h := sha256.New()
	for _, part := range []string{seed, o.GetTokenUrl(), o.GetClientId()} {
		// Length-prefix each part so that adjacent parts cannot run together.
		fmt.Fprintf(h, "%d:%s;", len(part), part)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// StaleState reports whether the state stored for a credential with spec old no
// longer applies once the credential is updated to spec updated.
//
// Stored state is a refresh token, which belongs to the endpoint, client and seed
// it was issued for: the same things seedDigest covers. An edit that leaves them
// alone, such as other scopes, a revocation URL or a rotated client secret, must
// keep the state, because the provider has already retired the seed and nothing
// else could refresh the credential.
func StaleState(old, updated *v1alpha1.CredentialSpec) bool {
	before := old.GetOauthRefreshToken()
	if before == nil {
		return false // only the refresh token type has state
	}
	after := updated.GetOauthRefreshToken()
	return after == nil ||
		before.GetTokenUrl() != after.GetTokenUrl() ||
		before.GetClientId() != after.GetClientId() ||
		!proto.Equal(before.GetRefreshTokenSecret(), after.GetRefreshTokenSecret())
}
