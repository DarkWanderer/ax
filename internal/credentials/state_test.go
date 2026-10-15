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
	"testing"

	"github.com/google/ax/internal/credentials"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/protobuf/proto"
)

func refreshSpec() *v1alpha1.CredentialSpec {
	return &v1alpha1.CredentialSpec{Type: &v1alpha1.CredentialSpec_OauthRefreshToken{OauthRefreshToken: &v1alpha1.OAuthRefreshTokenCredential{
		TokenUrl:           "https://idp.example.com/token",
		ClientId:           "client",
		ClientSecret:       &v1alpha1.SecretKeyRef{Name: "idp", Key: "client-secret"},
		Scopes:             []string{"repo"},
		RefreshTokenSecret: &v1alpha1.SecretKeyRef{Name: "idp", Key: "refresh"},
		RevocationUrl:      "https://idp.example.com/revoke",
	}}}
}

func TestStaleState(t *testing.T) {
	edit := func(f func(*v1alpha1.OAuthRefreshTokenCredential)) *v1alpha1.CredentialSpec {
		s := refreshSpec()
		f(s.GetOauthRefreshToken())
		return s
	}
	tests := []struct {
		name    string
		updated *v1alpha1.CredentialSpec
		want    bool
	}{
		// The refresh token still belongs to the same grant: keep it, because the
		// provider has already retired the seed.
		{"identical", refreshSpec(), false},
		{"scopes", edit(func(o *v1alpha1.OAuthRefreshTokenCredential) { o.Scopes = []string{"repo", "read:user"} }), false},
		{"scopes removed", edit(func(o *v1alpha1.OAuthRefreshTokenCredential) { o.Scopes = nil }), false},
		{"revocation url", edit(func(o *v1alpha1.OAuthRefreshTokenCredential) { o.RevocationUrl = "https://idp.example.com/r2" }), false},
		{"revocation url removed", edit(func(o *v1alpha1.OAuthRefreshTokenCredential) { o.RevocationUrl = "" }), false},
		{"client secret rotated", edit(func(o *v1alpha1.OAuthRefreshTokenCredential) { o.ClientSecret.Key = "client-secret-2" }), false},
		{"client secret removed", edit(func(o *v1alpha1.OAuthRefreshTokenCredential) { o.ClientSecret = nil }), false},

		// The token was issued to another endpoint, client or seed.
		{"token url", edit(func(o *v1alpha1.OAuthRefreshTokenCredential) { o.TokenUrl = "https://other.example.com/token" }), true},
		{"client id", edit(func(o *v1alpha1.OAuthRefreshTokenCredential) { o.ClientId = "other" }), true},
		{"seed secret name", edit(func(o *v1alpha1.OAuthRefreshTokenCredential) { o.RefreshTokenSecret.Name = "other" }), true},
		{"seed secret key", edit(func(o *v1alpha1.OAuthRefreshTokenCredential) { o.RefreshTokenSecret.Key = "other" }), true},
		{"seed secret removed", edit(func(o *v1alpha1.OAuthRefreshTokenCredential) { o.RefreshTokenSecret = nil }), true},
		{"changed to another type", &v1alpha1.CredentialSpec{Type: &v1alpha1.CredentialSpec_OauthAccessToken{OauthAccessToken: &v1alpha1.OAuthAccessTokenCredential{}}}, true},
		{"spec removed", nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := credentials.StaleState(refreshSpec(), tt.updated); got != tt.want {
				t.Errorf("StaleState = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStaleState_OnlyRefreshTokensHaveState(t *testing.T) {
	access := &v1alpha1.CredentialSpec{Type: &v1alpha1.CredentialSpec_OauthAccessToken{OauthAccessToken: &v1alpha1.OAuthAccessTokenCredential{
		TokenSecret: &v1alpha1.SecretKeyRef{Name: "a", Key: "b"},
	}}}
	changed := proto.Clone(access).(*v1alpha1.CredentialSpec)
	changed.GetOauthAccessToken().TokenSecret.Name = "c"
	for name, tc := range map[string]struct{ old, updated *v1alpha1.CredentialSpec }{
		"access token edited":     {access, changed},
		"new refresh credential":  {nil, refreshSpec()},
		"access token to refresh": {access, refreshSpec()},
		"nothing to nothing":      {nil, nil},
	} {
		if credentials.StaleState(tc.old, tc.updated) {
			t.Errorf("%s: nothing was stored, so nothing is stale", name)
		}
	}
}
