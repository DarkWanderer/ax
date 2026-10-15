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
	"fmt"

	"github.com/google/ax/pkg/apis/v1alpha1"
)

// issueAccessToken returns the stored token as-is. Such a token is long-lived
// and has no expiry that AX can know.
func (s *Service) issueAccessToken(ctx context.Context, cred *v1alpha1.Credential, o *v1alpha1.OAuthAccessTokenCredential) (Token, error) {
	if o == nil {
		return Token{}, fmt.Errorf("credential %s: oauthAccessToken configuration is missing", describe(cred))
	}
	v, err := s.secret(ctx, cred, o.GetTokenSecret())
	if err != nil {
		return Token{}, err
	}
	return Token{Value: v}, nil
}

// revokeAccessToken does nothing: the token handed out is the stored one, so
// revoking it would invalidate the credential itself.
func revokeAccessToken() (bool, error) { return false, nil }
