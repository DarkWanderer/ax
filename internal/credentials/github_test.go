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
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/google/ax/pkg/apis/v1alpha1"
)

func TestMintAndRevokeScopedInstallationToken(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pemKey := string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
	var issued, revoked bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/app/installations/42/access_tokens":
			issued = true
			if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ey") {
				t.Error("missing app JWT")
			}
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
			_, _ = w.Write([]byte(`{"token":"ghs_secret"}`))
		case "/installation/token":
			revoked = true
			if got := r.Header.Get("Authorization"); got != "Bearer ghs_secret" {
				t.Errorf("revoke auth = %s", got)
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected endpoint %s", r.URL.Path)
		}
	}))
	defer srv.Close()
	app := &v1alpha1.GitHubAppCredential{AppId: 7, InstallationId: 42, Repositories: []string{"first", "second", "third"}, Permissions: map[string]string{"contents": "read"}}
	client := &GitHubClient{BaseURL: srv.URL, HTTPClient: srv.Client()}
	token, err := client.Mint(context.Background(), app, pemKey)
	if err != nil || token != "ghs_secret" {
		t.Fatalf("mint token=%q err=%v", token, err)
	}
	if err := client.Revoke(context.Background(), token); err != nil {
		t.Fatal(err)
	}
	if !issued || !revoked {
		t.Fatalf("issued=%t revoked=%t", issued, revoked)
	}
}
