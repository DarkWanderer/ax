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

package redis_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/ax/internal/store"
	axredis "github.com/google/ax/internal/store/redis"
	"github.com/google/ax/pkg/apis/v1alpha1"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

func newStore(t *testing.T, opts axredis.Options) (*axredis.Store, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })
	return axredis.NewStore(client, opts), mr
}

func credentialOf(name, atespace string, spec *v1alpha1.CredentialSpec) *v1alpha1.Credential {
	return &v1alpha1.Credential{Metadata: &v1alpha1.ObjectMeta{Name: name, Atespace: atespace}, Spec: spec}
}

func accessSpec() *v1alpha1.CredentialSpec {
	return &v1alpha1.CredentialSpec{Type: &v1alpha1.CredentialSpec_OauthAccessToken{OauthAccessToken: &v1alpha1.OAuthAccessTokenCredential{
		TokenSecret: &v1alpha1.SecretKeyRef{Name: "s", Key: "k"},
	}}}
}

func TestCredential_RoundTripsEveryType(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t, axredis.Options{})
	for name, spec := range map[string]*v1alpha1.CredentialSpec{
		"github": {Type: &v1alpha1.CredentialSpec_GithubApp{GithubApp: &v1alpha1.GitHubAppCredential{
			AppId: 1, InstallationId: 2,
			PrivateKeySecret: &v1alpha1.SecretKeyRef{Name: "k", Key: "pem"},
			Repositories:     []string{"a", "b"},
			Permissions:      map[string]string{"contents": "read", "issues": "write"},
		}}},
		"access": accessSpec(),
		"refresh": {Type: &v1alpha1.CredentialSpec_OauthRefreshToken{OauthRefreshToken: &v1alpha1.OAuthRefreshTokenCredential{
			TokenUrl: "https://idp/token", ClientId: "c", Scopes: []string{"x", "y"},
			ClientSecret:       &v1alpha1.SecretKeyRef{Name: "s", Key: "cs"},
			RefreshTokenSecret: &v1alpha1.SecretKeyRef{Name: "s", Key: "rt"},
			RevocationUrl:      "https://idp/revoke",
		}}},
	} {
		if err := s.SaveCredential(ctx, credentialOf(name, "team", spec)); err != nil {
			t.Fatalf("save %s: %v", name, err)
		}
		got, err := s.GetCredential(ctx, "team", name)
		if err != nil {
			t.Fatalf("get %s: %v", name, err)
		}
		if !proto.Equal(got.GetSpec(), spec) {
			t.Errorf("%s: spec changed in storage:\n got %v\nwant %v", name, got.GetSpec(), spec)
		}
	}
}

func TestCredential_SaveDefaults(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t, axredis.Options{})
	if err := s.SaveCredential(ctx, credentialOf("c", "", accessSpec())); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetCredential(ctx, "", "c")
	if err != nil {
		t.Fatal(err)
	}
	if got.GetMetadata().GetAtespace() != "default" || got.GetApiVersion() != v1alpha1.APIVersion || got.GetKind() != v1alpha1.KindCredential {
		t.Errorf("defaults not applied: %v", got)
	}
	if err := s.SaveCredential(ctx, &v1alpha1.Credential{}); err == nil {
		t.Error("expected an error for a nameless credential")
	}
}

func TestCredential_NotFound(t *testing.T) {
	s, _ := newStore(t, axredis.Options{})
	if _, err := s.GetCredential(context.Background(), "default", "nope"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetCredential = %v, want ErrNotFound", err)
	}
}

func names(creds []*v1alpha1.Credential) map[string]bool {
	out := map[string]bool{}
	for _, c := range creds {
		out[c.GetMetadata().GetAtespace()+"/"+c.GetMetadata().GetName()] = true
	}
	return out
}

func TestCredential_List(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t, axredis.Options{})
	if got, err := s.ListCredentials(ctx, ""); err != nil || got == nil || len(got) != 0 {
		t.Fatalf("empty list = %v, %v; want a non-nil empty slice", got, err)
	}
	for _, c := range []struct{ ns, name string }{{"a", "one"}, {"a", "two"}, {"b", "one"}} {
		if err := s.SaveCredential(ctx, credentialOf(c.name, c.ns, accessSpec())); err != nil {
			t.Fatal(err)
		}
	}

	for _, tc := range []struct {
		atespace string
		want     []string
	}{
		{"", []string{"a/one", "a/two", "b/one"}},
		{"*", []string{"a/one", "a/two", "b/one"}},
		{"a", []string{"a/one", "a/two"}},
		{"b", []string{"b/one"}},
		{"nowhere", nil},
	} {
		got, err := s.ListCredentials(ctx, tc.atespace)
		if err != nil {
			t.Fatalf("list %q: %v", tc.atespace, err)
		}
		gotNames := names(got)
		if len(gotNames) != len(tc.want) {
			t.Errorf("list %q = %v, want %v", tc.atespace, gotNames, tc.want)
		}
		for _, w := range tc.want {
			if !gotNames[w] {
				t.Errorf("list %q = %v, missing %s", tc.atespace, gotNames, w)
			}
		}
	}
}

func TestCredential_ListSkipsIndexEntriesWithoutARecord(t *testing.T) {
	ctx := context.Background()
	s, mr := newStore(t, axredis.Options{})
	for _, n := range []string{"keep", "lost"} {
		if err := s.SaveCredential(ctx, credentialOf(n, "default", accessSpec())); err != nil {
			t.Fatal(err)
		}
	}
	mr.Del("ax:credential:default:lost")

	got, err := s.ListCredentials(ctx, "default")
	if err != nil {
		t.Fatal(err)
	}
	if n := names(got); len(n) != 1 || !n["default/keep"] {
		t.Errorf("list = %v, want only default/keep", n)
	}
}

func TestCredential_DeleteRemovesRecordIndexesAndState(t *testing.T) {
	ctx := context.Background()
	s, mr := newStore(t, axredis.Options{})
	for _, n := range []string{"gone", "kept"} {
		if err := s.SaveCredential(ctx, credentialOf(n, "default", accessSpec())); err != nil {
			t.Fatal(err)
		}
		if err := s.SaveCredentialState(ctx, "default", n, store.CredentialState{RefreshToken: "rt-" + n, SeedDigest: "d"}); err != nil {
			t.Fatal(err)
		}
	}

	if err := s.DeleteCredential(ctx, "", "gone"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetCredential(ctx, "default", "gone"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("record survived: %v", err)
	}
	if _, err := s.GetCredentialState(ctx, "default", "gone"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("state survived: %v", err)
	}
	for _, key := range mr.Keys() {
		if strings.Contains(key, "gone") {
			t.Errorf("key %q survived", key)
		}
	}
	if members, _ := mr.ZMembers("ax:credentials:index"); len(members) != 1 || members[0] != "default:kept" {
		t.Errorf("global index = %v", members)
	}
	if members, _ := mr.ZMembers("ax:credentials:atespace:default"); len(members) != 1 || members[0] != "kept" {
		t.Errorf("atespace index = %v", members)
	}
	// The neighbour is untouched.
	if _, err := s.GetCredential(ctx, "default", "kept"); err != nil {
		t.Errorf("kept credential: %v", err)
	}
	if st, err := s.GetCredentialState(ctx, "default", "kept"); err != nil || st.RefreshToken != "rt-kept" {
		t.Errorf("kept state = %v, %v", st, err)
	}
	// Deleting what is not there is not an error.
	if err := s.DeleteCredential(ctx, "default", "gone"); err != nil {
		t.Errorf("second delete: %v", err)
	}
}

func TestCredentialState_SaveGetDelete(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t, axredis.Options{})

	if _, err := s.GetCredentialState(ctx, "default", "idp"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetCredentialState on empty store = %v, want ErrNotFound", err)
	}
	want := store.CredentialState{RefreshToken: "rt-2", SeedDigest: "abc"}
	if err := s.SaveCredentialState(ctx, "", "idp", want); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetCredentialState(ctx, "default", "idp")
	if err != nil || *got != want {
		t.Fatalf("GetCredentialState = %+v, %v; want %+v", got, err, want)
	}

	// Saving again replaces, which is how each rotation lands.
	next := store.CredentialState{RefreshToken: "rt-3", SeedDigest: "abc"}
	if err := s.SaveCredentialState(ctx, "default", "idp", next); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.GetCredentialState(ctx, "default", "idp"); *got != next {
		t.Errorf("state after second save = %+v, want %+v", got, next)
	}

	if err := s.DeleteCredentialState(ctx, "", "idp"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetCredentialState(ctx, "default", "idp"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("state after delete = %v, want ErrNotFound", err)
	}
	if err := s.DeleteCredentialState(ctx, "default", "idp"); err != nil {
		t.Errorf("deleting missing state: %v", err)
	}
}

func TestCredentialState_NeverExpiresAndStaysOutOfTheCredentialKeys(t *testing.T) {
	ctx := context.Background()
	s, mr := newStore(t, axredis.Options{})
	if err := s.SaveCredential(ctx, credentialOf("idp", "default", accessSpec())); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveCredentialState(ctx, "default", "idp", store.CredentialState{RefreshToken: "super-secret-rt", SeedDigest: "d"}); err != nil {
		t.Fatal(err)
	}

	// A rotated refresh token that expired would strand the credential.
	if ttl := mr.TTL("ax:credential-state:default:idp"); ttl != 0 {
		t.Errorf("state TTL = %v, want none", ttl)
	}
	mr.FastForward(365 * 24 * time.Hour)
	if _, err := s.GetCredentialState(ctx, "default", "idp"); err != nil {
		t.Errorf("state expired: %v", err)
	}

	// The token is not part of what any credential read returns.
	got, err := s.GetCredential(ctx, "default", "idp")
	if err != nil {
		t.Fatal(err)
	}
	list, _ := s.ListCredentials(ctx, "")
	for _, m := range append(list, got) {
		if strings.Contains(m.String(), "super-secret-rt") {
			t.Errorf("state leaked into %v", m)
		}
	}
	// And it does not live under a key the credential listings scan.
	for _, key := range mr.Keys() {
		if strings.Contains(key, "super-secret-rt") {
			t.Errorf("key name %q holds the token", key)
		}
		if strings.HasPrefix(key, "ax:credential:") || strings.HasPrefix(key, "ax:credentials:") {
			if v, _ := mr.Get(key); strings.Contains(v, "super-secret-rt") {
				t.Errorf("key %q holds the token", key)
			}
		}
	}
}

func TestCredentialState_SurvivesANewStore(t *testing.T) {
	// ax-server replicas and restarts share state through Redis alone.
	ctx := context.Background()
	mr := miniredis.RunT(t)
	open := func() *axredis.Store {
		c := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { c.Close() })
		return axredis.NewStore(c, axredis.Options{})
	}
	want := store.CredentialState{RefreshToken: "rt", SeedDigest: "d"}
	if err := open().SaveCredentialState(ctx, "default", "idp", want); err != nil {
		t.Fatal(err)
	}
	if got, err := open().GetCredentialState(ctx, "default", "idp"); err != nil || *got != want {
		t.Errorf("state through a second store = %v, %v", got, err)
	}
}

func TestCredential_KeyPrefixIsolatesStores(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })
	a := axredis.NewStore(client, axredis.Options{KeyPrefix: "a"})
	b := axredis.NewStore(client, axredis.Options{KeyPrefix: "b"})

	if err := a.SaveCredential(ctx, credentialOf("c", "default", accessSpec())); err != nil {
		t.Fatal(err)
	}
	if err := a.SaveCredentialState(ctx, "default", "c", store.CredentialState{RefreshToken: "rt"}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.GetCredential(ctx, "default", "c"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("credential visible under another prefix: %v", err)
	}
	if _, err := b.GetCredentialState(ctx, "default", "c"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("state visible under another prefix: %v", err)
	}
	if got, _ := b.ListCredentials(ctx, ""); len(got) != 0 {
		t.Errorf("list under another prefix = %v", got)
	}
	if !mr.Exists("a:credential:default:c") || !mr.Exists("a:credential-state:default:c") {
		t.Errorf("keys = %v, want the a: prefix", mr.Keys())
	}
}
