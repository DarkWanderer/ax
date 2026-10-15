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

package memory_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/protobuf/proto"
)

func testCredential(atespace, name string) *v1alpha1.Credential {
	return &v1alpha1.Credential{
		Metadata: &v1alpha1.ObjectMeta{Atespace: atespace, Name: name},
		Spec: &v1alpha1.CredentialSpec{Type: &v1alpha1.CredentialSpec_OauthAccessToken{OauthAccessToken: &v1alpha1.OAuthAccessTokenCredential{
			TokenSecret: &v1alpha1.SecretKeyRef{Name: "s", Key: "k"},
		}}},
	}
}

func TestCredential_SaveGetListDelete(t *testing.T) {
	ctx := context.Background()
	s := memory.NewStore()

	if _, err := s.GetCredential(ctx, "default", "gh"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetCredential on empty store = %v, want ErrNotFound", err)
	}

	for _, c := range []*v1alpha1.Credential{testCredential("", "a"), testCredential("default", "b"), testCredential("other", "c")} {
		if err := s.SaveCredential(ctx, c); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.GetCredential(ctx, "default", "a")
	if err != nil {
		t.Fatalf("an empty atespace should default: %v", err)
	}
	if got.GetMetadata().GetAtespace() != "default" {
		t.Errorf("atespace = %q, want default", got.GetMetadata().GetAtespace())
	}

	// Mutating what a caller got back must not change the stored resource.
	got.Metadata.Name = "mutated"
	again, _ := s.GetCredential(ctx, "default", "a")
	if again.GetMetadata().GetName() != "a" {
		t.Errorf("stored credential was aliased to the returned value")
	}

	all, _ := s.ListCredentials(ctx, "")
	if len(all) != 3 {
		t.Errorf("list all = %d, want 3", len(all))
	}
	def, _ := s.ListCredentials(ctx, "default")
	if len(def) != 2 {
		t.Errorf("list default = %d, want 2", len(def))
	}
	star, _ := s.ListCredentials(ctx, "*")
	if len(star) != 3 {
		t.Errorf("list * = %d, want 3", len(star))
	}

	if err := s.DeleteCredential(ctx, "default", "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetCredential(ctx, "default", "a"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetCredential after delete = %v, want ErrNotFound", err)
	}
}

func TestCredential_SaveRequiresName(t *testing.T) {
	s := memory.NewStore()
	if err := s.SaveCredential(context.Background(), &v1alpha1.Credential{Metadata: &v1alpha1.ObjectMeta{}}); err == nil {
		t.Fatal("expected an error for a nameless credential")
	}
}

func TestCredentialState_SaveGetDelete(t *testing.T) {
	ctx := context.Background()
	s := memory.NewStore()

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
	if err := s.DeleteCredentialState(ctx, "default", "idp"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetCredentialState(ctx, "default", "idp"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetCredentialState after delete = %v, want ErrNotFound", err)
	}
	// Deleting what is not there is not an error.
	if err := s.DeleteCredentialState(ctx, "default", "idp"); err != nil {
		t.Errorf("deleting missing state: %v", err)
	}
}

func TestCredentialState_RemovedWithCredential(t *testing.T) {
	ctx := context.Background()
	s := memory.NewStore()
	c := testCredential("default", "idp")
	if err := s.SaveCredential(ctx, c); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveCredentialState(ctx, "default", "idp", store.CredentialState{RefreshToken: "rt"}); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteCredential(ctx, "default", "idp"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetCredentialState(ctx, "default", "idp"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("state survived credential deletion: %v", err)
	}
}

func TestCredentialState_NotPartOfCredential(t *testing.T) {
	ctx := context.Background()
	s := memory.NewStore()
	c := testCredential("default", "idp")
	if err := s.SaveCredential(ctx, c); err != nil {
		t.Fatal(err)
	}
	before, _ := s.GetCredential(ctx, "default", "idp")
	if err := s.SaveCredentialState(ctx, "default", "idp", store.CredentialState{RefreshToken: "super-secret-rt"}); err != nil {
		t.Fatal(err)
	}
	after, _ := s.GetCredential(ctx, "default", "idp")
	if !proto.Equal(before, after) {
		t.Errorf("saving state changed the credential resource")
	}
}
