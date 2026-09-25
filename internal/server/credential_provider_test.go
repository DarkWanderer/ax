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

package server_test

import (
	"context"
	"testing"

	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCredentialProviderTaskScope(t *testing.T) {
	ctx := context.Background()
	s := server.NewServer(memory.NewStore())
	p := &v1alpha1.CredentialProvider{Metadata: &v1alpha1.ObjectMeta{Name: "github", Atespace: "team-a"}, Spec: &v1alpha1.CredentialProviderSpec{GithubApp: &v1alpha1.GitHubAppCredential{AppId: 1, InstallationId: 2, PrivateKeySecret: &v1alpha1.SecretKeyRef{Name: "key", Key: "pem"}, Repositories: []string{"one"}, Permissions: map[string]string{"contents": "read"}}}}
	if _, err := s.UpdateCredentialProvider(ctx, &v1alpha1.UpdateCredentialProviderRequest{CredentialProvider: p}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		atespace string
		want     codes.Code
	}{{"team-a", codes.OK}, {"team-b", codes.InvalidArgument}} {
		task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "job", Atespace: tc.atespace}, Spec: &v1alpha1.TaskSpec{CredentialProvider: &v1alpha1.CredentialProviderRef{Name: "github"}}}
		_, err := s.CreateTask(ctx, &v1alpha1.CreateTaskRequest{Task: task})
		if status.Code(err) != tc.want {
			t.Errorf("atespace %s: %v", tc.atespace, err)
		}
	}
	list, err := s.ListCredentialProviders(ctx, &v1alpha1.ListCredentialProvidersRequest{Atespace: "team-a"})
	if err != nil || len(list.GetCredentialProviders()) != 1 {
		t.Fatalf("list: %v %v", list, err)
	}
	if _, err := s.DeleteCredentialProvider(ctx, &v1alpha1.DeleteCredentialProviderRequest{Atespace: "team-a", Name: "github"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetCredentialProvider(ctx, &v1alpha1.GetCredentialProviderRequest{Atespace: "team-a", Name: "github"}); status.Code(err) != codes.NotFound {
		t.Fatalf("get after delete: %v", err)
	}
}
