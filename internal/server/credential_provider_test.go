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

	"github.com/google/ax/internal/lock"
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

// orderTrackingStore wraps a Store to record when GetCredentialProvider is
// called, so a test can check it happened after a lock was taken.
type orderTrackingStore struct {
	*memory.MemoryStore
	log *[]string
}

func (s *orderTrackingStore) GetCredentialProvider(ctx context.Context, atespace, name string) (*v1alpha1.CredentialProvider, error) {
	*s.log = append(*s.log, "get")
	return s.MemoryStore.GetCredentialProvider(ctx, atespace, name)
}

// orderTrackingLocker wraps a Locker to record when Lock is called.
type orderTrackingLocker struct {
	lock.Locker
	log *[]string
}

func (l *orderTrackingLocker) Lock(ctx context.Context, kind, atespace, name string) (func(), error) {
	*l.log = append(*l.log, "lock")
	return l.Locker.Lock(ctx, kind, atespace, name)
}

// TestUpdateCredentialProviderLocksBeforeReadingExisting covers the race an
// update's read-modify-write of an existing provider's metadata could lose to
// a concurrent delete: the lock must be held before defaultMetadata's lookup
// reads the store, not just before the final save, or a delete that
// interleaves between that read and the write could be undone by the update
// saving a stale read back into existence.
func TestUpdateCredentialProviderLocksBeforeReadingExisting(t *testing.T) {
	ctx := context.Background()
	var log []string
	st := &orderTrackingStore{MemoryStore: memory.NewStore(), log: &log}
	lk := &orderTrackingLocker{Locker: lock.NewMemoryLocker(), log: &log}
	s := server.NewServer(st, server.Options{Locker: lk})

	p := &v1alpha1.CredentialProvider{Metadata: &v1alpha1.ObjectMeta{Name: "github", Atespace: "team-a"}, Spec: &v1alpha1.CredentialProviderSpec{GithubApp: &v1alpha1.GitHubAppCredential{AppId: 1, InstallationId: 2, PrivateKeySecret: &v1alpha1.SecretKeyRef{Name: "key", Key: "pem"}, Repositories: []string{"one"}, Permissions: map[string]string{"contents": "read"}}}}
	// First call creates the provider; no existing one to read, but the log
	// still shows the lock preceding the (not-found) lookup.
	if _, err := s.UpdateCredentialProvider(ctx, &v1alpha1.UpdateCredentialProviderRequest{CredentialProvider: p}); err != nil {
		t.Fatal(err)
	}
	// Second call updates it with a fresh request (CreationTimestamp unset,
	// as a real client's would be), so defaultMetadata's lookup now finds a
	// real provider to read -- this is the read the lock must precede.
	log = nil
	p2 := &v1alpha1.CredentialProvider{Metadata: &v1alpha1.ObjectMeta{Name: "github", Atespace: "team-a"}, Spec: p.Spec}
	if _, err := s.UpdateCredentialProvider(ctx, &v1alpha1.UpdateCredentialProviderRequest{CredentialProvider: p2}); err != nil {
		t.Fatal(err)
	}
	if len(log) < 2 || log[0] != "lock" || log[1] != "get" {
		t.Fatalf("call order = %v, want lock before get", log)
	}
}
