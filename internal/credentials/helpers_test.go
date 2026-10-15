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
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/google/ax/internal/credentials"
	"github.com/google/ax/internal/lock"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

// fakeSecrets is a SecretResolver backed by a map keyed by namespace/name/key.
// It records every lookup so tests can check which Secret was read from where.
type fakeSecrets struct {
	mu     sync.Mutex
	values map[string]string
	calls  []string
}

func secretID(namespace, name, key string) string { return namespace + "/" + name + "/" + key }

func newSecrets(kv map[string]string) *fakeSecrets { return &fakeSecrets{values: kv} }

func (f *fakeSecrets) set(namespace, name, key, value string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.values[secretID(namespace, name, key)] = value
}

func (f *fakeSecrets) Resolve(_ context.Context, namespace, name, key string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := secretID(namespace, name, key)
	f.calls = append(f.calls, id)
	v, ok := f.values[id]
	if !ok {
		return "", fmt.Errorf("secret %s not found", id)
	}
	return v, nil
}

// failingState wraps a StateStore and fails saves on demand.
type failingState struct {
	credentials.StateStore
	failSave bool
}

func (f *failingState) SaveCredentialState(ctx context.Context, atespace, name string, st store.CredentialState) error {
	if f.failSave {
		return fmt.Errorf("redis unavailable")
	}
	return f.StateStore.SaveCredentialState(ctx, atespace, name, st)
}

func newService(secrets *fakeSecrets) (*credentials.Service, *memory.MemoryStore) {
	st := memory.NewStore()
	return &credentials.Service{
		Secrets: secrets.Resolve,
		State:   st,
		Locker:  lock.NewMemoryLocker(),
	}, st
}

func accessTokenCredential() *v1alpha1.Credential {
	return &v1alpha1.Credential{
		Metadata: &v1alpha1.ObjectMeta{Name: "claude", Atespace: "team-a"},
		Spec: &v1alpha1.CredentialSpec{Type: &v1alpha1.CredentialSpec_OauthAccessToken{OauthAccessToken: &v1alpha1.OAuthAccessTokenCredential{
			TokenSecret: &v1alpha1.SecretKeyRef{Name: "claude-oauth", Key: "token"},
		}}},
	}
}

// captureLogs routes slog to a buffer at debug level for the duration of a test.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

func assertNotLogged(t *testing.T, logs *bytes.Buffer, secrets ...string) {
	t.Helper()
	for _, s := range secrets {
		if strings.Contains(logs.String(), s) {
			t.Errorf("secret %q appears in logs:\n%s", s, logs)
		}
	}
}
