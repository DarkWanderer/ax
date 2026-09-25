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

package memory

import (
	"context"
	"errors"

	"github.com/google/ax/internal/store"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

func (s *MemoryStore) SaveCredentialProvider(_ context.Context, p *v1alpha1.CredentialProvider) error {
	if p.GetMetadata().GetName() == "" {
		return errors.New("credential provider name is required")
	}
	if p.Metadata.Atespace == "" {
		p.Metadata.Atespace = "default"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.credentialProviders[taskKey(p.Metadata.Atespace, p.Metadata.Name)] = clone(p)
	return nil
}

func (s *MemoryStore) GetCredentialProvider(_ context.Context, atespace, name string) (*v1alpha1.CredentialProvider, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.credentialProviders[taskKey(atespace, name)]
	if !ok {
		return nil, store.ErrNotFound
	}
	return clone(p), nil
}

func (s *MemoryStore) ListCredentialProviders(_ context.Context, atespace string) ([]*v1alpha1.CredentialProvider, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*v1alpha1.CredentialProvider
	for _, p := range s.credentialProviders {
		if atespace == "" || atespace == "*" || p.Metadata.Atespace == atespace {
			out = append(out, clone(p))
		}
	}
	return out, nil
}

func (s *MemoryStore) DeleteCredentialProvider(_ context.Context, atespace, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.credentialProviders, taskKey(atespace, name))
	return nil
}
