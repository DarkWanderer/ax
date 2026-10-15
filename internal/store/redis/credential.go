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

package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/ax/internal/store"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"
)

func (s *Store) credentialKey(atespace, name string) string {
	return fmt.Sprintf("%s:credential:%s:%s", s.opts.KeyPrefix, atespace, name)
}

func (s *Store) credentialIndexKey() string {
	return fmt.Sprintf("%s:credentials:index", s.opts.KeyPrefix)
}

func (s *Store) credentialAtespaceIndexKey(atespace string) string {
	return fmt.Sprintf("%s:credentials:atespace:%s", s.opts.KeyPrefix, atespace)
}

// credentialStateKey is deliberately outside the credential: and credentials:
// key families so that no listing of credentials can reach it.
func (s *Store) credentialStateKey(atespace, name string) string {
	return fmt.Sprintf("%s:credential-state:%s:%s", s.opts.KeyPrefix, atespace, name)
}

// SaveCredential stores a credential.
func (s *Store) SaveCredential(ctx context.Context, cred *v1alpha1.Credential) error {
	if cred.Metadata == nil {
		cred.Metadata = &v1alpha1.ObjectMeta{}
	}
	if cred.Metadata.Name == "" {
		return errors.New("credential name is required")
	}
	if cred.Metadata.Atespace == "" {
		cred.Metadata.Atespace = "default"
	}
	if cred.ApiVersion == "" {
		cred.ApiVersion = v1alpha1.APIVersion
	}
	if cred.Kind == "" {
		cred.Kind = v1alpha1.KindCredential
	}

	data, err := protojson.Marshal(cred)
	if err != nil {
		return fmt.Errorf("marshaling credential: %w", err)
	}

	atespace := cred.Metadata.Atespace
	name := cred.Metadata.Name
	score := float64(time.Now().UnixNano())
	member := fmt.Sprintf("%s:%s", atespace, name)

	pipe := s.client.TxPipeline()
	pipe.Set(ctx, s.credentialKey(atespace, name), data, 0)
	pipe.ZAdd(ctx, s.credentialIndexKey(), redis.Z{Score: score, Member: member})
	pipe.ZAdd(ctx, s.credentialAtespaceIndexKey(atespace), redis.Z{Score: score, Member: name})
	_, err = pipe.Exec(ctx)
	return err
}

// DeleteCredential removes a credential together with its state.
func (s *Store) DeleteCredential(ctx context.Context, atespace, name string) error {
	if atespace == "" {
		atespace = "default"
	}
	member := fmt.Sprintf("%s:%s", atespace, name)

	pipe := s.client.TxPipeline()
	pipe.Del(ctx, s.credentialKey(atespace, name))
	pipe.Del(ctx, s.credentialStateKey(atespace, name))
	pipe.ZRem(ctx, s.credentialIndexKey(), member)
	pipe.ZRem(ctx, s.credentialAtespaceIndexKey(atespace), name)
	_, err := pipe.Exec(ctx)
	return err
}

// ListCredentials lists credentials for an atespace or across all atespaces.
func (s *Store) ListCredentials(ctx context.Context, atespace string) ([]*v1alpha1.Credential, error) {
	var members []string
	var err error

	if atespace == "" || atespace == "*" {
		members, err = s.client.ZRevRange(ctx, s.credentialIndexKey(), 0, -1).Result()
	} else {
		names, nErr := s.client.ZRevRange(ctx, s.credentialAtespaceIndexKey(atespace), 0, -1).Result()
		if nErr == nil {
			for _, n := range names {
				members = append(members, fmt.Sprintf("%s:%s", atespace, n))
			}
		}
		err = nErr
	}

	if err != nil {
		return nil, fmt.Errorf("listing credential index: %w", err)
	}
	if len(members) == 0 {
		return []*v1alpha1.Credential{}, nil
	}

	keys := make([]string, len(members))
	for i, m := range members {
		parts := strings.SplitN(m, ":", 2)
		if len(parts) == 2 {
			keys[i] = s.credentialKey(parts[0], parts[1])
		} else {
			keys[i] = s.credentialKey("default", m)
		}
	}

	vals, err := s.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("batch fetching credentials: %w", err)
	}

	creds := make([]*v1alpha1.Credential, 0, len(vals))
	for _, v := range vals {
		str, ok := v.(string)
		if !ok {
			continue
		}
		var c v1alpha1.Credential
		if err := jsonUnmarshalOpts.Unmarshal([]byte(str), &c); err == nil {
			creds = append(creds, &c)
		}
	}
	return creds, nil
}

// GetCredential retrieves a credential by atespace and name.
func (s *Store) GetCredential(ctx context.Context, atespace, name string) (*v1alpha1.Credential, error) {
	if atespace == "" {
		atespace = "default"
	}
	val, err := s.client.Get(ctx, s.credentialKey(atespace, name)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting credential from redis: %w", err)
	}

	var c v1alpha1.Credential
	if err := jsonUnmarshalOpts.Unmarshal([]byte(val), &c); err != nil {
		return nil, fmt.Errorf("unmarshaling credential: %w", err)
	}
	return &c, nil
}

// credentialStateRecord is the stored form of store.CredentialState.
type credentialStateRecord struct {
	RefreshToken string `json:"refreshToken"`
	SeedDigest   string `json:"seedDigest"`
}

// GetCredentialState retrieves a credential's state, or store.ErrNotFound.
func (s *Store) GetCredentialState(ctx context.Context, atespace, name string) (*store.CredentialState, error) {
	if atespace == "" {
		atespace = "default"
	}
	val, err := s.client.Get(ctx, s.credentialStateKey(atespace, name)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting credential state from redis: %w", err)
	}
	var rec credentialStateRecord
	if err := json.Unmarshal([]byte(val), &rec); err != nil {
		return nil, fmt.Errorf("unmarshaling credential state: %w", err)
	}
	return &store.CredentialState{RefreshToken: rec.RefreshToken, SeedDigest: rec.SeedDigest}, nil
}

// SaveCredentialState stores a credential's state. It has no expiry: a rotated
// refresh token must outlive the process for as long as the credential exists.
func (s *Store) SaveCredentialState(ctx context.Context, atespace, name string, state store.CredentialState) error {
	if atespace == "" {
		atespace = "default"
	}
	data, err := json.Marshal(credentialStateRecord{RefreshToken: state.RefreshToken, SeedDigest: state.SeedDigest})
	if err != nil {
		return fmt.Errorf("marshaling credential state: %w", err)
	}
	return s.client.Set(ctx, s.credentialStateKey(atespace, name), data, 0).Err()
}

// DeleteCredentialState removes a credential's state. Missing state is not an error.
func (s *Store) DeleteCredentialState(ctx context.Context, atespace, name string) error {
	if atespace == "" {
		atespace = "default"
	}
	return s.client.Del(ctx, s.credentialStateKey(atespace, name)).Err()
}
