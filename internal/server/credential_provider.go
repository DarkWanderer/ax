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

package server

import (
	"context"
	"errors"

	"github.com/google/ax/internal/store"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) GetCredentialProvider(ctx context.Context, req *v1alpha1.GetCredentialProviderRequest) (*v1alpha1.CredentialProvider, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	atespace := req.GetAtespace()
	if atespace == "" {
		atespace = "default"
	}
	p, err := s.store.GetCredentialProvider(ctx, atespace, req.GetName())
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "credential provider not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "getting credential provider: %v", err)
	}
	return p, nil
}

func (s *Server) ListCredentialProviders(ctx context.Context, req *v1alpha1.ListCredentialProvidersRequest) (*v1alpha1.ListCredentialProvidersResponse, error) {
	atespace := ""
	if req != nil {
		atespace = req.GetAtespace()
	}
	providers, err := s.store.ListCredentialProviders(ctx, atespace)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "listing credential providers: %v", err)
	}
	return &v1alpha1.ListCredentialProvidersResponse{CredentialProviders: providers}, nil
}

func (s *Server) UpdateCredentialProvider(ctx context.Context, req *v1alpha1.UpdateCredentialProviderRequest) (*v1alpha1.CredentialProvider, error) {
	if req == nil || req.GetCredentialProvider() == nil {
		return nil, status.Error(codes.InvalidArgument, "credential provider required")
	}
	p := req.CredentialProvider
	if err := v1alpha1.ValidateCredentialProvider(p); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	p.Metadata = defaultMetadata(p.Metadata, func(atespace, name string) *v1alpha1.ObjectMeta {
		old, err := s.store.GetCredentialProvider(ctx, atespace, name)
		if err != nil {
			return nil
		}
		return old.GetMetadata()
	})

	atespace := p.Metadata.Atespace
	name := p.Metadata.Name

	// Acquire exclusive lock for this credential provider, so a concurrent
	// delete can't be undone by an update racing it back into existence.
	unlock, err := s.locker.Lock(ctx, "credentialprovider", atespace, name)
	if err != nil {
		return nil, status.Errorf(codes.Aborted, "locking credential provider %s/%s: %v", atespace, name, err)
	}
	defer unlock()

	if p.ApiVersion == "" {
		p.ApiVersion = v1alpha1.APIVersion
	}
	if p.Kind == "" {
		p.Kind = v1alpha1.KindCredentialProvider
	}
	if err := s.store.SaveCredentialProvider(ctx, p); err != nil {
		return nil, status.Errorf(codes.Internal, "saving credential provider: %v", err)
	}
	return p, nil
}

func (s *Server) DeleteCredentialProvider(ctx context.Context, req *v1alpha1.DeleteCredentialProviderRequest) (*v1alpha1.DeleteCredentialProviderResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	atespace := req.GetAtespace()
	if atespace == "" {
		atespace = "default"
	}
	name := req.GetName()

	// Acquire exclusive lock for this credential provider, so a concurrent
	// update can't recreate it after this delete has already reported success.
	unlock, err := s.locker.Lock(ctx, "credentialprovider", atespace, name)
	if err != nil {
		return nil, status.Errorf(codes.Aborted, "locking credential provider %s/%s: %v", atespace, name, err)
	}
	defer unlock()

	if err := s.store.DeleteCredentialProvider(ctx, atespace, name); err != nil {
		return nil, status.Errorf(codes.Internal, "deleting credential provider: %v", err)
	}
	return &v1alpha1.DeleteCredentialProviderResponse{}, nil
}
