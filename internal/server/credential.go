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
	"log/slog"
	"time"

	"github.com/google/ax/internal/credentials"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// testCredentialTimeout bounds obtaining the test token.
	testCredentialTimeout = 30 * time.Second
	// testRevokeTimeout bounds revoking it. Revocation runs on a context that
	// outlives the request's, so a client that gives up cannot leave the token valid.
	testRevokeTimeout = 10 * time.Second
)

// --- Credentials ---

func (s *Server) GetCredential(ctx context.Context, req *v1alpha1.GetCredentialRequest) (*v1alpha1.Credential, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	atespace := req.Atespace
	if atespace == "" {
		atespace = "default"
	}
	cred, err := s.store.GetCredential(ctx, atespace, req.Name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "credential %q not found in atespace %q", req.Name, atespace)
		}
		return nil, status.Errorf(codes.Internal, "getting credential: %v", err)
	}
	return cred, nil
}

func (s *Server) ListCredentials(ctx context.Context, req *v1alpha1.ListCredentialsRequest) (*v1alpha1.ListCredentialsResponse, error) {
	atespace := ""
	if req != nil {
		atespace = req.Atespace
	}
	creds, err := s.store.ListCredentials(ctx, atespace)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "listing credentials: %v", err)
	}
	return &v1alpha1.ListCredentialsResponse{Credentials: creds}, nil
}

func (s *Server) UpdateCredential(ctx context.Context, req *v1alpha1.UpdateCredentialRequest) (*v1alpha1.Credential, error) {
	if req == nil || req.Credential == nil {
		return nil, status.Error(codes.InvalidArgument, "credential required")
	}
	if err := v1alpha1.ValidateCredential(req.Credential); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	req.Credential.Metadata = defaultMetadata(req.Credential.Metadata, func(atespace, name string) *v1alpha1.ObjectMeta {
		existing, err := s.store.GetCredential(ctx, atespace, name)
		if err != nil {
			return nil
		}
		return existing.GetMetadata()
	})

	atespace := req.Credential.Metadata.Atespace
	name := req.Credential.Metadata.Name

	// Acquire exclusive lock for this credential
	unlock, err := s.locker.Lock(ctx, "credential", atespace, name)
	if err != nil {
		return nil, status.Errorf(codes.Aborted, "locking credential %s/%s: %v", atespace, name, err)
	}
	defer unlock()

	// Stored state is a rotated refresh token, which only fits the endpoint,
	// client and seed it was issued for. When an update changes those, drop it so
	// the next refresh starts from the seed instead of presenting the old token to
	// the new endpoint. Any other edit keeps it: the provider has already retired
	// the seed.
	existing, err := s.store.GetCredential(ctx, atespace, name)
	switch {
	case err == nil:
		if credentials.StaleState(existing.GetSpec(), req.Credential.GetSpec()) {
			if err := s.store.DeleteCredentialState(ctx, atespace, name); err != nil {
				return nil, status.Errorf(codes.Internal, "dropping credential state: %v", err)
			}
		}
	case !errors.Is(err, store.ErrNotFound):
		return nil, status.Errorf(codes.Internal, "getting existing credential: %v", err)
	}

	if err := s.store.SaveCredential(ctx, req.Credential); err != nil {
		return nil, status.Errorf(codes.Internal, "saving credential: %v", err)
	}
	return req.Credential, nil
}

func (s *Server) DeleteCredential(ctx context.Context, req *v1alpha1.DeleteCredentialRequest) (*v1alpha1.DeleteCredentialResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	atespace := req.Atespace
	if atespace == "" {
		atespace = "default"
	}
	name := req.Name

	// Acquire exclusive lock for this credential
	unlock, err := s.locker.Lock(ctx, "credential", atespace, name)
	if err != nil {
		return nil, status.Errorf(codes.Aborted, "locking credential %s/%s: %v", atespace, name, err)
	}
	defer unlock()

	if err := s.store.DeleteCredential(ctx, atespace, name); err != nil {
		return nil, status.Errorf(codes.Internal, "deleting credential: %v", err)
	}
	return &v1alpha1.DeleteCredentialResponse{}, nil
}

// TestCredential obtains a token the way a task would, reports it masked, and
// revokes it again where the issuer allows. The raw token stays on the server.
func (s *Server) TestCredential(ctx context.Context, req *v1alpha1.TestCredentialRequest) (*v1alpha1.TestCredentialResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	if s.credentials == nil {
		return nil, status.Error(codes.FailedPrecondition, "credentials are not configured on this server")
	}
	atespace := req.Atespace
	if atespace == "" {
		atespace = "default"
	}
	cred, err := s.store.GetCredential(ctx, atespace, req.Name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "credential %q not found in atespace %q", req.Name, atespace)
		}
		return nil, status.Errorf(codes.Internal, "getting credential: %v", err)
	}

	issueCtx, cancel := context.WithTimeout(ctx, testCredentialTimeout)
	defer cancel()
	tok, err := s.credentials.Issue(issueCtx, cred)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "issuing a token for credential %q: %v", req.Name, err)
	}

	revokeCtx, cancelRevoke := context.WithTimeout(context.WithoutCancel(ctx), testRevokeTimeout)
	defer cancelRevoke()
	revoked, err := s.credentials.Revoke(revokeCtx, cred, tok)
	if err != nil {
		// The test itself succeeded; the response reports revoked=false and the
		// token lapses on its own for the types that expire.
		slog.Warn("could not revoke the token issued by a credential test", "atespace", atespace, "credential", req.Name, "error", err)
		revoked = false
	}

	resp := &v1alpha1.TestCredentialResponse{
		Type:        v1alpha1.CredentialType(cred.GetSpec()),
		MaskedToken: credentials.Mask(tok.Value),
		Revoked:     revoked,
	}
	if !tok.ExpiresAt.IsZero() {
		resp.ExpireTime = timestamppb.New(tok.ExpiresAt)
	}
	return resp, nil
}
