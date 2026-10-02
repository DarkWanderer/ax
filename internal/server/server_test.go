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
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestServerHealthzHTTP(t *testing.T) {
	memStore := memory.NewStore()
	srv := server.NewServer(memStore)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from /healthz, got %d", rec.Code)
	}
	if rec.Body.String() != "ok\n" {
		t.Fatalf("expected 'ok\\n' from /healthz, got %q", rec.Body.String())
	}

	// Verify non-healthz HTTP returns 404
	req404 := httptest.NewRequest(http.MethodGet, "/api/v1/tasks", nil)
	rec404 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec404, req404)
	if rec404.Code != http.StatusNotFound {
		t.Fatalf("expected 404 Not Found for /api/v1/tasks, got %d", rec404.Code)
	}
}

func TestServerGRPC(t *testing.T) {
	memStore := memory.NewStore()
	srv := server.NewServer(memStore)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer ln.Close()

	httpServer := &http.Server{
		Handler: srv.Handler(),
	}
	httpServer.Protocols = new(http.Protocols)
	httpServer.Protocols.SetHTTP1(true)
	httpServer.Protocols.SetUnencryptedHTTP2(true)

	go func() {
		_ = httpServer.Serve(ln)
	}()
	defer httpServer.Close()

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial gRPC: %v", err)
	}
	defer conn.Close()

	client := v1alpha1.NewAXClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Create one resource of each kind through the typed RPCs. Metadata is left
	// partially empty to exercise server-side defaulting.
	if _, err := client.UpdateWorkspace(ctx, &v1alpha1.UpdateWorkspaceRequest{Workspace: &v1alpha1.Workspace{
		Metadata: &v1alpha1.ObjectMeta{Name: "grpc-ws"},
		Spec:     &v1alpha1.WorkspaceSpec{},
	}}); err != nil {
		t.Fatalf("UpdateWorkspace failed: %v", err)
	}
	if _, err := client.UpdateModel(ctx, &v1alpha1.UpdateModelRequest{Model: &v1alpha1.Model{
		Metadata: &v1alpha1.ObjectMeta{Name: "grpc-model"},
		Spec:     &v1alpha1.ModelSpec{Provider: "google", Model: "gemini-3.8-flash"},
	}}); err != nil {
		t.Fatalf("UpdateModel failed: %v", err)
	}
	provider := &v1alpha1.CredentialProvider{Metadata: &v1alpha1.ObjectMeta{Name: "grpc-github"}, Spec: &v1alpha1.CredentialProviderSpec{GithubApp: &v1alpha1.GitHubAppCredential{AppId: 1, InstallationId: 2, PrivateKeySecret: &v1alpha1.SecretKeyRef{Name: "key", Key: "pem"}, Repositories: []string{"one", "two"}, Permissions: map[string]string{"contents": "read"}}}}
	if _, err := client.UpdateCredentialProvider(ctx, &v1alpha1.UpdateCredentialProviderRequest{CredentialProvider: provider}); err != nil {
		t.Fatalf("UpdateCredentialProvider failed: %v", err)
	}
	gotProvider, err := client.GetCredentialProvider(ctx, &v1alpha1.GetCredentialProviderRequest{Atespace: "default", Name: "grpc-github"})
	if err != nil || len(gotProvider.GetSpec().GetGithubApp().GetRepositories()) != 2 {
		t.Fatalf("GetCredentialProvider: %v %v", gotProvider, err)
	}
	providers, err := client.ListCredentialProviders(ctx, &v1alpha1.ListCredentialProvidersRequest{Atespace: "default"})
	if err != nil || len(providers.GetCredentialProviders()) != 1 {
		t.Fatalf("ListCredentialProviders: %v %v", providers, err)
	}
	if _, err := client.CreateTask(ctx, &v1alpha1.CreateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "grpc-task"},
		Spec:     &v1alpha1.TaskSpec{Image: "alpine"},
	}}); err != nil {
		t.Fatalf("CreateTask failed: %v", err)
	}

	// 2. Defaulting applies to every kind: atespace and creation timestamp are filled in.
	ws, err := client.GetWorkspace(ctx, &v1alpha1.GetWorkspaceRequest{Atespace: "default", Name: "grpc-ws"})
	if err != nil {
		t.Fatalf("GetWorkspace failed: %v", err)
	}
	if ws.GetMetadata().GetAtespace() != "default" || ws.GetMetadata().GetCreationTimestamp() == nil {
		t.Errorf("expected workspace metadata to be defaulted, got %v", ws.GetMetadata())
	}

	// 3. GetTask & ListTasks
	task, err := client.GetTask(ctx, &v1alpha1.GetTaskRequest{Atespace: "default", Name: "grpc-task"})
	if err != nil {
		t.Fatalf("GetTask failed: %v", err)
	}
	if task.Metadata.Name != "grpc-task" {
		t.Errorf("expected name 'grpc-task', got %s", task.Metadata.Name)
	}
	if task.Metadata.CreationTimestamp == nil {
		t.Errorf("expected creation timestamp to be populated on applied task")
	}

	listTasksResp, err := client.ListTasks(ctx, &v1alpha1.ListTasksRequest{Atespace: "default"})
	if err != nil {
		t.Fatalf("ListTasks failed: %v", err)
	}
	if len(listTasksResp.Tasks) != 1 {
		t.Fatalf("expected 1 task in list, got %d", len(listTasksResp.Tasks))
	}
	if listTasksResp.Tasks[0].Metadata.CreationTimestamp == nil {
		t.Errorf("expected creation timestamp on listed task")
	}

	// Test that Task is immutable
	task.Spec.Image = "ghcr.io/test/updated-image"
	_, err = client.CreateTask(ctx, &v1alpha1.CreateTaskRequest{Task: task})
	if err == nil {
		t.Fatalf("expected CreateTask to fail on existing task because tasks are immutable")
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("expected FailedPrecondition code, got %v", status.Code(err))
	}

	// 4. Suspend & Resume Task
	suspTask, err := client.SuspendTask(ctx, &v1alpha1.SuspendTaskRequest{Atespace: "default", Name: "grpc-task"})
	if err != nil {
		t.Fatalf("SuspendTask failed: %v", err)
	}
	if suspTask.Status.Phase != "Suspended" {
		t.Errorf("expected task phase to be 'Suspended', got %q", suspTask.Status.Phase)
	}

	resTask, err := client.ResumeTask(ctx, &v1alpha1.ResumeTaskRequest{Atespace: "default", Name: "grpc-task"})
	if err != nil {
		t.Fatalf("ResumeTask failed: %v", err)
	}
	if resTask.Status.Phase != "Running" {
		t.Errorf("expected task phase to be 'Running', got %q", resTask.Status.Phase)
	}

	// 5. Workspaces
	ws, err = client.GetWorkspace(ctx, &v1alpha1.GetWorkspaceRequest{Atespace: "default", Name: "grpc-ws"})
	if err != nil {
		t.Fatalf("GetWorkspace failed: %v", err)
	}
	if ws.Metadata.Name != "grpc-ws" {
		t.Errorf("expected workspace 'grpc-ws', got %s", ws.Metadata.Name)
	}

	listWorkspacesResp, err := client.ListWorkspaces(ctx, &v1alpha1.ListWorkspacesRequest{Atespace: "default"})
	if err != nil {
		t.Fatalf("ListWorkspaces failed: %v", err)
	}
	if len(listWorkspacesResp.Workspaces) != 1 {
		t.Fatalf("expected 1 workspace in list, got %d", len(listWorkspacesResp.Workspaces))
	}

	// 7. Models
	model, err := client.GetModel(ctx, &v1alpha1.GetModelRequest{Atespace: "default", Name: "grpc-model"})
	if err != nil {
		t.Fatalf("GetModel failed: %v", err)
	}
	if model.Metadata.Name != "grpc-model" {
		t.Errorf("expected model 'grpc-model', got %s", model.Metadata.Name)
	}

	listModelsResp, err := client.ListModels(ctx, &v1alpha1.ListModelsRequest{Atespace: "default"})
	if err != nil {
		t.Fatalf("ListModels failed: %v", err)
	}
	if len(listModelsResp.Models) != 1 {
		t.Fatalf("expected 1 model in list, got %d", len(listModelsResp.Models))
	}

	// 7b. WatchTask (should receive INITIAL state)
	stream, err := client.WatchTask(ctx, &v1alpha1.WatchTaskRequest{Atespace: "default", Name: "grpc-task"})
	if err != nil {
		t.Fatalf("WatchTask failed: %v", err)
	}
	watchMsg, err := stream.Recv()
	if err != nil {
		t.Fatalf("WatchTask Recv failed: %v", err)
	}
	if watchMsg.Action != "INITIAL" {
		t.Errorf("expected INITIAL action, got %s", watchMsg.Action)
	}
	if watchMsg.Task == nil || watchMsg.Task.Metadata == nil || watchMsg.Task.Metadata.Name != "grpc-task" {
		t.Errorf("expected task 'grpc-task' in watch, got %+v", watchMsg.Task)
	}

	// 8. Delete operations
	if _, err := client.DeleteTask(ctx, &v1alpha1.DeleteTaskRequest{Atespace: "default", Name: "grpc-task"}); err != nil {
		t.Fatalf("DeleteTask failed: %v", err)
	}
	if _, err := client.DeleteTask(ctx, &v1alpha1.DeleteTaskRequest{Atespace: "default", Name: "no-such-task"}); status.Code(err) != codes.NotFound {
		t.Errorf("expected NotFound deleting a missing task, got %v", err)
	}
	if _, err := client.DeleteWorkspace(ctx, &v1alpha1.DeleteWorkspaceRequest{Atespace: "default", Name: "grpc-ws"}); err != nil {
		t.Fatalf("DeleteWorkspace failed: %v", err)
	}
	if _, err := client.DeleteModel(ctx, &v1alpha1.DeleteModelRequest{Atespace: "default", Name: "grpc-model"}); err != nil {
		t.Fatalf("DeleteModel failed: %v", err)
	}

	// Check 404 after delete
	_, err = client.GetTask(ctx, &v1alpha1.GetTaskRequest{Atespace: "default", Name: "grpc-task"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound code, got %v", err)
	}
}

// Names and atespaces become Substrate resource names, which must be RFC 1123
// labels. The server rejects them up front instead of failing during Substrate
// actor creation.
func TestCreate_RejectsInvalidNames(t *testing.T) {
	srv := server.NewServer(memory.NewStore())
	ctx := context.Background()

	for _, meta := range []*v1alpha1.ObjectMeta{
		{Name: "Task-With-Caps"},
		{Name: "under_score"},
		{Name: ""},
		{Name: "ok", Atespace: "Not-Lowercase"},
	} {
		if _, err := srv.CreateTask(ctx, &v1alpha1.CreateTaskRequest{Task: &v1alpha1.Task{Metadata: meta}}); status.Code(err) != codes.InvalidArgument {
			t.Errorf("CreateTask(%v): got %v, want InvalidArgument", meta, err)
		}
		if _, err := srv.UpdateWorkspace(ctx, &v1alpha1.UpdateWorkspaceRequest{Workspace: &v1alpha1.Workspace{Metadata: meta}}); status.Code(err) != codes.InvalidArgument {
			t.Errorf("UpdateWorkspace(%v): got %v, want InvalidArgument", meta, err)
		}
		if _, err := srv.UpdateModel(ctx, &v1alpha1.UpdateModelRequest{Model: &v1alpha1.Model{Metadata: meta}}); status.Code(err) != codes.InvalidArgument {
			t.Errorf("UpdateModel(%v): got %v, want InvalidArgument", meta, err)
		}
	}

	// Nothing invalid was persisted.
	if resp, err := srv.ListTasks(ctx, &v1alpha1.ListTasksRequest{}); err != nil || len(resp.GetTasks()) != 0 {
		t.Errorf("ListTasks after rejected applies = %v, %v; want empty", resp.GetTasks(), err)
	}

	// Valid names still go through, with and without an explicit atespace.
	good := &v1alpha1.ObjectMeta{Name: "task-with-caps", Atespace: "team-a"}
	if _, err := srv.CreateTask(ctx, &v1alpha1.CreateTaskRequest{Task: &v1alpha1.Task{Metadata: good}}); err != nil {
		t.Errorf("CreateTask(%v): %v", good, err)
	}
	if _, err := srv.UpdateWorkspace(ctx, &v1alpha1.UpdateWorkspaceRequest{Workspace: &v1alpha1.Workspace{Metadata: &v1alpha1.ObjectMeta{Name: "ws-1"}}}); err != nil {
		t.Errorf("UpdateWorkspace: %v", err)
	}
	if _, err := srv.UpdateModel(ctx, &v1alpha1.UpdateModelRequest{Model: &v1alpha1.Model{Metadata: &v1alpha1.ObjectMeta{Name: "gemini"}}}); err != nil {
		t.Errorf("UpdateModel: %v", err)
	}
}

func TestCreateTask_ValidatesWorkspaceBindings(t *testing.T) {
	srv := server.NewServer(memory.NewStore())
	ctx := context.Background()

	_, err := srv.CreateTask(ctx, &v1alpha1.CreateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "bad"},
		Spec: &v1alpha1.TaskSpec{
			Workspaces: []*v1alpha1.WorkspaceRef{{Name: "a", Path: "/same"}, {Name: "b", Path: "/same"}},
		},
	}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for colliding workspace paths, got %v", err)
	}

	_, err = srv.CreateTask(ctx, &v1alpha1.CreateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "good"},
		Spec: &v1alpha1.TaskSpec{
			Workspaces: []*v1alpha1.WorkspaceRef{{Name: "a"}, {Name: "b"}},
		},
	}})
	if err != nil {
		t.Fatalf("expected a valid multi-workspace task to be accepted, got %v", err)
	}
}

type fakeReconciler struct {
	reconcileCount int
	deleteCount    int
	deleteErr      error
	onDelete       func(ctx context.Context, atespace, taskName string) error
	// cancelCtx, when set, is called right after a successful reconcile, to
	// simulate the caller's context expiring exactly in the gap between
	// reconciliation finishing and the handler's final status write.
	cancelCtx context.CancelFunc
	// started, when set, is closed (once) as soon as ReconcileWithProvider is
	// entered, so a test can wait for reconciliation (and any credential
	// provider lock held across it) to be underway before acting. Guarded by
	// startedOnce since a test may reuse the same fakeReconciler across
	// multiple calls after the one it's actually synchronizing on.
	started     chan struct{}
	startedOnce sync.Once
	// release, when set, is waited on before ReconcileWithProvider returns,
	// so a test can hold reconciliation open to probe what else can or
	// cannot proceed concurrently with it.
	release chan struct{}
}

func (f *fakeReconciler) Reconcile(ctx context.Context, task *v1alpha1.Task, workspaces ...*v1alpha1.Workspace) (*v1alpha1.Task, error) {
	return f.ReconcileWithProvider(ctx, task, nil, workspaces...)
}

func (f *fakeReconciler) ReconcileWithProvider(ctx context.Context, task *v1alpha1.Task, provider *v1alpha1.CredentialProvider, workspaces ...*v1alpha1.Workspace) (*v1alpha1.Task, error) {
	f.reconcileCount++
	if f.started != nil {
		f.startedOnce.Do(func() { close(f.started) })
	}
	if f.release != nil {
		<-f.release
	}
	task.Status = &v1alpha1.TaskStatus{
		Phase: task.GetStatus().GetPhase(),
	}
	if f.cancelCtx != nil {
		f.cancelCtx()
	}
	return task, nil
}

func (f *fakeReconciler) ReconcileDelete(ctx context.Context, atespace, taskName string, hasCredentialProvider bool) error {
	f.deleteCount++
	if f.onDelete != nil {
		return f.onDelete(ctx, atespace, taskName)
	}
	return f.deleteErr
}

// TestResumeTaskPersistsStatusDespiteExpiredRequestContext covers a request
// context that expires exactly after reconciliation succeeds but before the
// resulting status is persisted -- as a client-set deadline that merely
// matches the reconciler's own workspace-readiness poll window easily can.
// The final status write must not be lost to that same expired context, or a
// genuinely successful resume gets reported as failed and the task is left
// stored as Suspended while its actor is actually running.
func TestResumeTaskPersistsStatusDespiteExpiredRequestContext(t *testing.T) {
	rec := &fakeReconciler{}
	srv := server.NewServer(memory.NewStore(), server.Options{Reconciler: rec})

	if _, err := srv.CreateTask(context.Background(), &v1alpha1.CreateTaskRequest{
		Task: &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "task-resume"}, Spec: &v1alpha1.TaskSpec{Image: "alpine"}},
	}); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	rec.cancelCtx = cancel
	task, err := srv.ResumeTask(ctx, &v1alpha1.ResumeTaskRequest{Name: "task-resume"})
	if err != nil {
		t.Fatalf("ResumeTask failed even though the context only expired after reconciliation succeeded: %v", err)
	}
	if task.GetStatus().GetPhase() != "Running" {
		t.Fatalf("returned phase = %q, want Running", task.GetStatus().GetPhase())
	}

	stored, err := srv.GetTask(context.Background(), &v1alpha1.GetTaskRequest{Name: "task-resume"})
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if stored.GetStatus().GetPhase() != "Running" {
		t.Fatalf("stored phase = %q, want Running (the status write must not be lost to the expired request context)", stored.GetStatus().GetPhase())
	}
}

// TestCreateTask_HoldsCredentialProviderLockAcrossReconcile covers a
// CreateTask whose reconciliation (which mints a GitHub installation token
// from the credential provider's current repository/permission scope) is
// still in flight: a concurrent DeleteCredentialProvider for that same
// provider must not be able to acquire its lock and report success while
// token issuance against the still-being-read configuration hasn't finished,
// since that could delete (or a concurrent restrictive update could narrow)
// the provider out from under a token already being minted from stale scope.
func TestCreateTask_HoldsCredentialProviderLockAcrossReconcile(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	rec := &fakeReconciler{started: started, release: release}
	srv := server.NewServer(memory.NewStore(), server.Options{Reconciler: rec})

	provider := &v1alpha1.CredentialProvider{Metadata: &v1alpha1.ObjectMeta{Name: "github"}, Spec: &v1alpha1.CredentialProviderSpec{GithubApp: &v1alpha1.GitHubAppCredential{AppId: 1, InstallationId: 2, PrivateKeySecret: &v1alpha1.SecretKeyRef{Name: "key", Key: "pem"}, Repositories: []string{"one"}, Permissions: map[string]string{"contents": "read"}}}}
	if _, err := srv.UpdateCredentialProvider(context.Background(), &v1alpha1.UpdateCredentialProviderRequest{CredentialProvider: provider}); err != nil {
		t.Fatalf("UpdateCredentialProvider: %v", err)
	}

	createDone := make(chan error, 1)
	go func() {
		_, err := srv.CreateTask(context.Background(), &v1alpha1.CreateTaskRequest{
			Task: &v1alpha1.Task{
				Metadata: &v1alpha1.ObjectMeta{Name: "job"},
				Spec:     &v1alpha1.TaskSpec{CredentialProvider: &v1alpha1.CredentialProviderRef{Name: "github"}},
			},
		})
		createDone <- err
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("CreateTask never reached reconciliation")
	}

	// CreateTask is now holding the credential provider's lock mid-reconcile:
	// a delete attempting to acquire the same lock must block, not succeed.
	shortCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err := srv.DeleteCredentialProvider(shortCtx, &v1alpha1.DeleteCredentialProviderRequest{Name: "github"})
	cancel()
	if err == nil {
		t.Fatal("DeleteCredentialProvider succeeded while CreateTask was still reconciling against that provider")
	}

	close(release)
	if err := <-createDone; err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	// With reconciliation finished and the lock released, the delete can now
	// proceed normally.
	if _, err := srv.DeleteCredentialProvider(context.Background(), &v1alpha1.DeleteCredentialProviderRequest{Name: "github"}); err != nil {
		t.Fatalf("DeleteCredentialProvider after release: %v", err)
	}
}

// TestCreateTask_FailedProviderLockLeavesNoTaskRecord covers a CreateTask
// whose credential provider lock acquisition is contended until its request
// context expires: if that lock were acquired only after the task record is
// saved, the save would already be durable (tasks are immutable) by the time
// the lock attempt times out, so CreateTask would report Aborted while a
// retry then fails with "already exists" forever, having never actually
// reconciled. The lock must be acquired, and therefore be the thing that
// fails, before anything is persisted.
func TestCreateTask_FailedProviderLockLeavesNoTaskRecord(t *testing.T) {
	holderStarted := make(chan struct{})
	holderRelease := make(chan struct{})
	holderRec := &fakeReconciler{started: holderStarted, release: holderRelease}
	srv := server.NewServer(memory.NewStore(), server.Options{Reconciler: holderRec})

	provider := &v1alpha1.CredentialProvider{Metadata: &v1alpha1.ObjectMeta{Name: "github"}, Spec: &v1alpha1.CredentialProviderSpec{GithubApp: &v1alpha1.GitHubAppCredential{AppId: 1, InstallationId: 2, PrivateKeySecret: &v1alpha1.SecretKeyRef{Name: "key", Key: "pem"}, Repositories: []string{"one"}, Permissions: map[string]string{"contents": "read"}}}}
	if _, err := srv.UpdateCredentialProvider(context.Background(), &v1alpha1.UpdateCredentialProviderRequest{CredentialProvider: provider}); err != nil {
		t.Fatalf("UpdateCredentialProvider: %v", err)
	}

	// Hold the provider's lock open via a first task's in-flight reconcile.
	holderDone := make(chan error, 1)
	go func() {
		_, err := srv.CreateTask(context.Background(), &v1alpha1.CreateTaskRequest{
			Task: &v1alpha1.Task{
				Metadata: &v1alpha1.ObjectMeta{Name: "holder"},
				Spec:     &v1alpha1.TaskSpec{CredentialProvider: &v1alpha1.CredentialProviderRef{Name: "github"}},
			},
		})
		holderDone <- err
	}()
	select {
	case <-holderStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("holder CreateTask never reached reconciliation")
	}

	// A second, different task contends for the same provider's lock, with a
	// context that expires well before the holder releases it.
	shortCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err := srv.CreateTask(shortCtx, &v1alpha1.CreateTaskRequest{
		Task: &v1alpha1.Task{
			Metadata: &v1alpha1.ObjectMeta{Name: "contender"},
			Spec:     &v1alpha1.TaskSpec{CredentialProvider: &v1alpha1.CredentialProviderRef{Name: "github"}},
		},
	})
	cancel()
	if err == nil {
		t.Fatal("contending CreateTask succeeded despite the provider's lock being held")
	}

	// The failed attempt must not have left a durable (and immutable) task
	// record behind: GetTask should report it was never saved.
	if _, getErr := srv.GetTask(context.Background(), &v1alpha1.GetTaskRequest{Name: "contender"}); status.Code(getErr) != codes.NotFound {
		t.Fatalf("GetTask after failed lock acquisition: err=%v, want NotFound (no task record should have been saved)", getErr)
	}

	close(holderRelease)
	if err := <-holderDone; err != nil {
		t.Fatalf("holder CreateTask: %v", err)
	}

	// Now that the lock is free, creating the same task name must succeed --
	// it must not be stuck as "already exists" from a save that never
	// actually happened.
	if _, err := srv.CreateTask(context.Background(), &v1alpha1.CreateTaskRequest{
		Task: &v1alpha1.Task{
			Metadata: &v1alpha1.ObjectMeta{Name: "contender"},
			Spec:     &v1alpha1.TaskSpec{CredentialProvider: &v1alpha1.CredentialProviderRef{Name: "github"}},
		},
	}); err != nil {
		t.Fatalf("CreateTask retry after lock freed: %v", err)
	}
}

func TestServer_DirectReconcilerLifecycle(t *testing.T) {
	rec := &fakeReconciler{}
	srv := server.NewServer(memory.NewStore(), server.Options{Reconciler: rec})
	ctx := context.Background()

	// 1. CreateTask directly calls Reconciler
	task, err := srv.CreateTask(ctx, &v1alpha1.CreateTaskRequest{
		Task: &v1alpha1.Task{
			Metadata: &v1alpha1.ObjectMeta{Name: "task-rec"},
			Spec:     &v1alpha1.TaskSpec{Image: "alpine"},
		},
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if rec.reconcileCount != 1 {
		t.Errorf("expected 1 reconcile call on CreateTask, got %d", rec.reconcileCount)
	}
	if task.GetStatus().GetPhase() != "Suspended" {
		t.Errorf("expected phase Suspended, got %s", task.GetStatus().GetPhase())
	}

	// 2. ResumeTask directly calls Reconciler
	task, err = srv.ResumeTask(ctx, &v1alpha1.ResumeTaskRequest{Name: "task-rec"})
	if err != nil {
		t.Fatalf("ResumeTask: %v", err)
	}
	if rec.reconcileCount != 2 {
		t.Errorf("expected 2 reconcile calls after ResumeTask, got %d", rec.reconcileCount)
	}
	if task.GetStatus().GetPhase() != "Running" {
		t.Errorf("expected phase Running, got %s", task.GetStatus().GetPhase())
	}

	// 3. SuspendTask directly calls Reconciler
	task, err = srv.SuspendTask(ctx, &v1alpha1.SuspendTaskRequest{Name: "task-rec"})
	if err != nil {
		t.Fatalf("SuspendTask: %v", err)
	}
	if rec.reconcileCount != 3 {
		t.Errorf("expected 3 reconcile calls after SuspendTask, got %d", rec.reconcileCount)
	}
	if task.GetStatus().GetPhase() != "Suspended" {
		t.Errorf("expected phase Suspended, got %s", task.GetStatus().GetPhase())
	}

	// 4. DeleteTask directly calls ReconcileDelete
	_, err = srv.DeleteTask(ctx, &v1alpha1.DeleteTaskRequest{Name: "task-rec"})
	if err != nil {
		t.Fatalf("DeleteTask: %v", err)
	}
	if rec.deleteCount != 1 {
		t.Errorf("expected 1 delete call, got %d", rec.deleteCount)
	}

	// 5. Verify task is gone
	_, err = srv.GetTask(ctx, &v1alpha1.GetTaskRequest{Name: "task-rec"})
	if status.Code(err) != codes.NotFound {
		t.Errorf("expected NotFound after DeleteTask, got %v", err)
	}
}

func TestServer_DeleteTask_BlocksAndSetsPhaseTerminating(t *testing.T) {
	st := memory.NewStore()
	var observedPhaseDuringDelete string
	rec := &fakeReconciler{
		onDelete: func(ctx context.Context, atespace, taskName string) error {
			t, err := st.GetTask(ctx, atespace, taskName)
			if err == nil && t.Status != nil {
				observedPhaseDuringDelete = t.Status.Phase
			}
			return nil
		},
	}
	srv := server.NewServer(st, server.Options{Reconciler: rec})
	ctx := context.Background()

	_, err := srv.CreateTask(ctx, &v1alpha1.CreateTaskRequest{
		Task: &v1alpha1.Task{
			Metadata: &v1alpha1.ObjectMeta{Name: "task-term"},
			Spec:     &v1alpha1.TaskSpec{Image: "alpine"},
		},
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	_, err = srv.DeleteTask(ctx, &v1alpha1.DeleteTaskRequest{Name: "task-term"})
	if err != nil {
		t.Fatalf("DeleteTask: %v", err)
	}

	if observedPhaseDuringDelete != v1alpha1.PhaseTerminating {
		t.Errorf("expected phase %q during delete, got %q", v1alpha1.PhaseTerminating, observedPhaseDuringDelete)
	}

	_, err = srv.GetTask(ctx, &v1alpha1.GetTaskRequest{Name: "task-term"})
	if status.Code(err) != codes.NotFound {
		t.Errorf("expected NotFound after successful DeleteTask, got %v", err)
	}
}

func TestServer_DeleteTask_ReconcileErrorRetainsTask(t *testing.T) {
	st := memory.NewStore()
	rec := &fakeReconciler{
		deleteErr: errors.New("substrate timeout deleting actor"),
	}
	srv := server.NewServer(st, server.Options{Reconciler: rec})
	ctx := context.Background()

	_, err := srv.CreateTask(ctx, &v1alpha1.CreateTaskRequest{
		Task: &v1alpha1.Task{
			Metadata: &v1alpha1.ObjectMeta{Name: "task-err"},
			Spec:     &v1alpha1.TaskSpec{Image: "alpine"},
		},
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	_, err = srv.DeleteTask(ctx, &v1alpha1.DeleteTaskRequest{Name: "task-err"})
	if err == nil {
		t.Fatal("expected DeleteTask to fail when ReconcileDelete fails")
	}

	task, err := srv.GetTask(ctx, &v1alpha1.GetTaskRequest{Name: "task-err"})
	if err != nil {
		t.Fatalf("expected task to remain in store, got %v", err)
	}
	if task.GetStatus().GetPhase() != v1alpha1.PhaseTerminating {
		t.Errorf("expected phase %q, got %q", v1alpha1.PhaseTerminating, task.GetStatus().GetPhase())
	}
}

type failUpdateStatusStore struct {
	store.Store
	failUpdateStatus bool
}

func (f *failUpdateStatusStore) UpdateTaskStatus(ctx context.Context, atespace, name string, status *v1alpha1.TaskStatus) error {
	if f.failUpdateStatus {
		return errors.New("simulated store failure")
	}
	return f.Store.UpdateTaskStatus(ctx, atespace, name, status)
}

func TestServer_DeleteTask_UpdateStatusError(t *testing.T) {
	base := memory.NewStore()
	st := &failUpdateStatusStore{Store: base}
	rec := &fakeReconciler{}
	srv := server.NewServer(st, server.Options{Reconciler: rec})
	ctx := context.Background()

	_, err := srv.CreateTask(ctx, &v1alpha1.CreateTaskRequest{
		Task: &v1alpha1.Task{
			Metadata: &v1alpha1.ObjectMeta{Name: "task-status-fail"},
			Spec:     &v1alpha1.TaskSpec{Image: "alpine"},
		},
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	st.failUpdateStatus = true
	_, err = srv.DeleteTask(ctx, &v1alpha1.DeleteTaskRequest{Name: "task-status-fail"})
	if err == nil {
		t.Fatal("expected DeleteTask to fail when UpdateTaskStatus fails")
	}
	if rec.deleteCount != 0 {
		t.Errorf("expected ReconcileDelete not to be called if UpdateTaskStatus fails, got %d calls", rec.deleteCount)
	}
}
