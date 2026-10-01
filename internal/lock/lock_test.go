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

package lock_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/ax/internal/lock"
	"github.com/redis/go-redis/v9"
)

func TestMemoryLocker_SerializesSameResource(t *testing.T) {
	locker := lock.NewMemoryLocker()
	ctx := context.Background()

	var counter int64
	var wg sync.WaitGroup

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			unlock, err := locker.Lock(ctx, "task", "default", "my-task")
			if err != nil {
				t.Errorf("unexpected lock error: %v", err)
				return
			}
			defer unlock()

			current := atomic.AddInt64(&counter, 1)
			time.Sleep(10 * time.Millisecond)
			if atomic.LoadInt64(&counter) != current {
				t.Errorf("race condition detected: counter changed while lock held")
			}
		}()
	}

	wg.Wait()
}

func TestMemoryLocker_ConcurrentDifferentResources(t *testing.T) {
	locker := lock.NewMemoryLocker()
	ctx := context.Background()

	unlock1, err := locker.Lock(ctx, "task", "default", "task-1")
	if err != nil {
		t.Fatalf("lock task-1: %v", err)
	}
	defer unlock1()

	// Different resource should acquire immediately
	start := time.Now()
	unlock2, err := locker.Lock(ctx, "task", "default", "task-2")
	if err != nil {
		t.Fatalf("lock task-2: %v", err)
	}
	defer unlock2()

	if time.Since(start) > 50*time.Millisecond {
		t.Errorf("different resource lock was blocked")
	}
}

func TestMemoryLocker_Timeout(t *testing.T) {
	locker := lock.NewMemoryLocker()
	ctx := context.Background()

	unlock1, err := locker.Lock(ctx, "task", "default", "task-1")
	if err != nil {
		t.Fatalf("lock task-1: %v", err)
	}
	defer unlock1()

	timeoutCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()

	_, err = locker.Lock(timeoutCtx, "task", "default", "task-1")
	if err == nil {
		t.Fatalf("expected timeout error, got nil")
	}
}

func newMiniredisLocker(t *testing.T, ttl, fallback time.Duration) *lock.RedisLocker {
	t.Helper()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("starting miniredis: %v", err)
	}
	t.Cleanup(srv.Close)
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { client.Close() })
	return lock.NewRedisLocker(client, lock.RedisLockerOptions{TTL: ttl, FallbackInterval: fallback})
}

// TestRedisLocker_RenewsBeforeTTLExpires covers work held under the lock
// running longer than its fixed TTL (as a credentialed resume realistically
// can): without renewal, Redis would expire the key out from under the
// holder and let a second Lock call acquire it while the first is still
// active.
func TestRedisLocker_RenewsBeforeTTLExpires(t *testing.T) {
	locker := newMiniredisLocker(t, 100*time.Millisecond, 20*time.Millisecond)
	ctx := context.Background()

	unlock, err := locker.Lock(ctx, "task", "default", "job")
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}

	// Outlive the original TTL several times over while still holding the
	// lock, so renewal -- not luck -- is what's being exercised.
	time.Sleep(350 * time.Millisecond)

	contendedCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if _, err := locker.Lock(contendedCtx, "task", "default", "job"); err == nil {
		t.Fatal("a second Lock call succeeded while the first lock, held past its TTL, was still active")
	}

	unlock()

	releasedCtx, cancel2 := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel2()
	unlock2, err := locker.Lock(releasedCtx, "task", "default", "job")
	if err != nil {
		t.Fatalf("Lock after unlock: %v", err)
	}
	unlock2()
}
