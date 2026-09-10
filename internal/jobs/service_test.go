package jobs

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestCreateAndGet(t *testing.T) {
	s := NewService(1, 4)
	defer s.Stop()

	job, err := s.Create(context.Background(), "hello")
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != StatusQueued {
		t.Fatalf("expected queued, got %s", job.Status)
	}

	got, ok := s.Get(job.ID)
	if !ok || got.ID != job.ID {
		t.Fatalf("job not found")
	}
}

func TestFailedJob(t *testing.T) {
	s := NewService(1, 4)
	defer s.Stop()

	job, err := s.Create(context.Background(), "please fail")
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		got, _ := s.Get(job.ID)
		if got.Status == StatusFailed {
			if got.Error == "" {
				t.Fatal("expected failure message")
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("job did not fail")
}

func TestJobMovesFromProcessingToCompleted(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})

	s := NewService(1, 1)
	s.processor = ProcessorFunc(func(ctx context.Context, payload string) error {
		close(started)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	t.Cleanup(s.Stop)

	job, err := s.Create(context.Background(), "hello")
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	<-started

	processing, ok := s.Get(job.ID)
	if !ok || processing.Status != StatusProcessing {
		t.Fatalf("job while processor is blocked = %#v, found %t; want processing", processing, ok)
	}

	close(release)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		completed, ok := s.Get(job.ID)
		if ok && completed.Status == StatusCompleted {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("job did not complete after processor returned")
}

func TestConcurrentCreate(t *testing.T) {
	s := NewService(4, 100)
	defer s.Stop()

	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.Create(context.Background(), "hello"); err != nil {
				t.Errorf("create: %v", err)
			}
		}()
	}
	wg.Wait()
}

func TestQueueFullDoesNotStoreRejectedJob(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once

	s := NewService(1, 1)
	s.processor = ProcessorFunc(func(ctx context.Context, payload string) error {
		startedOnce.Do(func() { close(started) })
		<-release
		return nil
	})
	t.Cleanup(func() {
		close(release)
		s.Stop()
	})

	// Keep the only worker occupied and fill the only queue slot so the next
	// Create call deterministically exercises the backpressure path.
	if _, err := s.Create(context.Background(), "first"); err != nil {
		t.Fatalf("create first job: %v", err)
	}
	<-started
	if _, err := s.Create(context.Background(), "second"); err != nil {
		t.Fatalf("create second job: %v", err)
	}

	if _, err := s.Create(context.Background(), "rejected"); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("create error = %v, want %v", err, ErrQueueFull)
	}

	// A rejected job was never accepted for processing, so it must not remain
	// discoverable in the service as a permanently queued ghost record.
	if job, ok := s.Get("job-3"); ok {
		t.Fatalf("rejected job remains stored with status %q", job.Status)
	}
}

func TestCreateRejectsCanceledContext(t *testing.T) {
	s := NewService(1, 1)
	t.Cleanup(s.Stop)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := s.Create(ctx, "hello"); !errors.Is(err, context.Canceled) {
		t.Fatalf("create error = %v, want %v", err, context.Canceled)
	}
	if _, ok := s.Get("job-1"); ok {
		t.Fatal("canceled request stored a job")
	}
}

func TestWorkersProcessJobsConcurrently(t *testing.T) {
	started := make(chan string, 2)
	release := make(chan struct{})

	s := NewService(2, 2)
	s.processor = ProcessorFunc(func(ctx context.Context, payload string) error {
		started <- payload
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	t.Cleanup(s.Stop)

	if _, err := s.Create(context.Background(), "first"); err != nil {
		t.Fatalf("create first job: %v", err)
	}
	if _, err := s.Create(context.Background(), "second"); err != nil {
		t.Fatalf("create second job: %v", err)
	}

	// Both jobs must enter the processor before either is released; otherwise
	// the configured workers are not actually operating concurrently.
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			close(release)
			t.Fatal("jobs were not processed concurrently")
		}
	}
	close(release)
}

func TestStopCancelsInFlightProcessor(t *testing.T) {
	started := make(chan struct{})
	stopped := make(chan struct{})

	s := NewService(1, 1)
	s.processor = ProcessorFunc(func(ctx context.Context, payload string) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})

	job, err := s.Create(context.Background(), "in progress")
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	<-started

	go func() {
		s.Stop()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop did not cancel the in-flight processor")
	}

	got, ok := s.Get(job.ID)
	if !ok {
		t.Fatal("job disappeared during shutdown")
	}
	if got.Status != StatusFailed || got.Error == "" {
		t.Fatalf("job after shutdown = status %q, error %q; want failed with an error", got.Status, got.Error)
	}
}

func TestCreateAndStopAreSafeWhenConcurrent(t *testing.T) {
	const creators = 100

	s := NewService(4, creators)
	start := make(chan struct{})
	panics := make(chan any, creators)
	var wg sync.WaitGroup

	for i := 0; i < creators; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if recovered := recover(); recovered != nil {
					panics <- recovered
				}
			}()
			<-start
			_, err := s.Create(context.Background(), "hello")
			if err != nil && !errors.Is(err, ErrServiceStopping) && !errors.Is(err, ErrQueueFull) {
				t.Errorf("unexpected create error: %v", err)
			}
		}()
	}

	close(start)
	s.Stop()
	wg.Wait()
	close(panics)

	for recovered := range panics {
		t.Errorf("Create panicked during Stop: %v", recovered)
	}
}

func TestStopIsSafeWhenCalledMoreThanOnce(t *testing.T) {
	s := NewService(2, 2)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Stop()
		}()
	}
	wg.Wait()
}
