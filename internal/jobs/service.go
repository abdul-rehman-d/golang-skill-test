package jobs

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type Status string

const (
	StatusQueued     Status = "queued"
	StatusProcessing Status = "processing"
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"
)

type Job struct {
	ID        string    `json:"id"`
	Payload   string    `json:"payload"`
	Status    Status    `json:"status"`
	Error     string    `json:"error,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

type Processor interface {
	Process(ctx context.Context, payload string) error
}

var (
	// internal errors
	ErrQueueFull       = errors.New("job queue is full")
	ErrServiceStopping = errors.New("service is stopping")
)

type Service struct {
	mu        sync.RWMutex
	jobs      map[string]*Job
	queue     chan string
	processor Processor
	stopping  bool
	ctx       context.Context
	cancel    context.CancelFunc
	stopOnce  sync.Once
	wg        sync.WaitGroup
	sequence  uint64
}

func NewService(workers, queueCapacity int) *Service {
	if workers < 1 {
		workers = 1
	}
	if queueCapacity < 1 {
		queueCapacity = 1
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &Service{
		jobs:      make(map[string]*Job),
		queue:     make(chan string, queueCapacity),
		processor: ProcessorFunc(defaultProcessor),
		ctx:       ctx,
		cancel:    cancel,
	}
	s.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go s.worker()
	}
	return s
}

type ProcessorFunc func(context.Context, string) error

func (f ProcessorFunc) Process(ctx context.Context, payload string) error {
	return f(ctx, payload)
}

func defaultProcessor(ctx context.Context, payload string) error {
	select {
	case <-time.After(100 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}
	if payload == "" {
		return errors.New("empty payload")
	}
	if containsFail(payload) {
		return errors.New("simulated processing failure")
	}
	return nil
}

func containsFail(s string) bool {
	for i := 0; i+4 <= len(s); i++ {
		if s[i:i+4] == "fail" {
			return true
		}
	}
	return false
}

func (s *Service) Create(ctx context.Context, payload string) (*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopping {
		return nil, ErrServiceStopping
	}

	// ctx.Err() replaces the ctx.Done() select case
	// since there is no waiting now
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.sequence++
	id := fmt.Sprintf("job-%d", s.sequence)
	job := &Job{
		ID:        id,
		Payload:   payload,
		Status:    StatusQueued,
		CreatedAt: time.Now().UTC(),
	}

	s.jobs[id] = job

	select {
	case s.queue <- id:
		// accepted immediately
		return cloneJob(job), nil
	default:
		// rejected immediately
		delete(s.jobs, id)
		return nil, ErrQueueFull
	}
}

func (s *Service) Get(id string) (*Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, ok := s.jobs[id]
	if !ok {
		return nil, false
	}
	return cloneJob(job), true
}

func (s *Service) worker() {
	defer s.wg.Done()
	for id := range s.queue {
		s.process(id)
	}
}

func (s *Service) process(id string) {
	s.mu.Lock()
	job, ok := s.jobs[id]
	if !ok {
		s.mu.Unlock()
		return
	}
	job.Status = StatusProcessing
	s.mu.Unlock()

	err := s.processor.Process(s.ctx, job.Payload)

	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		job.Status = StatusFailed
		job.Error = err.Error()
		return
	}
	job.Status = StatusCompleted
}

func (s *Service) Stop() {
	s.stopOnce.Do(func() {
		s.mu.Lock()
		s.stopping = true
		s.cancel()
		close(s.queue)
		s.mu.Unlock()

		s.wg.Wait()
	})
}

func cloneJob(j *Job) *Job {
	copy := *j
	return &copy
}
