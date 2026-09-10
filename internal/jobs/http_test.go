package jobs

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestPostJobsRejectsInvalidRequests(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "empty body", body: ""},
		{name: "malformed JSON", body: `{"payload":`},
		{name: "missing payload", body: `{}`},
		{name: "empty payload", body: `{"payload":""}`},
		{name: "blank payload", body: `{"payload":"   "}`},
		// Decoding one value is not enough: accepting a second JSON value or
		// trailing garbage makes an otherwise malformed request look valid.
		{name: "multiple JSON values", body: `{"payload":"hello"}{"payload":"again"}`},
		{name: "trailing garbage", body: `{"payload":"hello"}not-json`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewService(1, 1)
			t.Cleanup(s.Stop)

			recorder := performRequest(s, http.MethodPost, "/jobs", tt.body)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
			}

			assertJSONContentType(t, recorder)
			assertErrorResponse(t, recorder)
		})
	}
}

func TestPostJobsCreatesQueuedJob(t *testing.T) {
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

	// Occupy the worker so it cannot change the HTTP-created job before the
	// response is encoded. The API contract says a newly created job is queued.
	if _, err := s.Create(context.Background(), "occupy worker"); err != nil {
		t.Fatalf("occupy worker: %v", err)
	}
	<-started

	recorder := performRequest(s, http.MethodPost, "/jobs", `{"payload":"hello"}`)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusCreated, recorder.Body.String())
	}

	assertJSONContentType(t, recorder)
	var job Job
	if err := json.NewDecoder(recorder.Body).Decode(&job); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if job.ID == "" {
		t.Fatal("created job has no ID")
	}
	if job.Status != StatusQueued {
		t.Fatalf("status = %q, want %q", job.Status, StatusQueued)
	}
}

func TestGetJobsReturnsCreatedJob(t *testing.T) {
	s := NewService(1, 1)
	t.Cleanup(s.Stop)

	job, err := s.Create(context.Background(), "hello")
	if err != nil {
		t.Fatalf("create job: %v", err)
	}

	recorder := performRequest(s, http.MethodGet, "/jobs/"+job.ID, "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}

	assertJSONContentType(t, recorder)
	var response Job
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.ID != job.ID {
		t.Fatalf("job ID = %q, want %q", response.ID, job.ID)
	}
}

func TestGetJobsReturnsNotFound(t *testing.T) {
	s := NewService(1, 1)
	t.Cleanup(s.Stop)

	recorder := performRequest(s, http.MethodGet, "/jobs/does-not-exist", "")
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusNotFound, recorder.Body.String())
	}

	assertJSONContentType(t, recorder)
	assertErrorResponse(t, recorder)
}

func TestPostJobsReturnsServiceUnavailableWhenQueueIsFull(t *testing.T) {
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

	// The first job occupies the only worker; the second occupies the only
	// queue slot. The third request must fail immediately instead of blocking.
	first := performRequest(s, http.MethodPost, "/jobs", `{"payload":"first"}`)
	if first.Code != http.StatusCreated {
		t.Fatalf("first status = %d, want %d", first.Code, http.StatusCreated)
	}
	<-started

	second := performRequest(s, http.MethodPost, "/jobs", `{"payload":"second"}`)
	if second.Code != http.StatusCreated {
		t.Fatalf("second status = %d, want %d", second.Code, http.StatusCreated)
	}

	third := performRequest(s, http.MethodPost, "/jobs", `{"payload":"third"}`)
	if third.Code != http.StatusServiceUnavailable {
		t.Fatalf("third status = %d, want %d; body = %s", third.Code, http.StatusServiceUnavailable, third.Body.String())
	}
	assertErrorResponse(t, third)
}

func TestPostJobsReturnsServiceUnavailableDuringShutdown(t *testing.T) {
	s := NewService(1, 1)
	s.Stop()

	recorder := performRequest(s, http.MethodPost, "/jobs", `{"payload":"hello"}`)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusServiceUnavailable, recorder.Body.String())
	}
	assertErrorResponse(t, recorder)
}

func performRequest(s *Service, method, target, body string) *httptest.ResponseRecorder {
	mux := http.NewServeMux()
	RegisterHandlers(mux, s)

	request := httptest.NewRequest(method, target, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)
	return recorder
}

func assertJSONContentType(t *testing.T, recorder *httptest.ResponseRecorder) {
	t.Helper()
	if got := recorder.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want %q", got, "application/json")
	}
}

func assertErrorResponse(t *testing.T, recorder *httptest.ResponseRecorder) {
	t.Helper()
	var response struct {
		Error string `json:"error"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if response.Error == "" {
		t.Fatal("error response has an empty message")
	}
}
