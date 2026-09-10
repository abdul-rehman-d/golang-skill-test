package jobs

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

type createJobRequest struct {
	Payload string `json:"payload"`
}

const MAX_REQUEST_BODY = 1 << 20 // 1 MiB

func RegisterHandlers(mux *http.ServeMux, service *Service) {
	mux.HandleFunc("POST /jobs", func(w http.ResponseWriter, r *http.Request) {
		var req createJobRequest

		r.Body = http.MaxBytesReader(w, r.Body, MAX_REQUEST_BODY)

		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}

		if err := json.Unmarshal(body, &req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}

		if strings.TrimSpace(req.Payload) == "" {
			writeError(w, http.StatusBadRequest, "payload is required")
			return
		}

		job, err := service.Create(r.Context(), req.Payload)
		if err != nil {
			if strings.Contains(err.Error(), "queue is full") {
				writeError(w, http.StatusServiceUnavailable, err.Error())
				return
			}
			writeError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, job)
	})

	mux.HandleFunc("GET /jobs/", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/jobs/")
		if id == "" {
			writeError(w, http.StatusNotFound, "job not found")
			return
		}
		job, ok := service.Get(id)
		if !ok {
			writeError(w, http.StatusNotFound, "job not found")
			return
		}
		writeJSON(w, http.StatusOK, job)
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
