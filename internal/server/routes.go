package server

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/AbraTM/gork/internal/job"
)

type publishRequest struct {
	ID      string          `json:"id"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type statsResponse struct {
	QueueLen    int `json:"queue_len"`
	WorkerCount int `json:"worker_count"`
}

func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	var req publishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	if req.Type == "" {
		http.Error(w, "type is required", http.StatusBadRequest)
		return
	}

	j := job.Job{
		ID:        req.ID,
		Type:      req.Type,
		Payload:   []byte(req.Payload),
		CreatedAt: time.Now(),
	}

	if err := s.engine.Publish(r.Context(), j); err != nil {
		http.Error(w, "failed to publish job", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	resp := statsResponse{
		QueueLen:    s.engine.QueueLen(),
		WorkerCount: s.engine.WorkerCount(),
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
	}
}
