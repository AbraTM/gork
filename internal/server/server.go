package server

import (
	"context"
	"net/http"

	"github.com/AbraTM/gork/internal/job"
)

type EngineAPI interface {
	Publish(ctx context.Context, j job.Job) error
	QueueLen() int
	WorkerCount() int
}

type Server struct {
	engine EngineAPI
	http   *http.Server
}

func New(addr string, e EngineAPI) *Server {
	s := &Server{engine: e}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", s.handlePublish)
	mux.HandleFunc("GET /stats", s.handleStats)

	s.http = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	return s
}

func (s *Server) Start() error {
	return s.http.ListenAndServe()
}

func (s *Server) Stop(ctx context.Context) error {
	return s.http.Shutdown(ctx)
}
