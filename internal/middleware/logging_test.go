package middleware_test

import (
	"context"
	"testing"

	"github.com/AbraTM/gork/internal/job"
	"github.com/AbraTM/gork/internal/middleware"
)

type mockHandlerLogging struct{}

func (mh *mockHandlerLogging) Handle(ctx context.Context, j job.Job) error {
	return nil
}

func TestWithLogging(t *testing.T) {
	handler := middleware.WithLogging(&mockHandlerLogging{})

	if handler == nil {
		t.Fatal("expected handler with logging, got nil")
	}
}
