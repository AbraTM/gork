package middleware

import (
	"context"
	"fmt"

	"github.com/AbraTM/gork/internal/job"
)

type loggingMiddleware struct {
	next job.Handler
}

func WithLogging(next job.Handler) job.Handler {
	return &loggingMiddleware{next: next}
}

func (m *loggingMiddleware) Handle(ctx context.Context, j job.Job) error {
	fmt.Printf("[logging] job=%s type=%s payload=%s started\n", j.ID, j.Type, string(j.Payload))
	err := m.next.Handle(ctx, j)
	if err != nil {
		fmt.Printf("[logging] job=%s type=%s failed: %v\n", j.ID, j.Type, err)
	}
	return err
}
