package middleware

import (
	"context"
	"fmt"
	"time"

	"github.com/AbraTM/gork/internal/job"
)

type loggingMiddleware struct {
	next job.Handler
}

func WithLogging(next job.Handler) job.Handler {
	return &loggingMiddleware{next: next}
}

func (m *loggingMiddleware) Handle(ctx context.Context, j job.Job) error {
	startTime := time.Now()
	fmt.Printf("[logging] job=%s type=%s started\n", j.ID, j.Type)

	err := m.next.Handle(ctx, j)
	duration := time.Since(startTime)

	if err != nil {
		fmt.Printf("[logging] job=%s type=%s failed in %v: %n\n", j.ID, j.Type, duration, err)
	} else {
		fmt.Printf("[logging] job=%s type=%s completed in %v\n", j.ID, j.Type, duration)
	}

	return nil
}
