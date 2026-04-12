package middleware

import (
	"context"
	"fmt"
	"time"

	"github.com/AbraTM/gork/internal/job"
)

type retryMiddleware struct {
	next       job.Handler
	maxRetries int
	baseDelay  time.Duration
}

func WithRetry(next job.Handler, maxRetries int, baseDelay time.Duration) job.Handler {
	return &retryMiddleware{
		next:       next,
		maxRetries: maxRetries,
		baseDelay:  baseDelay,
	}
}

func (m *retryMiddleware) Handle(ctx context.Context, j job.Job) error {
	var err error
	for attempt := 0; attempt < m.maxRetries; attempt++ {
		if attempt > 0 {
			delay := m.baseDelay * time.Duration(attempt)
			fmt.Printf("[retry] job=%s attempt=%d waiting %v\n", j.ID, attempt, delay)

			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		err = m.next.Handle(ctx, j)
		if err == nil {
			return nil
		}

		fmt.Printf("[retry] job=%s attempt=%d failed: %v\n", j.ID, attempt, err)
	}

	return fmt.Errorf("job %s failed after %d attempts: %w", j.ID, m.maxRetries, err)
}
