package middleware

import (
	"context"
	"fmt"

	"github.com/AbraTM/gork/internal/job"
)

type recoveryMiddleware struct {
	next job.Handler
}

func WithRecovery(next job.Handler) job.Handler {
	return &recoveryMiddleware{next: next}
}

func (m *recoveryMiddleware) Handle(ctx context.Context, j job.Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("[recovery] job=%s panicked: %v\n", j.ID, r)
			err = fmt.Errorf("panic: %v", r)
		}
	}()

	return m.next.Handle(ctx, j)
}
