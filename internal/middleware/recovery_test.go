package middleware_test

import (
	"context"
	"testing"

	"github.com/AbraTM/gork/internal/job"
	"github.com/AbraTM/gork/internal/middleware"
)

type mockHandlerPanic struct {
	panic bool
}

func (mh *mockHandlerPanic) Handle(ctx context.Context, j job.Job) error {
	if mh.panic {
		panic("dummy panic")
	}
	return nil
}

func TestWithRecovery(t *testing.T) {
	handler := middleware.WithRecovery(&mockHandlerPanic{})

	if handler == nil {
		t.Fatal("expected handler with recovery, got nil")
	}
}

func TestWithRecovery_Panic(t *testing.T) {
	handler := middleware.WithRecovery(&mockHandlerPanic{panic: true})
	ctx := context.Background()

	if err := handler.Handle(ctx, newTestJob()); err == nil {
		t.Fatal("expected error, got nil")
	}
}
