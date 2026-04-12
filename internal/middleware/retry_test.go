package middleware_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/AbraTM/gork/internal/job"
	"github.com/AbraTM/gork/internal/middleware"
)

const testMaxRetries = 5
const testBaseDelay = 1 * time.Millisecond

type mockHandlerRetry struct {
	attempts  int
	failTimes int
	err       error
}

func (mh *mockHandlerRetry) Handle(ctx context.Context, j job.Job) error {
	mh.attempts++
	if mh.attempts <= mh.failTimes {
		return mh.err
	}
	return nil
}

func TestWithRetry(t *testing.T) {
	handler := middleware.WithRetry(&mockHandlerRetry{}, testMaxRetries, testBaseDelay)
	if handler == nil {
		t.Fatal("expected handler with retries, got nil")
	}
}

func TestWithRetry_Fail_Once_And_Succeed(t *testing.T) {
	handler := &mockHandlerRetry{
		failTimes: 1,
		err:       fmt.Errorf("dummy error"),
	}

	handlerWithRetry := middleware.WithRetry(
		handler,
		testMaxRetries,
		testBaseDelay,
	)

	ctx := context.Background()

	err := handlerWithRetry.Handle(ctx, newTestJob())
	if err != nil {
		t.Fatalf("unexpected err, %v\n", err)
	}
}

func TestWithRetry_Fail_All(t *testing.T) {
	handler := &mockHandlerRetry{
		failTimes: testMaxRetries + 1,
		err:       fmt.Errorf("dummy error"),
	}

	handlerWithRetry := middleware.WithRetry(
		handler,
		testMaxRetries,
		testBaseDelay,
	)

	ctx := context.Background()

	err := handlerWithRetry.Handle(ctx, newTestJob())

	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestWithRetry_Fail_Context_Close(t *testing.T) {
	handler := &mockHandlerRetry{
		failTimes: 2,
		err:       fmt.Errorf("dummy error"),
	}

	handlerWithRetry := middleware.WithRetry(
		handler,
		testMaxRetries,
		testBaseDelay,
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := handlerWithRetry.Handle(ctx, newTestJob())

	if err == nil {
		t.Fatal("expected error got nil")
	}

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v\n", err)
	}
}
