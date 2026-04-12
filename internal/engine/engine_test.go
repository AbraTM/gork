package engine_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/AbraTM/gork/internal/engine"
	"github.com/AbraTM/gork/internal/job"
)

const testJobCount = 10

type trackingHandler struct {
	mu    sync.Mutex
	count int
}

func (th *trackingHandler) Handle(ctx context.Context, j job.Job) error {
	th.mu.Lock()
	defer th.mu.Unlock()
	th.count++
	return nil
}

func (th *trackingHandler) Count() int {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.count
}

func newTestEngineConfig() engine.Config {
	return engine.Config{
		IntitalWorkers: 2,
		QueueSize:      100,
		ScalerConfig: engine.AutoScalerConfig{
			MaxWorkers:     5,
			MinWorkers:     1,
			ScaleUpAt:      50,
			ScaleDownAt:    5,
			EvalInterval:   100 * time.Millisecond,
			CooldownPeriod: 100 * time.Millisecond,
		},
	}
}

func newTestJobFactory() func() job.Job {
	id := 0

	return func() job.Job {
		id++

		return job.Job{
			ID:      fmt.Sprintf("test-job-%d", id),
			Type:    "email",
			Payload: []byte("user18@somemail.com"),
		}
	}
}

func waitForJobs(t *testing.T, e *engine.Engine, th *trackingHandler, expected int) {
	t.Helper()
	deadline := time.After(5 * time.Second)

	for {
		select {
		case <-deadline:
			t.Fatalf("timed out: processed %d/%d jobs", th.Count(), expected)
		default:
			if th.Count() >= expected {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestNewEngine(t *testing.T) {
	e := engine.NewEngine(newTestEngineConfig())

	if e == nil {
		t.Fatal("expected engine, got nil")
	}
}

func TestEngine(t *testing.T) {
	handler := &trackingHandler{}
	factory := newTestJobFactory()

	e := engine.NewEngine(newTestEngineConfig())
	e.Register("email", handler)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	e.Start(ctx)

	for range testJobCount {
		if err := e.Publish(ctx, factory()); err != nil {
			t.Fatalf("unexpected publish error, %v\n", err)
		}
	}

	waitForJobs(t, e, handler, testJobCount)
	e.Stop()
}

func TestEngine_Context_Cancel(t *testing.T) {
	handler := &trackingHandler{}
	factory := newTestJobFactory()

	cfg := newTestEngineConfig()
	cfg.QueueSize = 1
	e := engine.NewEngine(cfg)
	e.Register("email", handler)

	ctx, cancel := context.WithCancel(context.Background())
	e.Start(ctx)

	cancel()

	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for publish to fail")
		default:
			if err := e.Publish(ctx, factory()); err != nil {
				e.Stop()
				return
			}
			time.Sleep(1 * time.Millisecond)
		}
	}
}
