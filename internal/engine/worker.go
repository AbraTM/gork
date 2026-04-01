package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/AbraTM/gork/internal/job"
	"github.com/AbraTM/gork/internal/queue"
)

type Worker struct {
	id       int
	registry job.Registry
	queue    queue.Queue
}

func (w *Worker) Start(ctx context.Context) {
	jobs, err := w.queue.Consume(ctx)
	if err != nil {
		fmt.Printf("[worker-%d] faield to consume jobs, shutting down\n", w.id)
		return
	}

	select {
	case j, ok := <-jobs:
		if !ok {
			fmt.Printf("[worker-%d] queue closed, shutting down\n", w.id)
			return
		}
		w.process(ctx, j)
	case <-ctx.Done():
		fmt.Printf("[worker-%d] context closed, shutting done\n", w.id)
		return
	}
}

func (w *Worker) process(ctx context.Context, j job.Job) {
	start := time.Now()

	handler, err := w.registry.Get(j.Type)
	if err != nil {
		fmt.Printf("[worker-%d] no handler for job type %q: %v\n", w.id, j.Type, err)
		return
	}

	if err := handler.Hanlde(ctx, j); err != nil {
		fmt.Printf("[worker-%d] job %s failed; %v\n", w.id, j.ID, err)
		return
	}

	fmt.Printf("[worker-%d] job %s processed in %v\n", w.id, j.ID, time.Since(start))
}
