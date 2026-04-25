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
	registry *job.Registry
	queue    queue.Queue
}

func NewWorker(id int, r *job.Registry, q queue.Queue) *Worker {
	return &Worker{
		id:       id,
		registry: r,
		queue:    q,
	}
}

func (w *Worker) Start(ctx context.Context) {
	messages, err := w.queue.Consume(ctx)
	if err != nil {
		fmt.Printf("[worker-%d] failed to consume jobs, shutting down\n", w.id)
		return
	}

	for {
		select {
		case m, ok := <-messages:
			if !ok {
				fmt.Printf("[worker-%d] queue closed, shutting down\n", w.id)
				return
			}
			w.process(ctx, m)
		case <-ctx.Done():
			fmt.Printf("[worker-%d] context closed, shutting down\n", w.id)
			return
		}
	}
}

func (w *Worker) process(ctx context.Context, m job.Message) {
	start := time.Now()

	handler, err := w.registry.Get(m.Job.Type)
	if err != nil {
		fmt.Printf("[worker-%d] no handler for job type %q: %v\n", w.id, m.Job.Type, err)
		if err := m.Nack(); err != nil {
			fmt.Printf("[worker-%d] failed to nack job %s: %v\n", w.id, m.Job.ID, err)
		}
		return
	}

	if err := handler.Handle(ctx, m.Job); err != nil {
		fmt.Printf("[worker-%d] job %s failed; %v\n", w.id, m.Job.ID, err)
		if err := m.Nack(); err != nil {
			fmt.Printf("[worker-%d] failed to nack job %s: %v\n", w.id, m.Job.ID, err)
		}
		return
	}

	if err := m.Ack(); err != nil {
		fmt.Printf("[worker-%d] failed to ack job %s: %v\n", w.id, m.Job.ID, err)
		return
	}
	fmt.Printf("[worker-%d] job %s processed in %v\n", w.id, m.Job.ID, time.Since(start))
}
