package queue

import (
	"context"

	"github.com/AbraTM/gork/internal/job"
)

type InMemoryQueue struct {
	jobs chan job.Job
}

var _ Queue = (*InMemoryQueue)(nil)

func NewInMemoryQueue(size int) *InMemoryQueue {
	return &InMemoryQueue{
		jobs: make(chan job.Job, size),
	}
}

func (q *InMemoryQueue) Publish(ctx context.Context, j job.Job) error {
	select {
	case q.jobs <- j:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q *InMemoryQueue) Consume(ctx context.Context) (<-chan job.Job, error) {
	return q.jobs, nil
}
