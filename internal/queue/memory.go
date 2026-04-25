package queue

import (
	"context"

	"github.com/AbraTM/gork/internal/job"
)

type InMemoryQueue struct {
	messages chan job.Message
}

var _ Queue = (*InMemoryQueue)(nil)

func NewInMemoryQueue(size int) *InMemoryQueue {
	return &InMemoryQueue{
		messages: make(chan job.Message, size),
	}
}

func (q *InMemoryQueue) Publish(ctx context.Context, j job.Job) error {
	newMessage := job.Message{
		Job:  j,
		Ack:  func() error { return nil },
		Nack: func() error { return nil },
	}
	select {
	case q.messages <- newMessage:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q *InMemoryQueue) Consume(ctx context.Context) (<-chan job.Message, error) {
	return q.messages, nil
}

func (q *InMemoryQueue) Len() int {
	return len(q.messages)
}
