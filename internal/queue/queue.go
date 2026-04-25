package queue

import (
	"context"
	"github.com/AbraTM/gork/internal/job"
)

type Queue interface {
	Publish(ctx context.Context, j job.Job) error
	Consume(ctx context.Context) (<-chan job.Message, error)
	Len() int
}
