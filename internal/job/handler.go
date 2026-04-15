package job

import (
	"context"
)

type Handler interface {
	Handle(ctx context.Context, job Job) error
}

type HandlerFunc func(ctx context.Context, j Job) error

func (f HandlerFunc) Handle(ctx context.Context, j Job) error {
	return f(ctx, j)
}
