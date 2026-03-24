package job

import "context"

type Handler interface {
	Hanlde(ctx context.Context, job Job) error
}
