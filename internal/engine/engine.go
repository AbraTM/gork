package engine

import (
	"context"
	"fmt"

	"github.com/AbraTM/gork/internal/job"
	"github.com/AbraTM/gork/internal/queue"
)

type Engine struct {
	queue      queue.Queue
	registry   *job.Registry
	pool       *Pool
	autoscaler *AutoScaler
}

type Config struct {
	MaxWorker      int
	MinWorkers     int
	IntitalWorkers int
	QueueSize      int
	ScalerConfig   AutoScalerConfig
}

func NewEngine(c *Config) *Engine {
	q := queue.NewInMemoryQueue(c.QueueSize)
	r := job.NewRegistry()
	p := NewPool(r, q)
	as := NewAutoScaler(p, q, &c.ScalerConfig)

	return &Engine{
		queue:      q,
		registry:   r,
		pool:       p,
		autoscaler: as,
	}
}

func (e *Engine) Register(jobType string, h job.Handler) {
	e.registry.Register(jobType, h)
}

func (e *Engine) Publish(ctx context.Context, j job.Job) error {
	return e.queue.Publish(ctx, j)
}

func (e *Engine) Start(ctx context.Context, c Config) {
	fmt.Println("[engine] starting...")
	e.pool.Start(ctx, c.IntitalWorkers)
	go e.autoscaler.Start(ctx)
	fmt.Println("[engine] ready")
}

func (e *Engine) Stop() {
	fmt.Println("[engine] shutting down")
	e.pool.Stop()
	fmt.Println("[engine] shutdown complete")
}
