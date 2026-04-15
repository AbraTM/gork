package engine

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/AbraTM/gork/internal/queue"
)

type AutoScalerConfig struct {
	MaxWorkers     int
	MinWorkers     int
	ScaleUpAt      int
	ScaleDownAt    int
	EvalInterval   time.Duration
	CooldownPeriod time.Duration
}

type AutoScaler struct {
	pool         *Pool
	queue        queue.Queue
	config       *AutoScalerConfig
	lastScaledAt time.Time
}

func NewAutoScaler(p *Pool, q queue.Queue, c *AutoScalerConfig) *AutoScaler {
	return &AutoScaler{
		pool:         p,
		queue:        q,
		config:       c,
		lastScaledAt: time.Time{},
	}
}

func (as *AutoScaler) Start(ctx context.Context) {
	ticker := time.NewTicker(as.config.EvalInterval)
	defer ticker.Stop()
	fmt.Println("[autoscaler] started")

	for {
		select {
		case <-ticker.C:
			fmt.Println("[autoscaler] evaluating load state")
			as.evaluate(ctx)
		case <-ctx.Done():
			fmt.Println("[autoscaler] shutting down")
			return
		}
	}
}

func (as *AutoScaler) evaluate(ctx context.Context) {
	// Method that gets metrics
	currQueue := as.queue.Len()
	currWorkers := as.pool.WorkerCount()

	fmt.Printf("[autoscaler] current status jobs queued: %d, workers deployed: %d\n", currQueue, currWorkers)

	if time.Since(as.lastScaledAt) < as.config.CooldownPeriod {
		fmt.Println("[autoscaler] scaling under cooldown")
		return
	}

	// Scale up
	if currQueue > as.config.ScaleUpAt && currWorkers < as.config.MaxWorkers {
		ratio := float64(currQueue) / float64(as.config.ScaleUpAt)
		scaleTo := min(int(math.Ceil(float64(currWorkers)*ratio)), as.config.MaxWorkers)
		fmt.Printf("[autoscaler] scaling up workers from %d workers -> %d workers\n", currWorkers, scaleTo)
		as.pool.Scale(scaleTo)
		as.lastScaledAt = time.Now()
		return
	}

	// Scale down
	if currQueue < as.config.ScaleDownAt && currWorkers > as.config.MinWorkers {
		ratio := float64(currQueue) / float64(as.config.ScaleDownAt)
		scaleTo := max(int(math.Ceil(float64(currWorkers)*ratio)), as.config.MinWorkers)
		fmt.Printf("[autoscaler] scaling down workers from %d workers -> %d workers\n", currWorkers, scaleTo)
		as.pool.Scale(scaleTo)
		as.lastScaledAt = time.Now()
		return
	}
}
