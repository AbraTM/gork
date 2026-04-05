package engine

import (
	"context"
	"fmt"
	"sync"

	"github.com/AbraTM/gork/internal/job"
	"github.com/AbraTM/gork/internal/queue"
)

type Pool struct {
	registry      *job.Registry
	queue         queue.Queue
	workerCount   int
	workerContext context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	mu            sync.Mutex
}

func NewPool(r *job.Registry, q queue.Queue) *Pool {
	return &Pool{
		registry: r,
		queue:    q,
	}
}

func (p *Pool) Start(ctx context.Context, count int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	workerContext, cancel := context.WithCancel(ctx)
	p.workerContext = workerContext
	p.cancel = cancel

	for i := 0; i < count; i++ {
		p.startWorker(p.workerContext, i)
	}

	p.workerCount = count
	fmt.Printf("[pool] started %d workers\n", count)
}

func (p *Pool) startWorker(ctx context.Context, id int) {
	w := NewWorker(id, p.registry, p.queue)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		w.Start(ctx)
	}()
}

func (p *Pool) Stop() {
	fmt.Println("[pool] stopping all workers...")
	p.cancel()
	p.wg.Wait()
	fmt.Println("[pool] all workers stopped")
}

func (p *Pool) Scale(ctx context.Context, target int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	current := p.workerCount

	if target > current {
		diff := target - current
		fmt.Printf("[pool] scaling up from %d workers -> %d workers \n", current, target)
		for i := 0; i < diff; i++ {
			p.startWorker(p.workerContext, current+i)
		}
		p.workerCount = target
	} else {
		fmt.Printf("[pool] is scaling down from %d workers -> %d workers\n", current, target)
	}
}

func (p *Pool) WorkerCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.workerCount
}
