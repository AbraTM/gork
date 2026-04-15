package engine

import (
	"context"
	"fmt"
	"sync"

	"github.com/AbraTM/gork/internal/job"
	"github.com/AbraTM/gork/internal/queue"
)

type workerEntry struct {
	id     int
	cancel context.CancelFunc
}

type Pool struct {
	registry     *job.Registry
	queue        queue.Queue
	workers      []workerEntry
	poolContext  context.Context
	poolCancel   context.CancelFunc
	wg           sync.WaitGroup
	mu           sync.Mutex
	nextWorkerID int
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

	if p.poolContext != nil {
		fmt.Print("[pool] alredy started")
		return
	}

	p.poolContext, p.poolCancel = context.WithCancel(ctx)
	p.workers = make([]workerEntry, 0, count)

	for range count {
		p.startWorker()
	}

	fmt.Printf("[pool] started %d workers\n", count)
}

func (p *Pool) startWorker() {
	id := p.nextWorkerID
	p.nextWorkerID++

	workerContext, workerCancel := context.WithCancel(p.poolContext)
	p.workers = append(p.workers, workerEntry{
		id:     id,
		cancel: workerCancel,
	})

	w := NewWorker(id, p.registry, p.queue)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		w.Start(workerContext)
	}()
}

func (p *Pool) Stop() {
	fmt.Println("[pool] stopping all workers...")
	p.poolCancel()
	p.wg.Wait()
	fmt.Println("[pool] all workers stopped")
}

func (p *Pool) Scale(target int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	current := len(p.workers)
	if target > current {
		diff := target - current
		fmt.Printf("[pool] scaling up from %d workers -> %d workers \n", current, target)
		for range diff {
			p.startWorker()
		}
	} else {
		excess := current - target
		fmt.Printf("[pool] is scaling down from %d workers -> %d workers\n", current, target)
		for range excess {
			entry := p.workers[len(p.workers)-1]
			entry.cancel()
			p.workers = p.workers[:len(p.workers)-1]
		}
	}
}

func (p *Pool) WorkerCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return len(p.workers)
}
