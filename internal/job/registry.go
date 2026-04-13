package job

import (
	"fmt"
	"sync"
)

type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[string]Handler),
	}
}

func (r *Registry) Register(jobType string, h Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.handlers[jobType]; exists {
		panic(fmt.Sprintf("handler already exists for job type %q", jobType))
	}

	r.handlers[jobType] = h
}

func (r *Registry) Get(jobType string) (Handler, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	h, ok := r.handlers[jobType]
	if !ok {
		return nil, fmt.Errorf("no handler registered for job type %q", jobType)
	}
	return h, nil
}
