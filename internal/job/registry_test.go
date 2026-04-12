package job_test

import (
	"context"
	"sync"
	"testing"

	"github.com/AbraTM/gork/internal/job"
)

type mockHandler struct{}

func (h *mockHandler) Handle(ctx context.Context, j job.Job) error {
	return nil
}

func TestNewRegister(t *testing.T) {
	r := job.NewRegistry()

	if r == nil {
		t.Fatal("NewRegistry() returned nil")
	}
}

func TestRegistry_Register(t *testing.T) {
	r := job.NewRegistry()
	r.Register("email", &mockHandler{})

	h, err := r.Get("email")
	if err != nil {
		t.Fatalf("expected handler, got error: %v", err)
	}
	if h == nil {
		t.Fatal("expected handler, got nil")
	}
}

func TestRegistry_Register_Duplicate_Panics(t *testing.T) {
	r := job.NewRegistry()
	r.Register("email", &mockHandler{})

	defer func() {
		rec := recover()
		if rec == nil {
			t.Error("expected panic on duplicate regsitration, got none")
		}
	}()

	r.Register("email", &mockHandler{})
}

func TestRegistry_Get(t *testing.T) {
	tests := []struct {
		name       string
		registerAs string
		getAs      string
		wantErr    bool
	}{
		{
			name:       "return handler for registered type",
			registerAs: "email",
			getAs:      "email",
			wantErr:    false,
		},
		{
			name:       "return error for unkown type",
			registerAs: "email",
			getAs:      "invoice",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := job.NewRegistry()
			r.Register(tt.registerAs, &mockHandler{})

			h, err := r.Get(tt.getAs)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error, %v\n", err)
			}
			if h == nil {
				t.Fatal("expected handler, got nil")
			}
		})
	}
}

func TestRegistry_ConcurrentGet(t *testing.T) {
	r := job.NewRegistry()
	r.Register("email", &mockHandler{})

	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h, err := r.Get("email")

			if err != nil {
				t.Errorf("unexpected error: %v\n", err)
			}
			if h == nil {
				t.Error("expected handler, got nil")
			}
		}()
	}

	wg.Wait()
}
