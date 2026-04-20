package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/AbraTM/gork/internal/engine"
	"github.com/AbraTM/gork/internal/job"
	"github.com/AbraTM/gork/internal/server"
)

const defaultAddr = "localhost:8080"

// Demo Handlers
type emailHandler struct{}

func (eh *emailHandler) Handle(ctx context.Context, j job.Job) error {
	fmt.Printf("[email] processing payload: %s\n", string(j.Payload))
	time.Sleep(100 * time.Millisecond)
	return nil
}

type invoiceHandler struct{}

func (eh *invoiceHandler) Handle(ctx context.Context, j job.Job) error {
	fmt.Println("[invoice] processing payload %s\n", string(j.Payload))
	time.Sleep(200 * time.Millisecond)
	return nil
}

func runCmd() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	e := engine.NewEngine(engine.Config{
		IntitalWorkers: 2,
		QueueSize:      1000,
		ScalerConfig: engine.AutoScalerConfig{
			MaxWorkers:     20,
			MinWorkers:     1,
			ScaleUpAt:      10,
			ScaleDownAt:    2,
			EvalInterval:   500 * time.Millisecond,
			CooldownPeriod: 1 * time.Second,
		},
	})

	e.Register("email", &emailHandler{})
	e.Register("invoice", &invoiceHandler{})
	e.Start(ctx)

	s := server.New(defaultAddr, e)
	go func() {
		fmt.Printf("[gork] running on %s\n", defaultAddr)
		if err := s.Start(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("[gork] server error: %v\n", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	fmt.Println("\n[gork] shutting down...")

	shutdownCtx, shutdownCancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer shutdownCancel()

	if err := s.Stop(shutdownCtx); err != nil {
		fmt.Println("failed to properly shutdown the http server")
	}
	cancel()
	e.Stop()

	fmt.Println("[gork] goodbye")
}
