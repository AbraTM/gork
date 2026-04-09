package main

import (
	"context"
	"fmt"
	"time"

	"github.com/AbraTM/gork/internal/engine"
	"github.com/AbraTM/gork/internal/job"
)

type EmailHanlder struct{}

func (*EmailHanlder) Handle(ctx context.Context, job job.Job) error {
	fmt.Printf("[email] sending email to %s\n", string(job.Payload))
	time.Sleep(500 * time.Millisecond)
	return nil
}

func main() {
	fmt.Println("***** gork *****")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	e := engine.NewEngine(engine.Config{
		IntitalWorkers: 1,
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

	e.Register("email", &EmailHanlder{})
	e.Start(ctx)

	for i := 0; i <= 200; i++ {
		e.Publish(ctx, job.Job{
			ID:        fmt.Sprintf("job-%d", i),
			Type:      "email",
			Payload:   fmt.Appendf(make([]byte, 0, 32), "user%d@somemail.com", i),
			CreatedAt: time.Now(),
		})
	}

	for e.QueueLen() > 0 {
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(600 * time.Millisecond)
	e.Stop()
}
