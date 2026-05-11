package main

import (
	"context"
	"fmt"
	// "sync"
	"time"

	"github.com/AbraTM/gork/internal/engine"
	"github.com/AbraTM/gork/internal/job"
	"github.com/AbraTM/gork/internal/queue"
	"github.com/AbraTM/gork/internal/queue/rabbitmq"
)

const (
	totalJobs  = 200
	jobLatency = 500 * time.Millisecond
)

type BenchmarkConfig struct {
	name         string
	factory      func() engine.Config
	queueFactory func() (queue.Queue, error)
}

var benchmarks = []BenchmarkConfig{
	// {
	// 	name: "Synchronous (1 worker, no scaling)",
	// 	factory: func() engine.Config {
	// 		return engine.Config{
	// 			IntitalWorkers: 1,
	// 			QueueSize:      1000,
	// 			ScalerConfig: engine.AutoScalerConfig{
	// 				MaxWorkers:     1,
	// 				MinWorkers:     1,
	// 				ScaleUpAt:      99999,
	// 				ScaleDownAt:    0,
	// 				EvalInterval:   1 * time.Second,
	// 				CooldownPeriod: 1 * time.Second,
	// 			},
	// 		}
	// 	},
	// },
	// {
	// 	name: "Fixed Pool (5 workers, no scaling)",
	// 	factory: func() engine.Config {
	// 		return engine.Config{
	// 			IntitalWorkers: 5,
	// 			QueueSize:      1000,
	// 			ScalerConfig: engine.AutoScalerConfig{
	// 				MaxWorkers:     5,
	// 				MinWorkers:     5,
	// 				ScaleUpAt:      99999,
	// 				ScaleDownAt:    0,
	// 				EvalInterval:   1 * time.Second,
	// 				CooldownPeriod: 1 * time.Second,
	// 			},
	// 		}
	// 	},
	// },
	// {
	// 	name: "AutoScaler InMemory (1→20 workers, HPA-style)",
	// 	factory: func() engine.Config {
	// 		return engine.Config{
	// 			IntitalWorkers: 1,
	// 			QueueSize:      1000,
	// 			ScalerConfig: engine.AutoScalerConfig{
	// 				MaxWorkers:     20,
	// 				MinWorkers:     1,
	// 				ScaleUpAt:      10,
	// 				ScaleDownAt:    2,
	// 				EvalInterval:   500 * time.Millisecond,
	// 				CooldownPeriod: 1 * time.Second,
	// 			},
	// 		}
	// 	},
	// },
	{
		name: "AutoSclaer RabbitMQ (1->20 workers, HPA-style)",
		factory: func() engine.Config {
			return engine.Config{
				IntitalWorkers: 1,
				ScalerConfig: engine.AutoScalerConfig{
					MaxWorkers:     20,
					MinWorkers:     1,
					ScaleUpAt:      10,
					ScaleDownAt:    2,
					EvalInterval:   500 * time.Millisecond,
					CooldownPeriod: 1 * time.Second,
				},
			}
		},
		queueFactory: func() (queue.Queue, error) {
			return rabbitmq.NewRabbitMQQueue(
				"amqp://guest:guest@localhost:5672/",
				"http://guest:guest@127.0.0.1:15672",
				"gork.benchmark",
			)
		},
	},
}

type EmailHanlder struct{}

func (*EmailHanlder) Handle(ctx context.Context, j job.Job) error {
	time.Sleep(jobLatency)
	return nil
}

type result struct {
	name       string
	elapsed    time.Duration
	throughput float64
}

func run(cfg BenchmarkConfig) result {
	fmt.Println("=========================================")
	fmt.Printf("%-10s\n", cfg.name)
	fmt.Println("=========================================")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var e *engine.Engine
	if cfg.queueFactory != nil {
		q, err := cfg.queueFactory()
		if err != nil {
			fmt.Printf("failed to create a queue: %v\n", err)
			fmt.Println("is RabbitMQ running? try: make rabbitmq")
			return result{name: cfg.name}
		}
		if closer, ok := q.(interface{ Close() error }); ok {
			defer func() {
				if err := closer.Close(); err != nil {
					fmt.Printf("error closing the queue: %v\n", err)
				}
			}()
		}
		e = engine.NewEngineWithQueue(q, cfg.factory())
	} else {
		e = engine.NewEngine(cfg.factory())
	}

	e.Register("email", &EmailHanlder{})
	e.Start(ctx)

	start := time.Now()

	for i := range totalJobs {
		err := e.Publish(ctx, job.Job{
			ID:        fmt.Sprintf("job-%d", i),
			Type:      "email",
			Payload:   fmt.Appendf(make([]byte, 0, 32), "user%d@somemail.com", i),
			CreatedAt: time.Now(),
		})

		if err != nil {
			fmt.Printf("error while publishing, %v\n", err)
		}
	}

	for e.QueueLen() > 0 {
		time.Sleep(100 * time.Millisecond)
	}

	time.Sleep(jobLatency + 30*time.Second)
	e.Stop()

	elapsed := time.Since(start)
	throughput := float64(totalJobs) / elapsed.Seconds()
	fmt.Printf("[benchmark] completed in %v\n", elapsed)
	fmt.Printf("[benchmark] throughput: %.1f jobs/sec\n", throughput)

	return result{
		name:       cfg.name,
		elapsed:    elapsed,
		throughput: throughput,
	}
}

func main() {
	fmt.Println("***** gork benchmarking *****")
	fmt.Printf("Jobs: %d | Job Latency: %v\n", totalJobs, jobLatency)

	results := make([]result, 0, len(benchmarks))

	for _, cfg := range benchmarks {
		results = append(results, run(cfg))
	}

	// Summary Table
	fmt.Println("\n======= SUMMARY ========")
	fmt.Printf("%-40s %-12s %-12s\n", "Config", "Time", "Jobs/sec")
	fmt.Println("-----------------------------------------------------------")
	for _, r := range results {
		fmt.Printf("%-40s %-12v %-12.1f\n", r.name, r.elapsed.Round(time.Millisecond), r.throughput)
	}
}
