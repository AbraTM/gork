# gork 

> Auto-scaling background job processing engine written in Go.


`gork` is a pluggable, auto-scaling job processing engine inspired by [Sidekiq](https://sidekiq.org/), [Celery](https://docs.celeryq.dev/), and the [Kubernetes HPA](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/). Built on idiomatic Go concurrency primitives - goroutines, channels, and context-driven lifecycle management.

---

## Benchmark

200 jobs × 500ms latency on the same hardware:

| Mode | Time | Throughput | vs Baseline |
|---|---|---|---|
| Synchronous (1 worker) | 1m 40s | 2.0 jobs/sec | baseline |
| Fixed Pool (5 workers) | 20s | 9.9 jobs/sec | 5x |
| AutoScaler (1→20 workers) | 5.6s | 35.6 jobs/sec | **17.8x** |

Run the benchmark yourself:
```bash
go run cmd/benchmark/main.go
```

---

## Architecture

```
        Producer
            │
            ▼
        Queue Layer
   (in-memory / RabbitMQ)
            │
            ▼
      Worker Pool  ◄────────────┐
            │                   │
            ▼                   │
     Handler Registry           │
            │                   │
            ▼                   │
      Job Execution             │
                                │
         AutoScaler ────────────┘
```

### How It Works

1. **Producer** publishes jobs to the queue
2. **Workers** consume jobs from the queue concurrently
3. **Registry** maps each job type to its handler
4. **Handler** executes the job logic
5. **AutoScaler** continuously evaluates queue depth and adjusts worker count proportionally

---

## Features

- **Proportional autoscaling**: scales workers based on queue/threshold ratio, modelled after Kubernetes HPA
- **Plugin-style handlers**: any type implementing `Handle(ctx, job) error` is a valid handler
- **Composable middleware**: wrap handlers with logging, retry, and panic recovery
- **Context-driven lifecycle**: cancellation propagates through every component
- **Graceful shutdown**: in-flight jobs complete before exit
- **Race-condition-free**: verified with Go's built-in race detector

---

## Getting Started

### Prerequisites

```bash
go 1.24
```

### Install

```bash
git clone https://github.com/AbraTM/gork.git
cd gork
go mod download
```

## Testing

```bash
# run all tests
go test ./...

# with race detector (recommended)
go test -race ./...

# with coverage
go test -race -coverprofile=coverage.out ./...
go tool cover -func=coverage.out
```
20 tests across all packages, race detector clean.

---

## CI

Every push and pull request runs:

| Check | Tool |
|---|---|
| Static analysis | `go vet` |
| Unit + integration tests | `go test -race` |
| Code quality | `golangci-lint` |
| Benchmark | `go run cmd/benchmark/main.go` |

---

## Contributing

Contributions welcome. Please ensure:

- Idiomatic Go code
- Proper `context` usage throughout
- No goroutine leaks
- Tests for new features, race detector clean
