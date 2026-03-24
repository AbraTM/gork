# gork ⚙️

> An auto-scaling background job processing engine written in Go.

`gork` is a project built to explore Go's concurrency model — goroutines, channels, and dynamic worker scaling — through a real-world system design lens.

Inspired by [Sidekiq](https://sidekiq.org/), [Celery](https://docs.celeryq.dev/), and the [Kubernetes HPA](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/).

---

## What I'm Building & Learning

This project is a hands-on exploration of:

- Idiomatic Go concurrency (goroutines, channels, `select`, `context`)
- Dynamic worker pool scaling
- Plugin-style handler architecture via interfaces
- Backpressure, failure handling, and graceful shutdown
- Observability-driven system design

---

## Architecture

```
        CLI / API
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
         AutoScaler ─────────────┘
            │
            ▼
          Metrics
```

---

## Core Concepts

### Job
A unit of work with a type, payload, and metadata.

```go
type Job struct {
    ID        string
    Type      string
    Payload   []byte
    CreatedAt time.Time
    Retries   int
}
```

### Handler (Plugin System)
Each job type maps to a handler via a simple interface. Any type that implements `Handle` is a valid handler — no explicit registration needed.

```go
type Handler interface {
    Handle(ctx context.Context, job Job) error
}
```

### Registry
Maps job types to their handlers at runtime.

```go
registry.Register("email", &EmailHandler{})
```

### Worker Pool
Executes jobs concurrently using goroutines. Pool size is controlled dynamically by the autoscaler.

### AutoScaler
Continuously evaluates system state and adjusts worker count based on:
- Queue backlog
- Worker utilization
- Cooldown periods
- Configured min/max bounds

### Queue Abstraction
```go
type Queue interface {
    Publish(ctx context.Context, job Job) error
    Consume(ctx context.Context) (<-chan Job, error)
}
```

Currently implemented: **in-memory**. Planned: RabbitMQ, Kafka.

---

## Project Structure

```
gork/
├── cmd/
│   └── gork/
│       └── main.go           # CLI entrypoint
│
├── internal/
│   ├── engine/
│   │   ├── engine.go         # Core orchestration
│   │   ├── worker.go         # Worker logic
│   │   ├── pool.go           # Worker pool management
│   │   └── autoscaler.go     # Scaling logic
│   │
│   ├── job/
│   │   ├── job.go            # Job struct
│   │   ├── handler.go        # Handler interface
│   │   └── registry.go       # Handler registry
│   │
│   ├── queue/
│   │   ├── queue.go          # Queue interface
│   │   └── memory.go         # In-memory implementation
│   │
│   ├── middleware/
│   │   ├── logging.go
│   │   ├── retry.go
│   │   └── recovery.go
│   │
│   └── metrics/
│       └── metrics.go
│
└── pkg/
    └── logger/
```

---

## CLI Usage

```bash
# Start the engine
gork run --min-workers=2 --max-workers=10

# Enqueue a job
gork enqueue --type=email --payload='{"to":"user@example.com"}'

# View stats
gork stats
```

Example stats output:

```
Queue:     42
Workers:   6
Processed: 1200
Failed:    12
```

---

## Autoscaling Behavior

| Condition | Action |
|---|---|
| Queue grows beyond threshold | Scale up workers |
| Queue drains to empty | Scale down workers |
| Scaling event just occurred | Respect cooldown period |
| Always | Respect min/max worker bounds |

Example log:
```
[autoscaler] queue=120 workers=4 → scaling to 8
[worker-3] processed job=abc123 in 120ms
```

---

## Middleware

Handlers can be wrapped with composable middleware:

- **Logging** — records job execution and timing
- **Retry** — exponential backoff on failure
- **Recovery** — catches panics, prevents worker crashes
- **Metrics** — tracks latency and failure rates

---

## Concurrency Guarantees

- Bounded worker pool — no unbounded goroutine spawning
- Context-driven cancellation throughout
- Graceful shutdown — in-flight jobs complete before exit
- Race-condition-free scaling

---

## Roadmap

- [x] In-memory queue
- [x] Worker pool with dynamic scaling
- [x] Pluggable handler registry
- [x] Middleware support
- [ ] RabbitMQ integration
- [ ] Kafka support
- [ ] Prometheus metrics endpoint
- [ ] Grafana dashboards
- [ ] Priority queues
- [ ] Delayed / scheduled jobs
- [ ] Persistent job storage (Postgres)
- [ ] Distributed workers (multi-node)

---

## Key Concepts This Project Covers

| Concept | Where It Appears |
|---|---|
| Goroutines | Worker pool execution |
| Channels | Job dispatch, queue consumption |
| `select` | AutoScaler control loop |
| `sync.WaitGroup` | Graceful shutdown |
| `context` | Cancellation, timeouts |
| Interfaces | Handler, Queue abstractions |
| Middleware pattern | Handler wrapping |

---

## Contributing

Contributions and feedback welcome. Please ensure:

- Idiomatic Go code
- Proper `context` usage
- No goroutine leaks
- Tests for new features
