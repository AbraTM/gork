package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"

	"github.com/AbraTM/gork/internal/job"
	"github.com/AbraTM/gork/internal/queue"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQQueue struct {
	conn          *amqp.Connection
	publishChan   *amqp.Channel
	publishMu     sync.Mutex
	queueName     string
	managementURL string
	lastLen       int
	lastLenMu     sync.RWMutex
}

var _ queue.Queue = (*RabbitMQQueue)(nil)

type closer struct {
	name string
	fn   func() error
}

type queueStats struct {
	Messages int `json:"messages"`
}

func closeAll(closers ...closer) {
	for _, c := range closers {
		if err := c.fn(); err != nil {
			slog.Warn("failed to close resource during cleanup",
				"resource", c.name,
				"error", err,
			)
		}
	}
}

func NewRabbitMQQueue(amqpURL, managementURL, queueName string) (*RabbitMQQueue, error) {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	slog.Info("connected to RabbitMQ", "url", amqpURL)

	publishChan, err := conn.Channel()
	if err != nil {
		closeAll(
			closer{"connection", conn.Close},
		)
		return nil, fmt.Errorf("failed to open publish channel: %w", err)
	}

	_, err = publishChan.QueueDeclare(
		queueName,
		true, false, false, false, nil,
	)

	if err != nil {
		closeAll(
			closer{"connection", conn.Close},
			closer{"publish_channel", publishChan.Close},
		)
		return nil, fmt.Errorf("failed to declare queue %q: %w", queueName, err)
	}
	slog.Info("queue declared", "queue", queueName)

	return &RabbitMQQueue{
		conn:          conn,
		publishChan:   publishChan,
		queueName:     queueName,
		managementURL: managementURL,
	}, nil
}

func (q *RabbitMQQueue) Close() error {
	if err := q.publishChan.Close(); err != nil {
		return fmt.Errorf("failed to close publish channel: %w", err)
	}
	if err := q.conn.Close(); err != nil {
		return fmt.Errorf("failed to close connection: %w", err)
	}
	slog.Info("RabbitMQ connection closed")

	return nil
}

func (q *RabbitMQQueue) Publish(ctx context.Context, j job.Job) error {
	body, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("failed to marshal json: %w", err)
	}
	q.publishMu.Lock()
	defer q.publishMu.Unlock()

	err = q.publishChan.PublishWithContext(
		ctx, "", q.queueName, false, false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish job %q: %w", j.ID, err)
	}

	slog.Debug("job published", "job_id", j.ID, "type", j.Type)
	return nil
}

func (q *RabbitMQQueue) Consume(ctx context.Context) (<-chan job.Message, error) {
	ch, err := q.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open consumer channel: %w", err)
	}

	if err := ch.Qos(1, 0, false); err != nil {
		if closeErr := ch.Close(); closeErr != nil {
			slog.Warn("faile to close channel", "error", closeErr)
		}
		return nil, fmt.Errorf("failed to set QoS: %w", err)
	}

	deliveries, err := ch.Consume(
		q.queueName, "", false, false, false, false, nil,
	)
	if err != nil {
		if closeErr := ch.Close(); closeErr != nil {
			slog.Warn("faile to close channel", "error", closeErr)
		}
		return nil, fmt.Errorf("failed to start consuming: %w", err)
	}

	messages := make(chan job.Message)

	go func() {
		defer close(messages)
		for {
			select {
			case d, ok := <-deliveries:
				if !ok {
					slog.Info("delivery channel closed")
					return
				}

				var j job.Job
				if err := json.Unmarshal(d.Body, &j); err != nil {
					slog.Error("failed to unmarshal job", "error", err)
					if err := d.Nack(false, false); err != nil {
						slog.Debug("failed to send nack")
					}
					continue
				}

				messages <- job.Message{
					Job:  j,
					Ack:  func() error { return d.Ack(false) },
					Nack: func() error { return d.Nack(false, true) },
				}
			case <-ctx.Done():
				slog.Info("consumer stopping")
				return
			}
		}
	}()

	slog.Info("consuming from queue", "queue", q.queueName)
	return messages, nil
}

func (q *RabbitMQQueue) Len() int {
	url := fmt.Sprintf("%s/api/queues/%%2F/%s", q.managementURL, q.queueName)

	resp, err := http.Get(url)
	if err != nil {
		slog.Warn("failed to query management API", "error", err)
		q.lastLenMu.RLock()
		defer q.lastLenMu.RUnlock()
		return q.lastLen
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			slog.Warn("failed to close response body", "error", err)
		}
	}()

	var stats queueStats
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		slog.Warn("failed to decode queue stats", "error", err)
		q.lastLenMu.RLock()
		defer q.lastLenMu.RUnlock()
		return q.lastLen
	}

	q.lastLenMu.Lock()
	q.lastLen = stats.Messages
	q.lastLenMu.Unlock()

	return stats.Messages
}
