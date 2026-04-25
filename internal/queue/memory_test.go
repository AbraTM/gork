package queue_test

import (
	"context"
	"testing"
	"time"

	"github.com/AbraTM/gork/internal/job"
	"github.com/AbraTM/gork/internal/queue"
)

const testQueueSize = 10

func newTestJob() job.Job {
	return job.Job{
		ID:      "test-job-1",
		Type:    "email",
		Payload: []byte("user12@somemail.com"),
	}
}

func TestNewInMemoryQueue(t *testing.T) {
	mq := queue.NewInMemoryQueue(testQueueSize)

	if mq == nil {
		t.Fatal("expected in memory queue, got nil")
	}
}

func TestInMemoryQueue_Publish(t *testing.T) {
	mq := queue.NewInMemoryQueue(testQueueSize)

	currSize := mq.Len()

	ctx := context.Background()
	err := mq.Publish(ctx, newTestJob())
	if err != nil {
		t.Fatalf("unexpected error, %v\n", err)
	}

	if currSize == mq.Len() {
		t.Fatal("in memory queue failed to publish")
	}
}

func TestInMemoryQueue_Publish_Context_Close(t *testing.T) {
	mq := queue.NewInMemoryQueue(testQueueSize)

	ctx, cancel := context.WithCancel(context.Background())

	for range testQueueSize {
		if err := mq.Publish(ctx, newTestJob()); err != nil {
			t.Fatalf("error publishing job, %v\n", err)
		}
	}

	cancel()

	err := mq.Publish(ctx, newTestJob())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestInMemoryQueue_Consume_Returns_Message_Channel(t *testing.T) {
	mq := queue.NewInMemoryQueue(testQueueSize)

	ctx := context.Background()
	messagesChan, err := mq.Consume(ctx)

	if err != nil {
		t.Fatalf("unexpected error, %v\n", err)
	}

	if messagesChan == nil {
		t.Fatal("expected messages channel, got nil")
	}
}

func TestInMemoryQueue_Consume_Returns_Jobs(t *testing.T) {
	mq := queue.NewInMemoryQueue(testQueueSize)
	ctx := context.Background()
	testJob := newTestJob()
	publishErr := mq.Publish(ctx, testJob)
	if publishErr != nil {
		t.Fatalf("unexpected error while publishing, %v\n", publishErr)
	}

	messagesChan, consumeErr := mq.Consume(ctx)
	if consumeErr != nil {
		t.Fatalf("unexpected error while consuming, %v\n", consumeErr)
	}

	select {
	case m := <-messagesChan:
		if m.Job.ID != testJob.ID {
			t.Fatal("received an unexpected job")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for job")
	}
}

func TestNewInMemoryQueue_Message_Ack_Nack(t *testing.T) {
	mq := queue.NewInMemoryQueue(testQueueSize)
	ctx := context.Background()

	if err := mq.Publish(ctx, newTestJob()); err != nil {
		t.Fatalf("unexpected error while publishing %v\n", err)
	}

	messagesChan, _ := mq.Consume(ctx)
	m := <-messagesChan

	if err := m.Ack(); err != nil {
		t.Fatalf("expected nil from Ack got %v", err)
	}

	if err := mq.Publish(ctx, newTestJob()); err != nil {
		t.Fatalf("unexpected error while publishing %v\n", err)
	}
	m = <-messagesChan

	if err := m.Nack(); err != nil {
		t.Fatalf("expected nil from Nack, got %v", err)
	}
}
