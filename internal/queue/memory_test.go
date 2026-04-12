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

	for i := 0; i < testQueueSize; i++ {
		mq.Publish(ctx, newTestJob())
	}

	cancel()

	err := mq.Publish(ctx, newTestJob())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestInMemoryQueue_Consume_Returns_Jobs_Channel(t *testing.T) {
	mq := queue.NewInMemoryQueue(testQueueSize)

	ctx := context.Background()
	jobsChan, err := mq.Consume(ctx)

	if err != nil {
		t.Fatalf("unexpected error, %v\n", err)
	}

	if jobsChan == nil {
		t.Fatal("expected jobs channel, got nil")
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

	jobsChan, consumeErr := mq.Consume(ctx)
	if consumeErr != nil {
		t.Fatalf("unexpected error while consuming, %v\n", consumeErr)
	}

	select {
	case j := <-jobsChan:
		if j.ID != testJob.ID {
			t.Fatal("received an unexpected job")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for job")
	}
}
