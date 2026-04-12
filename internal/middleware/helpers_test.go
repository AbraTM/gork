package middleware_test

import (
	"fmt"

	"github.com/AbraTM/gork/internal/job"
)

func newTestJob() job.Job {
	return job.Job{
		ID:      "test-job-1",
		Type:    "email",
		Payload: []byte("user14@somemail.com"),
	}
}

func newTestJobFactory() func() job.Job {
	id := 0

	return func() job.Job {
		id++

		return job.Job{
			ID:      fmt.Sprintf("test-job-%d", id),
			Type:    "email",
			Payload: []byte(fmt.Sprintf("user%d@somemail.com", id)),
		}
	}
}
