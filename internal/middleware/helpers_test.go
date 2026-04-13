package middleware_test

import (
	"github.com/AbraTM/gork/internal/job"
)

func newTestJob() job.Job {
	return job.Job{
		ID:      "test-job-1",
		Type:    "email",
		Payload: []byte("user14@somemail.com"),
	}
}
