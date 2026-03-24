package job

import "time"

type Job struct {
	ID        string
	Type      string
	Payload   []byte
	CreatedAt time.Time
	Retries   int
}
