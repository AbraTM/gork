package job

type Message struct {
	Job  Job
	Ack  func() error
	Nack func() error
}
