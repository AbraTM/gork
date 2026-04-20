package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
)

func enqueueCmd() {
	flags := flag.NewFlagSet("enqueue", flag.ExitOnError)
	jobType := flags.String("type", "", "job type (required)")
	payload := flags.String("payload", "{}", "job payload is required")
	addr := flags.String("addr", defaultAddr, "engine address")
	if err := flags.Parse(os.Args[2:]); err != nil {
		fmt.Println("failed to parse flags.")
	}

	if *jobType == "" {
		fmt.Println("error: --type is required")
		flags.Usage()
		os.Exit(1)
	}

	body, err := json.Marshal(map[string]any{
		"type":    *jobType,
		"payload": json.RawMessage(*payload),
	})
	if err != nil {
		fmt.Printf("error: failed to build request: %v\n", err)
		os.Exit(1)
	}

	resp, err := http.Post(
		fmt.Sprintf("http://%s/jobs", *addr),
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		fmt.Printf("error: could not reach engine at %s\n", *addr)
		fmt.Println("is gork running ? try: gork run")
		os.Exit(1)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			fmt.Println("failed to properly close response body")
		}
	}()

	if resp.StatusCode != http.StatusAccepted {
		fmt.Printf("error: engine returned %d\n", resp.StatusCode)
		os.Exit(1)
	}

	fmt.Printf("job enqueued successfully [type=%s]\n", *jobType)
}
