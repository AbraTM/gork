package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
)

type statsResponse struct {
	QueueLen    int `json:"queue_len"`
	WorkerCount int `json:"worker_count"`
}

func showStatsCmd() {
	flags := flag.NewFlagSet("stats", flag.ExitOnError)
	addr := flags.String("addr", defaultAddr, "engine address")
	if err := flags.Parse(os.Args[2:]); err != nil {
		fmt.Println("failed to parse flags.")
	}

	resp, err := http.Get(fmt.Sprintf("http://%s/stats", *addr))
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

	var stats statsResponse
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		fmt.Printf("error: failed to decode response: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("======= gork stats =======")
	fmt.Printf("%-20s %d\n", "queue depth:", stats.QueueLen)
	fmt.Printf("%-20s %d\n", "workers:", stats.WorkerCount)
}
