package main

import (
	"fmt"
	"log/slog"
	"os"
)

func initLogger() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)
}

func printUsage() {
	fmt.Println("******* gork *******")
	fmt.Println("usage: gork <command>")
	fmt.Println("\ncommands:")
	fmt.Printf("  %-10s %s\n", "run", "start the engine")
	fmt.Printf("  %-10s %s\n", "enqueue", "publish a job")
	fmt.Printf("  %-10s %s\n", "stats", "show engine stats")
}

func main() {
	initLogger()

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "run":
		runCmd()
	case "enqueue":
		enqueueCmd()
	case "stats":
		showStatsCmd()
	default:
		fmt.Printf("unknown command: %s\n", os.Args[1])
		os.Exit(1)
	}
}
