package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"
)

// clientConfig groups together the connection parameters, communication limits,
// and the window for concurrent requests for the TCP stream client.
type clientConfig struct {
	address         string
	filePath        string
	timeout         time.Duration
	window          int
	maxMessageBytes int
}

// main starts the TCP stream client.
func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// run loads the requests and handles their exchange with the TCP server.
//
// It applies a global timeout, while also respecting the cancellation
// of the provided context.
func run(parentContext context.Context, args []string, output io.Writer) error {
	config, err := parseConfig(args)
	if err != nil {
		return err
	}

	requests, err := loadRequests(config.filePath, config.maxMessageBytes)
	if err != nil {
		return err
	}

	clientContext, cancel := context.WithTimeout(parentContext, config.timeout)
	defer cancel()

	return exchange(clientContext, config, requests, output)
}

// parseConfig parses and validates command-line arguments,
// including the connection, the request file and client limits.
func parseConfig(args []string) (clientConfig, error) {
	flags := flag.NewFlagSet("pondus-stream-client", flag.ContinueOnError)
	flags.SetOutput(io.Discard)

	address := flags.String(
		"addr",
		"127.0.0.1:9001",
		"TCP server address",
	)

	filePath := flags.String(
		"file",
		"clients/stream-requests.jsonl",
		"NDJSON file with submit messages",
	)

	timeout := flags.Duration(
		"timeout",
		30*time.Second,
		"total client deadline",
	)

	window := flags.Int(
		"window",
		4,
		"maximum outstanding requests",
	)

	maxMessageBytes := flags.Int(
		"max-message-bytes",
		65536,
		"maximum bytes per JSON line",
	)

	if err := flags.Parse(args); err != nil {
		return clientConfig{}, err
	}

	if flags.NArg() != 0 {
		return clientConfig{}, fmt.Errorf("unexpected arguments: %v", flags.Args())
	}

	config := clientConfig{
		address:         *address,
		filePath:        *filePath,
		timeout:         *timeout,
		window:          *window,
		maxMessageBytes: *maxMessageBytes,
	}
	if config.address == "" ||
		config.filePath == "" ||
		config.timeout <= 0 ||
		config.window <= 0 ||
		config.maxMessageBytes <= 0 {
		return clientConfig{}, errors.New("address, file, timeout, window and max-message-bytes must be valid and positive")
	}

	return config, nil
}
