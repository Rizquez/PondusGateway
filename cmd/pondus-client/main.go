package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"pondus-gateway/internal/transport/protocol"
	"time"
)

// main starts the TCP client
func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// run configures the TCP client based on the command-line
// arguments, loads a JSON request, and sends it to the server.
//
// It sets a timeout for the communication and displays the results.
//
// It returns an error if the exchange fails or the server responds
// with an error.
func run(args []string, output io.Writer) error {
	flags := flag.NewFlagSet("pondus-client", flag.ContinueOnError)
	flags.SetOutput(io.Discard)

	address := flags.String(
		"addr",
		"127.0.0.1:9000",
		"server TCP address",
	)

	path := flags.String(
		"file",
		"",
		"request JSON file",
	)

	timeout := flags.Duration(
		"timeout",
		30*time.Second,
		"total reporting period",
	)

	if err := flags.Parse(args); err != nil {
		return err
	}

	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", flags.Args())
	}

	if *address == "" || *path == "" || *timeout <= 0 {
		return fmt.Errorf("-addr and -file are required, and -timeout must be a positive number")
	}

	request, err := loadRequest(*path)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	response, err := exchange(ctx, *address, request)
	if err != nil {
		return err
	}

	printResults(output, response)

	if response.Status == protocol.StatusError {
		return fmt.Errorf(
			"server (%s): %s",
			response.Error.Code, response.Error.Message,
		)
	}

	return nil
}
