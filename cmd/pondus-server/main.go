package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"pondus-gateway/internal/domain"
	"pondus-gateway/internal/transport/tcp"
	"syscall"
	"time"
)

// main starts the TCP server
func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// run configures and starts the TCP server based on the command-line
// arguments, validating transport and execution limits.
//
// It keeps the service running until it receives an interrupt or
// termination signal, at which point it requests an orderly shutdown.
func run(args []string) error {
	flags := flag.NewFlagSet("pondus-server", flag.ContinueOnError)

	address := flags.String(
		"addr",
		"127.0.0.1:9000",
		"listening TCP address",
	)

	maxMessage := flags.Int64(
		"max-message-bytes",
		65536,
		"maximum message size, including line breaks",
	)

	maxConnections := flags.Int(
		"max-connections", 32, "simultaneous connections",
	)

	readTimeout := flags.Duration(
		"read-timeout",
		5*time.Second,
		"deadline for receiving an application",
	)

	writeTimeout := flags.Duration(
		"write-timeout",
		5*time.Second,
		"deadline for submitting a response",
	)

	concurrency := flags.Int(
		"concurrency",
		4,
		"maximum number of concurrent operations",
	)

	capacity := flags.Uint64(
		"capacity",
		10,
		"maximum capacity",
	)

	queue := flags.Int(
		"queue",
		20,
		"maximum queue size",
	)

	scheduler := flags.String(
		"scheduler",
		"fifo",
		"strategy: fifo, capacity-fit, or capacity-fit-guarded",
	)

	maxBypasses := flags.Uint64(
		"max-bypasses",
		0,
		"permitted overtakes in the protected scheduler",
	)

	duration := flags.Duration(
		"duration",
		100*time.Millisecond,
		"duration of sleep operations",
	)

	if err := flags.Parse(args); err != nil {
		return err
	}

	if flags.NArg() > 0 {
		return fmt.Errorf("unexpected arguments: %v", flags.Args())
	}

	engine, err := tcp.New(tcp.Config{
		Limits: domain.Limits{
			MaxConcurrent: *concurrency,
			MaxCapacity:   *capacity,
			MaxQueueSize:  *queue,
		},
		Scheduler:     *scheduler,
		MaxBypasses:   *maxBypasses,
		SleepDuration: *duration,
	})
	if err != nil {
		return err
	}

	server, err := NewServer(Config{
		MaxMessageBytes: *maxMessage,
		MaxConnections:  *maxConnections,
		ReadTimeout:     *readTimeout,
		WriteTimeout:    *writeTimeout,
	}, engine.Handle)
	if err != nil {
		return err
	}

	listener, err := net.Listen("tcp", *address)
	if err != nil {
		return fmt.Errorf(
			"listen on %s: %w",
			*address, err,
		)
	}

	defer listener.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Printf("Pondus TCP listening on %s\n", listener.Addr())

	return server.Serve(ctx, listener)
}
