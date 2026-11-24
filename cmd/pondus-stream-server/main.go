package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"pondus-gateway/internal/control/admission"
	"pondus-gateway/internal/control/scheduler"
	"pondus-gateway/internal/domain"
	"pondus-gateway/internal/execution/continuous"
	"pondus-gateway/internal/execution/simulate"
	tcpstream "pondus-gateway/internal/transport/tcp-stream"
	"syscall"
	"time"
)

// serverConfig groups together the parameters for server execution,
// scheduling, TCP transport and shutdown.
type serverConfig struct {
	address       string
	limits        domain.Limits
	scheduler     string
	maxBypasses   uint64
	duration      time.Duration
	transport     tcpstream.Config
	shutdownLimit time.Duration
}

// main starts the TCP stream server.
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// run processes the command-line arguments and starts
// the server with the validated configuration.
func run(ctx context.Context, args []string, output io.Writer) error {
	config, err := parseConfig(args)
	if err != nil {
		return err
	}
	return serve(ctx, config, output)
}

// parseConfig parses and validates the command-line arguments,
// building the complete server configuration.
func parseConfig(args []string) (serverConfig, error) {
	flags := flag.NewFlagSet("pondus-stream-server", flag.ContinueOnError)
	flags.SetOutput(io.Discard)

	address := flags.String(
		"addr",
		"127.0.0.1:9001",
		"TCP listen address",
	)

	maxConnections := flags.Int(
		"max-connections",
		32,
		"maximum concurrent TCP connections",
	)

	maxMessageBytes := flags.Int(
		"max-message-bytes",
		65536,
		"maximum bytes per JSON line",
	)

	outgoingQueue := flags.Int(
		"outgoing-queue",
		64,
		"maximum pending responses per connection",
	)

	readTimeout := flags.Duration(
		"read-timeout",
		0,
		"read deadline per line (0 disables)",
	)

	writeTimeout := flags.Duration(
		"write-timeout",
		5*time.Second,
		"write deadline per response",
	)

	concurrency := flags.Int(
		"concurrency",
		4,
		"maximum concurrent operations",
	)

	capacity := flags.Uint64(
		"capacity",
		10,
		"global capacity budget",
	)

	queue := flags.Int(
		"queue",
		20,
		"maximum pending operations",
	)

	scheduling := flags.String(
		"scheduler",
		"fifo",
		"fifo, capacity-fit or capacity-fit-guarded",
	)

	maxBypasses := flags.Uint64(
		"max-bypasses",
		0,
		"guarded scheduler bypass limit",
	)

	duration := flags.Duration(
		"duration",
		100*time.Millisecond,
		"sleep operation duration",
	)

	shutdownTimeout := flags.Duration(
		"shutdown-timeout",
		5*time.Second,
		"maximum wait for cooperative shutdown",
	)

	if err := flags.Parse(args); err != nil {
		return serverConfig{}, err
	}

	if flags.NArg() != 0 {
		return serverConfig{}, fmt.Errorf("unexpected arguments: %v", flags.Args())
	}

	config := serverConfig{
		address: *address,
		limits: domain.Limits{
			MaxConcurrent: *concurrency,
			MaxCapacity:   *capacity,
			MaxQueueSize:  *queue,
		},
		scheduler:     *scheduling,
		maxBypasses:   *maxBypasses,
		duration:      *duration,
		shutdownLimit: *shutdownTimeout,
		transport: tcpstream.Config{
			MaxConnections:  *maxConnections,
			MaxMessageBytes: *maxMessageBytes,
			OutgoingQueue:   *outgoingQueue,
			ReadTimeout:     *readTimeout,
			WriteTimeout:    *writeTimeout,
		},
	}

	if err := config.validate(); err != nil {
		return serverConfig{}, err
	}

	return config, nil
}

// validate checks the server parameters and compatibility
// between the limits, the transport and the planning strategy.
func (config serverConfig) validate() error {
	if config.address == "" || config.duration <= 0 || config.shutdownLimit <= 0 {
		return errors.New("address, duration and shutdown-timeout must be valid and positive")
	}

	if err := config.limits.Validate(); err != nil {
		return fmt.Errorf("limits: %w", err)
	}

	if err := config.transport.Validate(); err != nil {
		return fmt.Errorf("transport: %w", err)
	}

	_, err := config.selector()
	return err
}

// The selector builds the configured planning strategy,
// checking the lead time constraints.
func (config serverConfig) selector() (scheduler.Selector, error) {
	switch config.scheduler {
	case "fifo":
		if config.maxBypasses != 0 {
			return nil, errors.New("max-bypasses requires capacity-fit-guarded")
		}
		return scheduler.FIFO{}, nil
	case "capacity-fit":
		if config.maxBypasses != 0 {
			return nil, errors.New("max-bypasses requires capacity-fit-guarded")
		}
		return scheduler.CapacityFit{}, nil
	case "capacity-fit-guarded":
		return scheduler.NewStarvationGuard(scheduler.CapacityFit{}, config.maxBypasses)
	default:
		return nil, fmt.Errorf("unsupported scheduler: %q", config.scheduler)
	}
}

// The serve function initializes the controller, coordinator and TCP stream server,
// sharing execution resources among all connections.
//
// After the TCP service ends, it requests that the coordinator close within a timeout
// period and combines the errors from both components.
func serve(ctx context.Context, config serverConfig, output io.Writer) error {
	selector, err := config.selector()
	if err != nil {
		return err
	}

	controller, err := admission.NewWithSelector(config.limits, selector)
	if err != nil {
		return err
	}

	coordinator, err := continuous.New(controller, simulate.SleepExecutor{Duration: config.duration})
	if err != nil {
		return err
	}

	server, err := tcpstream.New(config.transport, coordinator)
	if err != nil {
		return err
	}

	listener, err := net.Listen("tcp", config.address)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", config.address, err)
	}
	defer listener.Close()

	if err := coordinator.Start(); err != nil {
		return err
	}

	fmt.Fprintf(output, "Pondus stream TCP listening on %s\n", listener.Addr())

	serveErr := server.Serve(ctx, listener)
	closeCtx, cancelClose := context.WithTimeout(context.Background(), config.shutdownLimit)
	defer cancelClose()
	closeErr := coordinator.Close(closeCtx)
	return errors.Join(serveErr, closeErr)
}
