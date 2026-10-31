package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"pondus-gateway/internal/admission"
	"pondus-gateway/internal/core"
	"pondus-gateway/internal/execution"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// run processes the arguments and runs the simulation in direct mode,
// using YAML configuration or by comparing planning strategies.
//
// Comparison mode requires the -client option. This option cannot be
// combined with execution parameters other than -compare.
func run(args []string, output io.Writer) error {
	flags := flag.NewFlagSet("pondus", flag.ContinueOnError)
	flags.SetOutput(io.Discard)

	concurrency := flags.Int(
		"concurrency", 4,
		"maximum concurrent operations",
	)
	capacity := flags.Uint64(
		"capacity", 10,
		"maximum available capacity",
	)
	operations := flags.Int(
		"operations", 20,
		"number of operations to generate",
	)
	duration := flags.Duration(
		"duration", 100*time.Millisecond,
		"simulated operation duration",
	)
	configPath := flags.String(
		"client", "",
		"path to YAML configuration file",
	)

	compare := flags.Bool(
		"compare",
		false,
		"compare all scheduling strategies",
	)

	if err := flags.Parse(args); err != nil {
		return err
	}

	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", flags.Args())
	}

	if *concurrency <= 0 {
		return fmt.Errorf("concurrency must be greater than zero")
	}

	if *capacity == 0 {
		return fmt.Errorf("capacity must be greater than zero")
	}

	if *operations < 0 {
		return fmt.Errorf("operations cannot be negative")
	}

	if *duration <= 0 {
		return fmt.Errorf("duration must be greater than zero")
	}

	if *configPath != "" {
		var mixed bool

		flags.Visit(func(f *flag.Flag) {
			if f.Name != "client" && f.Name != "compare" {
				mixed = true
			}
		})

		if mixed {
			return fmt.Errorf("cannot combine -client with other flags")
		}

		if *compare {
			return runComparison(*configPath, output)
		}

		return runConfigured(*configPath, output)
	}

	if *compare {
		return fmt.Errorf("-compare requires -client")
	}

	controller, err := admission.New(core.Limits{
		MaxConcurrent: *concurrency,
		MaxCapacity:   *capacity,
		MaxQueueSize:  *operations,
	})

	if err != nil {
		return err
	}

	executor := execution.SleepExecutor{
		Duration: *duration,
	}

	runner, err := execution.New(controller, executor)
	if err != nil {
		return err
	}

	// Reuses operation generation from YAML mode
	var config FileConfig
	config.Operations.Count = *operations
	batch := buildBatch(config)

	fmt.Fprintf(
		output,
		"Pondus Gateway | operations=%d concurrency=%d capacity=%d\n",
		*operations, *concurrency, *capacity,
	)

	results, report, err := measureRun(
		context.Background(),
		runner,
		controller,
		batch,
		*capacity,
	)

	printReport(output, report)

	if err != nil {
		return err
	}

	for _, result := range results {
		fmt.Fprintf(
			output,
			"%s | admission=%s | state=%s | error=%v\n",
			result.OperationID,
			result.Decision,
			result.State,
			result.Err,
		)
	}

	return nil
}
