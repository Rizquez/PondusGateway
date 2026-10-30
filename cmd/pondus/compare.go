package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"pondus-gateway/internal/observability"
)

// comparisonResult stores the results of a simulation
// run using a specific planning strategy.
//
// Associates the planner's name with the metrics report
// generated during its execution.
type comparisonResult struct {
	Scheduler string
	Report    observability.Report
}

// runComparison compares the available scheduling strategies using
// the same configuration and the same batch of operations.
//
// It loads and validates the configuration, generates the batch of
// operations and first verifies that all operations can be accommodated.
//
// It sequentially executes the FIFO, CapacityFit and CapacityFit with
// starvation protection strategies; each strategy uses its own controller
// and Runner.
//
// For the protected strategy, it uses the configured lead time limit or
// the default value of two if it is zero.
//
// Collects metrics from each simulation and displays a comparison table
// using printComparison.
//
// Returns an error if the configuration, admission or execution of any of
// the strategies fails; if all simulations complete successfully, returns nil.
func runComparison(path string, output io.Writer) error {
	file, err := loadConfig(path)
	if err != nil {
		return err
	}

	batch := buildBatch(file)

	limits := file.limits()

	if err := validateBatchAdmission(limits, batch); err != nil {
		return err
	}

	strategies := []string{
		"fifo",
		"capacity-fit",
		"capacity-fit-guarded",
	}

	maxBypasses := file.Scheduler.MaxBypasses
	if maxBypasses == 0 {
		maxBypasses = 2
	}

	comparisons := make(
		[]comparisonResult,
		0,
		len(strategies),
	)

	for _, strategy := range strategies {
		scenario := file
		scenario.Scheduler.Type = strategy
		scenario.Scheduler.MaxBypasses = 0

		if strategy == "capacity-fit-guarded" {
			scenario.Scheduler.MaxBypasses = maxBypasses
		}

		controller, runner, err := newConfiguredRunner(scenario)
		if err != nil {
			return fmt.Errorf("scheduler %s: %w", strategy, err)
		}

		_, report, err := measureRun(
			context.Background(),
			runner,
			controller,
			batch,
			limits.MaxCapacity,
		)
		if err != nil {
			return fmt.Errorf(
				"simulation with %s: %w",
				strategy, err,
			)
		}

		comparisons = append(
			comparisons,
			comparisonResult{
				Scheduler: strategy,
				Report:    report,
			},
		)
	}

	printComparison(output, comparisons)
	return nil
}

// printComparison prints a comparison of the metrics obtained during the execution of
// different planning strategies.
//
// It presents the results in two tables:
//   - Execution results: completed, failed, rejected operations and total duration.
//   - Resource utilization: maximum reserved capacity and average utilization percentage.
//
// Durations are rounded to the microsecond, and utilization percentages are displayed to
// two decimal places.
func printComparison(output io.Writer, comparisons []comparisonResult) {
	fmt.Fprintln(output, "Pondus Gateway | Scheduler comparison")
	fmt.Fprintln(output)

	fmt.Fprintf(
		output,
		"%-22s %9s %9s %9s %11s\n",
		"SCHEDULER",
		"COMPLETE",
		"FAILED",
		"REJECTED",
		"DURATION",
	)

	for _, item := range comparisons {
		report := item.Report

		fmt.Fprintf(
			output,
			"%-22s %9d %9d %9d %11s\n",
			item.Scheduler,
			report.Completed,
			report.Failed,
			report.Rejected,
			report.Duration.Round(time.Microsecond),
		)
	}

	fmt.Fprintln(output)
	fmt.Fprintf(
		output,
		"%-22s %14s %17s\n",
		"SCHEDULER",
		"PEAK RESERVED",
		"AVG UTILIZATION",
	)

	for _, item := range comparisons {
		fmt.Fprintf(
			output,
			"%-22s %14d %16.2f%%\n",
			item.Scheduler,
			item.Report.PeakUsed,
			item.Report.AverageUtilization,
		)
	}
}
