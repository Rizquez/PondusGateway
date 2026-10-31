package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"pondus-gateway/internal/observability"
)

type comparisonResult struct {
	Scheduler string
	Report    observability.Report
}

// runComparison compares planning strategies using the same
// set of operations and the same admission limits.
//
// Each strategy is executed using a separate controller.
// StarvationGuard uses a maximum of two advances when no
// other value has been configured.
//
// The comparison is terminated if any simulation returns an
// error.
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
