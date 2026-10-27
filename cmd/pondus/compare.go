package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"pondus-gateway/internal/admission"
	"pondus-gateway/internal/core"
	"pondus-gateway/internal/execution"
	"pondus-gateway/internal/observability"
)

type comparisonResult struct {
	Scheduler string
	Report    observability.Report
}

func runComparison(path string, output io.Writer) error {
	config, err := loadConfig(path)
	if err != nil {
		return err
	}

	batch := buildBatch(config)

	limits := core.Limits{
		MaxConcurrent: config.Limits.MaxConcurrent,
		MaxCapacity:   config.Limits.MaxCapacity,
		MaxQueueSize:  config.Limits.MaxQueueSize,
	}

	if err := validateBatchAdmission(limits, batch); err != nil {
		return err
	}

	strategies := []string{
		"fifo",
		"capacity-fit",
		"capacity-fit-guarded",
	}

	maxBypasses := config.Scheduler.MaxBypasses
	if maxBypasses == 0 {
		maxBypasses = 2
	}

	comparisons := make(
		[]comparisonResult,
		0,
		len(strategies),
	)

	for _, strategy := range strategies {
		scenario := config
		scenario.Scheduler.Type = strategy
		scenario.Scheduler.MaxBypasses = 0

		if strategy == "capacity-fit-guarded" {
			scenario.Scheduler.MaxBypasses = maxBypasses
		}

		selector, err := buildSelector(scenario)
		if err != nil {
			return fmt.Errorf(
				"scheduler %s: %w",
				strategy, err,
			)
		}

		controller, err := admission.NewWithSelector(
			limits,
			selector,
		)
		if err != nil {
			return err
		}

		duration, err := time.ParseDuration(
			config.Executor.Duration,
		)
		if err != nil {
			return err
		}

		runner, err := execution.New(
			controller,
			execution.SleepExecutor{
				Duration: duration,
			},
		)
		if err != nil {
			return err
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

func printComparison(
	output io.Writer,
	comparisons []comparisonResult,
) {
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
		r := item.Report

		fmt.Fprintf(
			output,
			"%-22s %9d %9d %9d %11s\n",
			item.Scheduler,
			r.Completed,
			r.Failed,
			r.Rejected,
			r.Duration.Round(time.Microsecond),
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
