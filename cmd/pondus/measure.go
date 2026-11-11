package main

import (
	"context"
	"fmt"
	"io"
	"pondus-gateway/internal/control/admission"
	"pondus-gateway/internal/domain"
	"pondus-gateway/internal/execution"
	"pondus-gateway/internal/observability"
	"time"
)

// outcome contains the results and any errors
// from an asynchronous Runner execution.
type outcome struct {
	results []execution.Result
	err     error
}

// measureRun runs a batch and collects execution and capacity
// utilization metrics through periodic sampling.
//
// The observed maximum and average utilization are estimates
// that may overlook capacity variations between samples.
func measureRun(
	ctx context.Context,
	runner *execution.Runner,
	controller *admission.Controller,
	operations []domain.Operation,
	capacity uint64,
) ([]execution.Result, observability.Report, error) {
	finished := make(chan outcome, 1)

	start := time.Now()
	last := start

	var sampler observability.Sampler

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	go func() {
		results, err := runner.Run(ctx, operations)
		finished <- outcome{
			results: results,
			err:     err,
		}
	}()

	for {
		select {
		case now := <-ticker.C:
			_, _, used := controller.Snapshot()

			sampler.Observe(used, now.Sub(last))
			last = now

		case result := <-finished:
			end := time.Now()

			_, _, used := controller.Snapshot()
			sampler.Observe(used, end.Sub(last))

			report := observability.Summarize(result.results, end.Sub(start))
			report.PeakUsed = sampler.PeakUsed()
			report.AverageUtilization = sampler.AverageUtilization(capacity)

			return result.results, report, result.err
		}
	}
}

// printReport displays the execution and capacity utilization
// metrics for a simulation.
func printReport(output io.Writer, report observability.Report) {
	fmt.Fprintln(output, "\n--- Simulation metrics ---")
	fmt.Fprintf(output, "Total: %d\n", report.Total)
	fmt.Fprintf(output, "Completed: %d\n", report.Completed)
	fmt.Fprintf(output, "Failed: %d\n", report.Failed)
	fmt.Fprintf(output, "Cancelled: %d\n", report.Cancelled)
	fmt.Fprintf(output, "Rejected: %d\n", report.Rejected)

	fmt.Fprintf(
		output,
		"Duration: %s\n",
		report.Duration,
	)

	fmt.Fprintf(
		output,
		"Peak reserved capacity (observed): %d\n",
		report.PeakUsed,
	)

	fmt.Fprintf(
		output,
		"Average reserved utilization (estimated): %.2f%%\n",
		report.AverageUtilization,
	)
}
