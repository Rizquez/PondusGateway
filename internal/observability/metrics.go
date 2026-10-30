package observability

import (
	"time"

	"pondus-gateway/internal/admission"
	"pondus-gateway/internal/core"
	"pondus-gateway/internal/execution"
)

// The report contains aggregated metrics for a batch of operations.
//
// It includes the total number of operations, their final results,
// the duration of the batch, and capacity utilization metrics.
type Report struct {
	Total              int
	Completed          int
	Failed             int
	Cancelled          int
	Rejected           int
	Duration           time.Duration
	PeakUsed           uint64
	AverageUtilization float64
}

// Sampler collects samples of capacity utilization
// to calculate peak consumption and average utilization.
//
// It maintains the most recent observed consumption, the
// cumulative time and the time-weighted sum of consumption.
type Sampler struct {
	peakUsed     uint64
	weightedUsed float64
	elapsed      time.Duration
	lastUsed     uint64
}

// The observation records a new instance of capacity utilization.
//
// The parameter used represents the capacity utilized in the new observation,
// while elapsed indicates the time that has elapsed since the previous observation.
//
// It accumulates the previous consumption weighted by the elapsed time, updates the
// latest observed consumption, and records the maximum reached.
//
// If elapsed is negative, it ignores the observation without changing  the Sampler’s
// state.
//
// The first observation starts with a previous consumption equal to zero.
func (sampler *Sampler) Observe(used uint64, elapsed time.Duration) {
	if elapsed < 0 {
		return
	}

	sampler.weightedUsed += float64(sampler.lastUsed) *
		elapsed.Seconds()

	sampler.elapsed += elapsed
	sampler.lastUsed = used

	if used > sampler.peakUsed {
		sampler.peakUsed = used
	}
}

// PeakUsed returns the maximum capacity recorded
// during the observations.
//
// Returns zero if no positive capacity utilization
// has been recorded.
func (sampler *Sampler) PeakUsed() uint64 {
	return sampler.peakUsed
}

// AverageUtilization calculates the average utilization percentage
// relative to the maximum capacity provided.
//
// It uses the time-weighted cumulative consumption and the total
// recorded duration to calculate the average.
//
// The result is expressed as a percentage and is not limited to the
// range between zero and one hundred.
//
// Returns zero if the provided capacity is zero or if no observation
// time has been accumulated.
func (sampler *Sampler) AverageUtilization(capacity uint64) float64 {
	if capacity == 0 || sampler.elapsed == 0 {
		return 0
	}

	return 100 * sampler.weightedUsed /
		(float64(capacity) * sampler.elapsed.Seconds())
}

// Summarize generates a report based on the execution results and the total
// duration provided.
//
// It counts the completed, failed, canceled, and rejected operations.
//
// Rejected operations are counted regardless of their status.
//
// Accepted operations whose status is not Completed, Failed or Cancelled are
// not included in any of these counters.
//
// Returns a Report with the calculated counters and the specified duration.
//
// The PeakUsed and AverageUtilization metrics remain at zero, since they are
// not calculated based on the received results.
func Summarize(results []execution.Result, duration time.Duration) Report {
	report := Report{
		Total:    len(results),
		Duration: duration,
	}

	for _, result := range results {
		if result.Decision == admission.Rejected {
			report.Rejected++
			continue
		}

		switch result.State {
		case core.StateCompleted:
			report.Completed++

		case core.StateFailed:
			report.Failed++

		case core.StateCancelled:
			report.Cancelled++
		}
	}

	return report
}
