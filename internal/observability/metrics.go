package observability

import (
	"pondus-gateway/internal/admission"
	"pondus-gateway/internal/core"
	"pondus-gateway/internal/execution"
	"time"
)

// This report groups the performance and capacity utilization metrics
// for a set of operations.
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

// Sampler collects capacity samples to calculate the observed
// maximum and the time-weighted average utilization.
//
// It does not include synchronization for concurrent accesses.
type Sampler struct {
	peakUsed     uint64
	weightedUsed float64
	elapsed      time.Duration
	lastUsed     uint64
}

// Observe records the current consumption and weights the previous
// consumption by the time elapsed since the last observation.
//
// Negative intervals are ignored. The first observation
// assumes a previous consumption of zero.
func (sampler *Sampler) Observe(used uint64, elapsed time.Duration) {
	if elapsed < 0 {
		return
	}

	sampler.weightedUsed += float64(sampler.lastUsed) * elapsed.Seconds()

	sampler.elapsed += elapsed
	sampler.lastUsed = used

	if used > sampler.peakUsed {
		sampler.peakUsed = used
	}
}

// PeakUsed returns the maximum observed capacity.
func (sampler *Sampler) PeakUsed() uint64 {
	return sampler.peakUsed
}

// AverageUtilization returns the average utilization as a percentage
// of the allocated capacity, without capping the result at 100%.
func (sampler *Sampler) AverageUtilization(capacity uint64) float64 {
	if capacity == 0 || sampler.elapsed == 0 {
		return 0
	}

	return 100 * sampler.weightedUsed / (float64(capacity) * sampler.elapsed.Seconds())
}

// Summarize aggregates the execution results, taking into account rejections
// regardless of the final status of the operations.
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
