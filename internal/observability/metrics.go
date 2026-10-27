package observability

import (
	"time"

	"pondus-gateway/internal/admission"
	"pondus-gateway/internal/core"
	"pondus-gateway/internal/execution"
)

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

type Sampler struct {
	peakUsed     uint64
	weightedUsed float64
	elapsed      time.Duration
	lastUsed     uint64
}

func (sampler *Sampler) Observe(
	used uint64,
	elapsed time.Duration,
) {
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

func (sampler *Sampler) PeakUsed() uint64 {
	return sampler.peakUsed
}

func (sampler *Sampler) AverageUtilization(
	capacity uint64,
) float64 {
	if capacity == 0 || sampler.elapsed == 0 {
		return 0
	}

	return 100 * sampler.weightedUsed /
		(float64(capacity) * sampler.elapsed.Seconds())
}
