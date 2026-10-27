package observability

import (
	"math"
	"testing"
	"time"

	"pondus-gateway/internal/admission"
	"pondus-gateway/internal/core"
	"pondus-gateway/internal/execution"
)

func TestSummarize(t *testing.T) {
	results := []execution.Result{
		{
			Decision: admission.Started,
			State:    core.StateCompleted,
		},
		{
			Decision: admission.Queued,
			State:    core.StateCompleted,
		},
		{
			Decision: admission.Started,
			State:    core.StateFailed,
		},
		{
			Decision: admission.Started,
			State:    core.StateCancelled,
		},
		{
			Decision: admission.Rejected,
		},
	}

	report := Summarize(results, 2*time.Second)

	if report.Total != 5 ||
		report.Completed != 2 ||
		report.Failed != 1 ||
		report.Cancelled != 1 ||
		report.Rejected != 1 ||
		report.Duration != 2*time.Second {

		t.Fatalf("unexpected report: %+v", report)
	}
}

func TestSampler(t *testing.T) {
	var sampler Sampler

	sampler.Observe(8, 0)
	sampler.Observe(4, time.Second)
	sampler.Observe(0, time.Second)
	sampler.Observe(0, time.Second)

	if got := sampler.PeakUsed(); got != 8 {
		t.Fatalf("peak = %d, want 8", got)
	}

	got := sampler.AverageUtilization(10)

	if math.Abs(got-40) > 0.001 {
		t.Fatalf("utilization = %.2f, want 40", got)
	}
}
