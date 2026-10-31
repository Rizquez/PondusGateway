package observability

import (
	"math"
	"pondus-gateway/internal/admission"
	"pondus-gateway/internal/core"
	"pondus-gateway/internal/execution"
	"testing"
	"time"
)

func TestSummarize(t *testing.T) {
	tests := []struct {
		name     string
		results  []execution.Result
		duration time.Duration
		want     Report
	}{
		{
			name: "mixed results",
			results: []execution.Result{
				{Decision: admission.Started, State: core.StateCompleted},
				{Decision: admission.Queued, State: core.StateCompleted},
				{Decision: admission.Started, State: core.StateFailed},
				{Decision: admission.Started, State: core.StateCancelled},
				{Decision: admission.Rejected, State: core.StateFailed},
			},
			duration: 2 * time.Second,
			want: Report{
				Total: 5, Completed: 2, Failed: 1,
				Cancelled: 1, Rejected: 1, Duration: 2 * time.Second,
			},
		},
		{
			name: "no transactions",
			want: Report{},
		},
		{
			name: "non-terminal states are not counted",
			results: []execution.Result{
				{Decision: admission.Started, State: core.StateRunning},
				{Decision: admission.Queued, State: core.StatePending},
			},
			want: Report{Total: 2},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := Summarize(test.results, test.duration); got != test.want {
				t.Fatalf(
					"Summarize() = %+v; want %+v",
					got, test.want,
				)
			}
		})
	}
}

func TestSampler(t *testing.T) {
	var sampler Sampler
	sampler.Observe(8, 0)
	sampler.Observe(4, time.Second)
	sampler.Observe(0, time.Second)
	sampler.Observe(0, time.Second)

	if got := sampler.PeakUsed(); got != 8 {
		t.Fatalf("PeakUsed() = %d; want 8", got)
	}
	if got := sampler.AverageUtilization(10); math.Abs(got-40) > 1e-9 {
		t.Fatalf("AverageUtilization(10) = %.6f; want 40", got)
	}
}

func TestSamplerIgnoresNegativeInterval(t *testing.T) {
	var sampler Sampler
	sampler.Observe(5, 0)
	sampler.Observe(9, -time.Second)
	sampler.Observe(0, time.Second)

	if got := sampler.PeakUsed(); got != 5 {
		t.Fatalf("PeakUsed() = %d; want 5", got)
	}
	if got := sampler.AverageUtilization(10); math.Abs(got-50) > 1e-9 {
		t.Fatalf("AverageUtilization(10) = %.6f; want 50", got)
	}
}

func TestSamplerZeroDenominator(t *testing.T) {
	var sampler Sampler
	if got := sampler.AverageUtilization(10); got != 0 {
		t.Fatalf("without samples: got %.6f; want 0", got)
	}
	sampler.Observe(4, time.Second)
	if got := sampler.AverageUtilization(0); got != 0 {
		t.Fatalf("zero capacity: got %.6f; want 0", got)
	}
}
