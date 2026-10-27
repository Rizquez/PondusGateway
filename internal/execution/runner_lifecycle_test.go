package execution

import (
	"context"
	"errors"
	"pondus-gateway/internal/admission"
	"pondus-gateway/internal/core"
	"testing"
	"time"
)

type lifecycleTestExecutor struct {
	err   error
	delay time.Duration
}

func (e lifecycleTestExecutor) Execute(
	ctx context.Context,
	_ core.Operation,
) error {
	if e.delay > 0 {
		timer := time.NewTimer(e.delay)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}

	return e.err
}

func TestRunnerLifecycle(t *testing.T) {
	tests := []struct {
		name      string
		executor  lifecycleTestExecutor
		timeout   time.Duration
		cancelled bool
		wantState core.State
		wantErr   error
	}{
		{
			name:      "completed",
			wantState: core.StateCompleted,
		},
		{
			name: "failed",
			executor: lifecycleTestExecutor{
				err: errors.New("execution failed"),
			},
			wantState: core.StateFailed,
		},
		{
			name: "timeout",
			executor: lifecycleTestExecutor{
				delay: time.Second,
			},
			timeout:   time.Millisecond,
			wantState: core.StateFailed,
			wantErr:   context.DeadlineExceeded,
		},
		{
			name:      "cancelled before start",
			cancelled: true,
			wantState: core.StateCancelled,
			wantErr:   context.Canceled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controller, err := admission.New(
				core.Limits{
					MaxConcurrent: 1,
					MaxCapacity:   1,
					MaxQueueSize:  1,
				},
			)
			if err != nil {
				t.Fatal(err)
			}

			runner, err := New(controller, tt.executor)
			if err != nil {
				t.Fatal(err)
			}

			if err := runner.SetTimeout(tt.timeout); err != nil {
				t.Fatal(err)
			}

			ctx, cancel := context.WithCancel(
				context.Background(),
			)
			defer cancel()

			if tt.cancelled {
				cancel()
			}

			results, err := runner.Run(
				ctx,
				[]core.Operation{
					{
						ID:       "op-1",
						ClientID: "client-1",
						Type:     "compute",
						Cost:     1,
					},
				},
			)
			if err != nil {
				t.Fatal(err)
			}

			if len(results) != 1 {
				t.Fatalf(
					"got %d results, want 1",
					len(results),
				)
			}

			result := results[0]

			if result.State != tt.wantState {
				t.Fatalf(
					"got state %s, want %s",
					result.State,
					tt.wantState,
				)
			}

			if tt.wantErr != nil &&
				!errors.Is(result.Err, tt.wantErr) {
				t.Fatalf(
					"got error %v, want %v",
					result.Err,
					tt.wantErr,
				)
			}

			active, queued, used := controller.Snapshot()

			if active != 0 || queued != 0 || used != 0 {
				t.Fatal("capacity was not fully released")
			}
		})
	}
}
