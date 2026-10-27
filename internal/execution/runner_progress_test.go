package execution

import (
	"context"
	"errors"
	"testing"

	"pondus-gateway/internal/admission"
	"pondus-gateway/internal/core"
	"pondus-gateway/internal/scheduler"
)

type neverPromoteSelector struct{}

func (neverPromoteSelector) Next(
	_ []core.Operation,
	_ uint64,
) int {
	return -1
}

var _ scheduler.Selector = neverPromoteSelector{}

func TestRunnerNoProgress(t *testing.T) {
	controller, err := admission.NewWithSelector(
		core.Limits{
			MaxConcurrent: 1,
			MaxCapacity:   2,
			MaxQueueSize:  2,
		},
		neverPromoteSelector{},
	)
	if err != nil {
		t.Fatal(err)
	}

	runner, err := New(
		controller,
		lifecycleTestExecutor{},
	)
	if err != nil {
		t.Fatal(err)
	}

	operations := []core.Operation{
		{
			ID:       "a",
			ClientID: "client-1",
			Type:     "compute",
			Cost:     1,
		},
		{
			ID:       "b",
			ClientID: "client-1",
			Type:     "compute",
			Cost:     1,
		},
	}

	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	defer cancel()

	results, err := runner.Run(ctx, operations)

	if !errors.Is(err, ErrNoProgress) {
		t.Fatalf(
			"got error %v, want ErrNoProgress",
			err,
		)
	}

	if len(results) != 2 {
		t.Fatalf("got %d results, want 2", len(results))
	}

	if results[0].State != core.StateCompleted {
		t.Fatalf(
			"operation a: got state %s, want completed",
			results[0].State,
		)
	}

	if results[1].State != core.StateCancelled {
		t.Fatalf(
			"operation b: got state %s, want cancelled",
			results[1].State,
		)
	}

	if !errors.Is(results[1].Err, ErrNoProgress) {
		t.Fatalf(
			"operation b: got error %v",
			results[1].Err,
		)
	}

	active, queued, used := controller.Snapshot()

	if active != 0 || queued != 0 || used != 0 {
		t.Fatalf(
			"capacity leak: active=%d queued=%d used=%d",
			active, queued, used,
		)
	}
}
