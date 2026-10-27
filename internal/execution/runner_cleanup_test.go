package execution

import (
	"context"
	"errors"
	"testing"
	"time"

	"pondus-gateway/internal/admission"
	"pondus-gateway/internal/core"
)

type invalidIndexSelector struct{}

func (invalidIndexSelector) Next(
	_ []core.Operation,
	_ uint64,
) int {
	return 999
}

type cleanupTestExecutor struct{}

func (cleanupTestExecutor) Execute(
	ctx context.Context,
	op core.Operation,
) error {
	if op.ID == "a" {
		return nil
	}

	<-ctx.Done()
	return ctx.Err()
}

func TestRunnerCleanupOnInternalError(t *testing.T) {
	controller, err := admission.NewWithSelector(
		core.Limits{
			MaxConcurrent: 2,
			MaxCapacity:   2,
			MaxQueueSize:  1,
		},
		invalidIndexSelector{},
	)
	if err != nil {
		t.Fatal(err)
	}

	runner, err := New(controller, cleanupTestExecutor{})
	if err != nil {
		t.Fatal(err)
	}

	ops := []core.Operation{
		{ID: "a", ClientID: "x", Type: "test", Cost: 1},
		{ID: "b", ClientID: "x", Type: "test", Cost: 1},
		{ID: "c", ClientID: "x", Type: "test", Cost: 1},
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		2*time.Second,
	)
	defer cancel()

	results, err := runner.Run(ctx, ops)

	if err == nil {
		t.Fatal("expected internal error")
	}

	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("runner only stopped because test timed out")
	}

	if len(results) != 3 {
		t.Fatalf("got %d results, want 3", len(results))
	}

	if results[0].State != core.StateCompleted {
		t.Fatalf("unexpected state for a: %s", results[0].State)
	}

	if results[1].State != core.StateCancelled {
		t.Fatalf("unexpected state for b: %s", results[1].State)
	}

	if results[2].State != core.StateCancelled {
		t.Fatalf("unexpected state for c: %s", results[2].State)
	}

	active, queued, used := controller.Snapshot()

	if active != 0 || queued != 0 || used != 0 {
		t.Fatalf(
			"capacity leak: active=%d queued=%d used=%d",
			active, queued, used,
		)
	}
}
