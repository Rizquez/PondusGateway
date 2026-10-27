package execution

import (
	"context"
	"errors"
	"pondus-gateway/internal/admission"
	"pondus-gateway/internal/core"
	"sync/atomic"
	"testing"
	"time"
)

type cancellableExecutor struct {
	runs    atomic.Int64
	started chan struct{}
}

func (executor *cancellableExecutor) Execute(ctx context.Context, _ core.Operation) error {
	executor.runs.Add(1)

	if executor.started != nil {
		select {
		case executor.started <- struct{}{}:
		default:
		}
	}

	select {
	case <-ctx.Done():
		return ctx.Err()

	case <-time.After(100 * time.Millisecond):
		return nil
	}
}

func newTestRunner(t *testing.T) (*admission.Controller, *Runner, *cancellableExecutor) {
	t.Helper()

	controller, err := admission.New(core.Limits{
		MaxConcurrent: 1,
		MaxCapacity:   1,
		MaxQueueSize:  3,
	})

	if err != nil {
		t.Fatal(err)
	}

	executor := &cancellableExecutor{
		started: make(chan struct{}, 1),
	}

	runner, err := New(controller, executor)
	if err != nil {
		t.Fatal(err)
	}

	return controller, runner, executor
}

func cancellationTestOp(id string) core.Operation {
	return core.Operation{
		ID:       id,
		ClientID: "client-1",
		Type:     "compute",
		Cost:     1,
	}
}

func TestRunnerOperationTimeout(t *testing.T) {
	controller, runner, exec := newTestRunner(t)

	if err := runner.SetTimeout(5 * time.Millisecond); err != nil {
		t.Fatal(err)
	}

	results, err := runner.Run(
		context.Background(),
		[]core.Operation{
			cancellationTestOp("a"),
			cancellationTestOp("b"),
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	for _, result := range results {
		if !errors.Is(result.Err, context.DeadlineExceeded) {
			t.Fatalf(
				"unexpected result: %+v",
				result,
			)
		}
	}

	if exec.runs.Load() != 2 {
		t.Fatal("expected two executions")
	}

	active, queued, used := controller.Snapshot()
	if active != 0 || queued != 0 || used != 0 {
		t.Fatal("capacity leak")
	}
}

func TestRunnerCancelPending(t *testing.T) {
	controller, runner, exec := newTestRunner(t)

	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	defer cancel()

	done := make(chan []Result, 1)
	errs := make(chan error, 1)

	go func() {
		results, err := runner.Run(
			ctx,
			[]core.Operation{
				cancellationTestOp("a"),
				cancellationTestOp("b"),
				cancellationTestOp("controller"),
			},
		)
		errs <- err
		done <- results
	}()

	// Waiting for the first operation to begin
	<-exec.started

	cancel()

	select {
	case results := <-done:
		if err := <-errs; err != nil {
			t.Fatal(err)
		}

		for _, result := range results {
			if !errors.Is(result.Err, context.Canceled) {
				t.Fatalf(
					"unexpected result: %+v",
					result,
				)
			}
		}

	case <-time.After(time.Second):
		t.Fatal("runner did not finish")
	}

	if exec.runs.Load() != 1 {
		t.Fatalf(
			"executed %d operations, want 1",
			exec.runs.Load(),
		)
	}

	active, queued, used := controller.Snapshot()
	if active != 0 || queued != 0 || used != 0 {
		t.Fatal("capacity leak after cancellation")
	}
}
