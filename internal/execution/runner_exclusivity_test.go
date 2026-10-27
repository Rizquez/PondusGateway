package execution

import (
	"context"
	"errors"
	"testing"
	"time"

	"pondus-gateway/internal/admission"
	"pondus-gateway/internal/core"
)

type blockingExecutor struct {
	started chan struct{}
	release chan struct{}
}

func (e *blockingExecutor) Execute(
	ctx context.Context,
	_ core.Operation,
) error {
	select {
	case e.started <- struct{}{}:
	default:
	}

	select {
	case <-e.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestRunnerExclusiveExecution(t *testing.T) {
	controller, err := admission.New(core.Limits{
		MaxConcurrent: 1,
		MaxCapacity:   1,
		MaxQueueSize:  1,
	})
	if err != nil {
		t.Fatal(err)
	}

	exec := &blockingExecutor{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	defer close(exec.release)

	runner, err := New(controller, exec)
	if err != nil {
		t.Fatal(err)
	}

	op := core.Operation{
		ID:       "a",
		ClientID: "client-1",
		Type:     "compute",
		Cost:     1,
	}

	type outcome struct {
		results []Result
		err     error
	}

	finished := make(chan outcome, 1)

	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	defer cancel()

	go func() {
		results, err := runner.Run(
			ctx,
			[]core.Operation{op},
		)
		finished <- outcome{results: results, err: err}
	}()

	select {
	case <-exec.started:
	case <-time.After(2 * time.Second):
		t.Fatal("executor did not start")
	}

	_, err = runner.Run(
		context.Background(),
		[]core.Operation{op},
	)

	if !errors.Is(err, ErrRunnerBusy) {
		t.Fatalf("got %v, want ErrRunnerBusy", err)
	}
	err = runner.SetTimeout(time.Second)

	if !errors.Is(err, ErrRunnerBusy) {
		t.Fatalf("got %v, want ErrRunnerBusy", err)
	}

	cancel()

	select {
	case result := <-finished:
		if result.err != nil {
			t.Fatal(result.err)
		}

		if len(result.results) != 1 {
			t.Fatal("expected one result")
		}

		if result.results[0].State != core.StateCancelled {
			t.Fatalf(
				"unexpected state: %s",
				result.results[0].State,
			)
		}

	case <-time.After(2 * time.Second):
		t.Fatal("runner did not finish")
	}

	if err := runner.SetTimeout(time.Second); err != nil {
		t.Fatal(err)
	}

	active, queued, used := controller.Snapshot()

	if active != 0 || queued != 0 || used != 0 {
		t.Fatal("capacity leak")
	}
}
