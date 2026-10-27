package execution

import (
	"context"
	"errors"
	"pondus-gateway/internal/core"
	"testing"
	"time"
)

type stubExecutor struct {
	err      error
	received core.Operation
}

func (stub *stubExecutor) Execute(_ context.Context, operation core.Operation) error {
	stub.received = operation
	return stub.err
}

// Verifies that stubExecutor implements Executor
var _ Executor = (*stubExecutor)(nil)

func TestExecutorContract(t *testing.T) {
	expected := errors.New("execution failed")

	stub := &stubExecutor{
		err: expected,
	}

	var executor Executor = stub

	operation := core.Operation{
		ID:       "op-1",
		ClientID: "client-1",
		Type:     "compute",
		Cost:     3,
	}

	err := executor.Execute(context.Background(), operation)

	if !errors.Is(err, expected) {
		t.Fatalf(
			"got error %v, want %v",
			err, expected,
		)
	}

	if stub.received != operation {
		t.Fatalf(
			"received %+v, want %+v",
			stub.received, operation,
		)
	}
}

// It is verified that all three types implement Executor
var (
	_ Executor = SleepExecutor{}
	_ Executor = CPUExecutor{}
	_ Executor = FailingExecutor{}
)

func TestSleepExecutor(t *testing.T) {
	op := core.Operation{ID: "op-1"}

	// Correct execution
	err := (SleepExecutor{
		Duration: time.Millisecond,
	}).Execute(context.Background(), op)

	if err != nil {
		t.Fatal(err)
	}

	// Invalid duration
	err = (SleepExecutor{}).Execute(context.Background(), op)

	if !errors.Is(err, ErrInvalidDuration) {
		t.Fatalf(
			"got %v, want ErrInvalidDuration",
			err,
		)
	}

	// Context canceled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = (SleepExecutor{
		Duration: time.Hour,
	}).Execute(ctx, op)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf(
			"got %v, want context.Canceled",
			err,
		)
	}

	// Timeout
	ctx, cancel = context.WithTimeout(
		context.Background(),
		time.Millisecond,
	)
	defer cancel()

	err = (SleepExecutor{
		Duration: time.Second,
	}).Execute(ctx, op)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf(
			"got %v, want context.DeadlineExceeded",
			err,
		)
	}
}

func TestCPUExecutor(t *testing.T) {
	operation := core.Operation{ID: "op-1"}

	// Correct execution
	err := (CPUExecutor{
		Iterations: 1000,
	}).Execute(context.Background(), operation)

	if err != nil {
		t.Fatal(err)
	}

	// Invalid number of iterations
	err = (CPUExecutor{}).Execute(context.Background(), operation)

	if !errors.Is(err, ErrInvalidIterations) {
		t.Fatalf(
			"got %v, want ErrInvalidIterations",
			err,
		)
	}

	// Context canceled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = (CPUExecutor{
		Iterations: 1000,
	}).Execute(ctx, operation)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf(
			"got %v, want context.Canceled",
			err,
		)
	}
}

func TestFailingExecutor(t *testing.T) {
	operation := core.Operation{ID: "op-1"}
	failure := errors.New("controlled failure")

	// Configuration error
	err := (FailingExecutor{
		Err: failure,
	}).Execute(context.Background(), operation)

	if !errors.Is(err, failure) {
		t.Fatalf(
			"got %v, want controlled failure",
			err,
		)
	}

	// Invalid configuration
	err = (FailingExecutor{}).Execute(
		context.Background(),
		operation,
	)

	if !errors.Is(err, ErrMissingFailure) {
		t.Fatalf(
			"got %v, want ErrMissingFailure",
			err,
		)
	}

	// Context canceled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = (FailingExecutor{
		Err: failure,
	}).Execute(ctx, operation)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf(
			"got %v, want context.Canceled",
			err,
		)
	}
}
