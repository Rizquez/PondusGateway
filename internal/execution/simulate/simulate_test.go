package simulate

import (
	"context"
	"errors"
	"pondus-gateway/internal/domain"
	"pondus-gateway/internal/execution"
	"testing"
	"time"
)

// It is verified that all three types implement Executor
var (
	_ execution.Executor = SleepExecutor{}
	_ execution.Executor = CPUExecutor{}
	_ execution.Executor = FailingExecutor{}
)

func TestSleepExecutor(t *testing.T) {
	operation := domain.Operation{ID: "op-1"}

	// Correct execution
	err := (SleepExecutor{
		Duration: time.Millisecond,
	}).Execute(context.Background(), operation)

	if err != nil {
		t.Fatal(err)
	}

	// Invalid duration
	err = (SleepExecutor{}).Execute(context.Background(), operation)

	if !errors.Is(err, ErrInvalidDuration) {
		t.Fatalf("got %v, want ErrInvalidDuration", err)
	}

	// Context canceled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = (SleepExecutor{
		Duration: time.Hour,
	}).Execute(ctx, operation)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}

	// Timeout
	ctx, cancel = context.WithTimeout(
		context.Background(),
		time.Millisecond,
	)
	defer cancel()

	err = (SleepExecutor{
		Duration: time.Second,
	}).Execute(ctx, operation)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want context.DeadlineExceeded", err)
	}
}

func TestCPUExecutor(t *testing.T) {
	operation := domain.Operation{ID: "op-1"}

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
		t.Fatalf("got %v, want ErrInvalidIterations", err)
	}

	// Context canceled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = (CPUExecutor{
		Iterations: 1000,
	}).Execute(ctx, operation)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}

func TestFailingExecutor(t *testing.T) {
	operation := domain.Operation{ID: "op-1"}
	failure := errors.New("controlled failure")

	// Configuration error
	err := (FailingExecutor{
		Err: failure,
	}).Execute(context.Background(), operation)

	if !errors.Is(err, failure) {
		t.Fatalf("got %v, want controlled failure", err)
	}

	// Invalid configuration
	err = (FailingExecutor{}).Execute(
		context.Background(),
		operation,
	)

	if !errors.Is(err, ErrMissingFailure) {
		t.Fatalf("got %v, want ErrMissingFailure", err)
	}

	// Context canceled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = (FailingExecutor{
		Err: failure,
	}).Execute(ctx, operation)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}
