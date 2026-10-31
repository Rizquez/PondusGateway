package execution

import (
	"context"
	"errors"
	"pondus-gateway/internal/admission"
	"pondus-gateway/internal/core"
	"pondus-gateway/internal/scheduler"
	"sync/atomic"
	"testing"
	"time"
)

// --- executor_test ---
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
	operation := core.Operation{ID: "op-1"}

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
	operation := core.Operation{ID: "op-1"}
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

// --- runner_test ---
type countingExecutor struct {
	active atomic.Int64
	max    atomic.Int64
}

func (counting *countingExecutor) Execute(ctx context.Context, _ core.Operation) error {
	now := counting.active.Add(1)
	defer counting.active.Add(-1)

	for {
		old := counting.max.Load()
		if now <= old || counting.max.CompareAndSwap(old, now) {
			break
		}
	}

	select {
	case <-time.After(time.Millisecond):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestRunnerConcurrentExecution(t *testing.T) {
	controller, err := admission.New(core.Limits{
		MaxConcurrent: 2,
		MaxCapacity:   3,
		MaxQueueSize:  5,
	})
	if err != nil {
		t.Fatal(err)
	}

	counting := &countingExecutor{}

	runner, err := New(controller, counting)
	if err != nil {
		t.Fatal(err)
	}

	var operations []core.Operation

	for i := range 7 {
		operations = append(operations, core.Operation{
			ID:       string(rune('a' + i)),
			ClientID: "client-1",
			Type:     "compute",
			Cost:     1,
		})
	}

	results, err := runner.Run(context.Background(), operations)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != len(operations) {
		t.Fatalf(
			"results=%d, want=%d",
			len(results), len(operations),
		)
	}

	for index, result := range results {
		if result.Err != nil ||
			result.Decision == admission.Rejected ||
			result.OperationID != operations[index].ID {
			t.Fatalf("unexpected result: %+v", result)
		}
	}

	if counting.max.Load() > 2 {
		t.Fatal("concurrency limit exceeded")
	}

	active, queued, used := controller.Snapshot()

	if active != 0 || queued != 0 || used != 0 {
		t.Fatalf(
			"capacity leak: active=%d queued=%d used=%d",
			active, queued, used,
		)
	}
}

func TestRunnerRejectedOperation(t *testing.T) {
	controller, err := admission.New(core.Limits{
		MaxConcurrent: 1,
		MaxCapacity:   1,
		MaxQueueSize:  0,
	})

	if err != nil {
		t.Fatal(err)
	}

	runner, err := New(controller, &countingExecutor{})
	if err != nil {
		t.Fatal(err)
	}

	operations := []core.Operation{
		{ID: "a", ClientID: "x", Type: "ping", Cost: 1},
		{ID: "b", ClientID: "x", Type: "ping", Cost: 1},
	}

	results, err := runner.Run(context.Background(), operations)
	if err != nil {
		t.Fatal(err)
	}

	if results[0].Decision != admission.Started ||
		results[1].Decision != admission.Rejected {
		t.Fatalf("unexpected decisions: %+v", results)
	}
}

func TestRunnerCancelledContext(t *testing.T) {
	controller, err := admission.New(core.Limits{
		MaxConcurrent: 1,
		MaxCapacity:   1,
		MaxQueueSize:  2,
	})

	if err != nil {
		t.Fatal(err)
	}

	runner, err := New(controller, &countingExecutor{})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	operations := []core.Operation{
		{ID: "a", ClientID: "x", Type: "ping", Cost: 1},
		{ID: "b", ClientID: "x", Type: "ping", Cost: 1},
	}

	results, err := runner.Run(ctx, operations)
	if err != nil {
		t.Fatal(err)
	}

	for _, result := range results {
		if !errors.Is(result.Err, context.Canceled) {
			t.Fatalf(
				"operation %s: got %v, want context.Canceled",
				result.OperationID, result.Err,
			)
		}
	}

	active, queued, used := controller.Snapshot()

	if active != 0 || queued != 0 || used != 0 {
		t.Fatal("capacity was not fully released")
	}
}

// --- runner_lifecycle_test ---
type lifecycleTestExecutor struct {
	err   error
	delay time.Duration
}

func (executor lifecycleTestExecutor) Execute(ctx context.Context, _ core.Operation) error {
	if executor.delay > 0 {
		timer := time.NewTimer(executor.delay)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}

	return executor.err
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

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
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

			runner, err := New(controller, test.executor)
			if err != nil {
				t.Fatal(err)
			}

			if err := runner.SetTimeout(test.timeout); err != nil {
				t.Fatal(err)
			}

			ctx, cancel := context.WithCancel(
				context.Background(),
			)
			defer cancel()

			if test.cancelled {
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
				t.Fatalf("got %d results, want 1", len(results))
			}

			result := results[0]

			if result.State != test.wantState {
				t.Fatalf(
					"got state %s, want %s",
					result.State, test.wantState,
				)
			}

			if test.wantErr != nil &&
				!errors.Is(result.Err, test.wantErr) {
				t.Fatalf(
					"got error %v, want %v",
					result.Err, test.wantErr,
				)
			}

			active, queued, used := controller.Snapshot()

			if active != 0 || queued != 0 || used != 0 {
				t.Fatal("capacity was not fully released")
			}
		})
	}
}

// --- runner_cancellation_test ---
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
			t.Fatalf("unexpected result: %+v", result)
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

	ctx, cancel := context.WithCancel(context.Background())
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
				t.Fatalf("unexpected result: %+v", result)
			}
		}

	case <-time.After(time.Second):
		t.Fatal("runner did not finish")
	}

	if exec.runs.Load() != 1 {
		t.Fatalf("executed %d operations, want 1", exec.runs.Load())
	}

	active, queued, used := controller.Snapshot()
	if active != 0 || queued != 0 || used != 0 {
		t.Fatal("capacity leak after cancellation")
	}
}

// --- runner_cleanup_test ---
type invalidIndexSelector struct{}

func (invalidIndexSelector) Next(_ []core.Operation, _ uint64) int {
	return 999
}

type cleanupTestExecutor struct{}

func (cleanupTestExecutor) Execute(ctx context.Context, operation core.Operation) error {
	if operation.ID == "a" {
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

// --- runner_exclusivity_test ---
type blockingExecutor struct {
	started chan struct{}
	release chan struct{}
}

func (executor *blockingExecutor) Execute(ctx context.Context, _ core.Operation) error {
	select {
	case executor.started <- struct{}{}:
	default:
	}

	select {
	case <-executor.release:
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

	operation := core.Operation{
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
			[]core.Operation{operation},
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
		[]core.Operation{operation},
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
			t.Fatalf("unexpected state: %s", result.results[0].State)
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

// --- runner_progress_test ---
type neverPromoteSelector struct{}

func (neverPromoteSelector) Next(_ []core.Operation, _ uint64) int {
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

	ctx := t.Context()

	results, err := runner.Run(ctx, operations)

	if !errors.Is(err, ErrNoProgress) {
		t.Fatalf("got error %v, want ErrNoProgress", err)
	}

	if len(results) != 2 {
		t.Fatalf("got %d results, want 2", len(results))
	}

	if results[0].State != core.StateCompleted {
		t.Fatalf("operation a: got state %s, want completed", results[0].State)
	}

	if results[1].State != core.StateCancelled {
		t.Fatalf("operation b: got state %s, want cancelled", results[1].State)
	}

	if !errors.Is(results[1].Err, ErrNoProgress) {
		t.Fatalf("operation b: got error %v", results[1].Err)
	}

	active, queued, used := controller.Snapshot()

	if active != 0 || queued != 0 || used != 0 {
		t.Fatalf(
			"capacity leak: active=%d queued=%d used=%d",
			active, queued, used,
		)
	}
}
