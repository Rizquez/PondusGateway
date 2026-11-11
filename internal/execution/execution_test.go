package execution

import (
	"context"
	"errors"
	"pondus-gateway/internal/control/admission"
	"pondus-gateway/internal/control/scheduler"
	"pondus-gateway/internal/domain"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- executor_test ---
type stubExecutor struct {
	err      error
	received domain.Operation
}

func (stub *stubExecutor) Execute(_ context.Context, operation domain.Operation) error {
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

	operation := domain.Operation{
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

// --- runner_test ---
type countingExecutor struct {
	active atomic.Int64
	max    atomic.Int64
}

func (counting *countingExecutor) Execute(ctx context.Context, _ domain.Operation) error {
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
	controller, err := admission.New(domain.Limits{
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

	var operations []domain.Operation

	for i := range 7 {
		operations = append(operations, domain.Operation{
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
	controller, err := admission.New(domain.Limits{
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

	operations := []domain.Operation{
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
	controller, err := admission.New(domain.Limits{
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

	operations := []domain.Operation{
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

func (executor lifecycleTestExecutor) Execute(ctx context.Context, _ domain.Operation) error {
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
		wantState domain.State
		wantErr   error
	}{
		{
			name:      "completed",
			wantState: domain.StateCompleted,
		},
		{
			name: "failed",
			executor: lifecycleTestExecutor{
				err: errors.New("execution failed"),
			},
			wantState: domain.StateFailed,
		},
		{
			name: "timeout",
			executor: lifecycleTestExecutor{
				delay: time.Second,
			},
			timeout:   time.Millisecond,
			wantState: domain.StateFailed,
			wantErr:   context.DeadlineExceeded,
		},
		{
			name:      "cancelled before start",
			cancelled: true,
			wantState: domain.StateCancelled,
			wantErr:   context.Canceled,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			controller, err := admission.New(
				domain.Limits{
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
				[]domain.Operation{
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

func (executor *cancellableExecutor) Execute(ctx context.Context, _ domain.Operation) error {
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

	controller, err := admission.New(domain.Limits{
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

func cancellationTestOp(id string) domain.Operation {
	return domain.Operation{
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
		[]domain.Operation{
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
			[]domain.Operation{
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

func (invalidIndexSelector) Next(_ []domain.Operation, _ uint64) int {
	return 999
}

type cleanupTestExecutor struct{}

func (cleanupTestExecutor) Execute(ctx context.Context, operation domain.Operation) error {
	if operation.ID == "a" {
		return nil
	}

	<-ctx.Done()
	return ctx.Err()
}

func TestRunnerCleanupOnInternalError(t *testing.T) {
	controller, err := admission.NewWithSelector(
		domain.Limits{
			MaxConcurrent: 2,
			MaxCapacity:   2,
			MaxQueueSize:  1,
		},
		invalidIndexSelector{},
	)
	if err != nil {
		t.Fatal(err)
	}

	owner, err := controller.Acquire()

	if err != nil {
		t.Fatalf("controller remained owned after cleanup: %v", err)
	}

	if err := owner.Release(); err != nil {
		t.Fatalf("release reacquired session: %v", err)
	}

	runner, err := New(controller, cleanupTestExecutor{})
	if err != nil {
		t.Fatal(err)
	}

	operations := []domain.Operation{
		{ID: "a", ClientID: "x", Type: "test", Cost: 1},
		{ID: "b", ClientID: "x", Type: "test", Cost: 1},
		{ID: "c", ClientID: "x", Type: "test", Cost: 1},
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		2*time.Second,
	)
	defer cancel()

	results, err := runner.Run(ctx, operations)

	if err == nil {
		t.Fatal("expected internal error")
	}

	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("runner only stopped because test timed out")
	}

	if len(results) != 3 {
		t.Fatalf("got %d results, want 3", len(results))
	}

	if results[0].State != domain.StateCompleted {
		t.Fatalf("unexpected state for a: %s", results[0].State)
	}

	if results[1].State != domain.StateCancelled {
		t.Fatalf("unexpected state for b: %s", results[1].State)
	}

	if results[2].State != domain.StateCancelled {
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

func (executor *blockingExecutor) Execute(ctx context.Context, _ domain.Operation) error {
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
	controller, err := admission.New(domain.Limits{
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

	operation := domain.Operation{
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
			[]domain.Operation{operation},
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
		[]domain.Operation{operation},
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

		if result.results[0].State != domain.StateCancelled {
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

func (neverPromoteSelector) Next(_ []domain.Operation, _ uint64) int {
	return -1
}

var _ scheduler.Selector = neverPromoteSelector{}

func TestRunnerNoProgress(t *testing.T) {
	controller, err := admission.NewWithSelector(
		domain.Limits{
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

	operations := []domain.Operation{
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

	if results[0].State != domain.StateCompleted {
		t.Fatalf("operation a: got state %s, want completed", results[0].State)
	}

	if results[1].State != domain.StateCancelled {
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

// --- owner_executor ---
type ownershipExecutor struct {
	started chan struct{}
	release chan struct{}
}

func (executor *ownershipExecutor) Execute(_ context.Context, _ domain.Operation) error {
	close(executor.started)
	<-executor.release
	return nil
}

func TestRunnerOwnsController(t *testing.T) {
	controller, err := admission.New(domain.Limits{MaxConcurrent: 1, MaxCapacity: 1, MaxQueueSize: 2})
	if err != nil {
		t.Fatal(err)
	}

	executor := &ownershipExecutor{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	runner, err := New(controller, executor)
	if err != nil {
		t.Fatal(err)
	}

	finished := make(chan error, 1)
	go func() {
		_, err := runner.Run(
			context.Background(),
			[]domain.Operation{{
				ID:       "owned",
				ClientID: "test",
				Type:     "compute",
				Cost:     1,
			}},
		)
		finished <- err
	}()

	select {
	case <-executor.started:
	case <-time.After(2 * time.Second):
		t.Fatal("executor did not start")
	}

	external := domain.Operation{
		ID:       "external",
		ClientID: "test",
		Type:     "compute",
		Cost:     1,
	}

	if decision, err := controller.Submit(external); decision != admission.Rejected || !errors.Is(err, admission.ErrControllerBusy) {
		t.Fatalf(
			"external Submit = (%v,%v); want (Rejected,ErrControllerBusy)",
			decision, err,
		)
	}

	if cancelled := controller.CancelPending(); len(cancelled) != 0 {
		t.Fatalf("external cancellation changed pending queue: %v", cancelled)
	}

	close(executor.release)

	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runner did not finish")
	}

	if active, queued, used := controller.Snapshot(); active != 0 || queued != 0 || used != 0 {
		t.Fatalf(
			"leaked state: active=%d queued=%d used=%d",
			active, queued, used,
		)
	}

	if decision, err := controller.Submit(external); decision != admission.Started || err != nil {
		t.Fatalf(
			"controller did not become available: %v, %v",
			decision, err,
		)
	}

	if err := controller.FinishWithoutPromotion(external.ID); err != nil {
		t.Fatal(err)
	}
}

// --- characterization of partial errors  ---

type promotionThenInvalidSelector struct {
	calls int
}

type partialPromotionExecutor struct {
	executed   atomic.Int32
	unexpected atomic.Int32
}

func (selector *promotionThenInvalidSelector) Next(pending []domain.Operation, _ uint64) int {
	selector.calls++
	if selector.calls == 1 {
		return 1
	}
	return len(pending)
}

func (executor *partialPromotionExecutor) Execute(_ context.Context, operation domain.Operation) error {
	executor.executed.Add(1)
	if operation.ID != "a" {
		executor.unexpected.Add(1)
	}
	return nil
}

func TestRunnerPartialPromotionFailure(t *testing.T) {
	selector := &promotionThenInvalidSelector{}

	controller, err := admission.NewWithSelector(domain.Limits{
		MaxConcurrent: 3,
		MaxCapacity:   4,
		MaxQueueSize:  3,
	}, selector)
	if err != nil {
		t.Fatal(err)
	}

	executor := &partialPromotionExecutor{}
	runner, err := New(controller, executor)
	if err != nil {
		t.Fatal(err)
	}

	operations := []domain.Operation{
		{ID: "a", ClientID: "test", Type: "work", Cost: 4},
		{ID: "b", ClientID: "test", Type: "work", Cost: 3},
		{ID: "c", ClientID: "test", Type: "work", Cost: 1},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

	defer cancel()

	results, err := runner.Run(ctx, operations)

	if err == nil || !strings.Contains(err.Error(), "scheduler returned invalid index") {
		t.Fatalf("expected scheduler error, got %v", err)
	}

	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("runner ended only because of timeout: %v", err)
	}

	if selector.calls != 2 {
		t.Fatalf("selector calls = %d, want 2", selector.calls)
	}

	if executor.executed.Load() != 1 || executor.unexpected.Load() != 0 {
		t.Fatalf(
			"executor calls = %d, unexpected = %d; want (1,0)",
			executor.executed.Load(), executor.unexpected.Load(),
		)
	}

	if len(results) != len(operations) {
		t.Fatalf(
			"results = %d, want %d",
			len(results), len(operations),
		)
	}

	for index, want := range []struct {
		decision admission.Decision
		state    domain.State
	}{
		{admission.Started, domain.StateCompleted},
		{admission.Queued, domain.StateCancelled},
		{admission.Queued, domain.StateCancelled},
	} {
		got := results[index]
		if got.OperationID != operations[index].ID || got.Decision != want.decision || got.State != want.state {
			t.Errorf(
				"result[%d] = %+v, want id=%q decision=%q state=%q",
				index, got, operations[index].ID, want.decision, want.state,
			)
		}
	}

	if active, queued, used := controller.Snapshot(); active != 0 || queued != 0 || used != 0 {
		t.Fatalf(
			"controller after cleanup = (%d,%d,%d), want (0,0,0)",
			active, queued, used,
		)
	}

	owner, err := controller.Acquire()
	if err != nil {
		t.Fatalf("controller not available after cleanup: %v", err)
	}

	if err := owner.Release(); err != nil {
		t.Fatalf("releasing newly acquired session: %v", err)
	}
}

func TestRunnerCancelledBeforeLaunchReleasesSession(t *testing.T) {
	controller, err := admission.New(domain.Limits{
		MaxConcurrent: 1,
		MaxCapacity:   1,
		MaxQueueSize:  3,
	})
	if err != nil {
		t.Fatal(err)
	}

	executor := &partialPromotionExecutor{}
	runner, err := New(controller, executor)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	cancel()

	operations := []domain.Operation{
		{ID: "a", ClientID: "test", Type: "work", Cost: 1},
		{ID: "b", ClientID: "test", Type: "work", Cost: 1},
		{ID: "c", ClientID: "test", Type: "work", Cost: 1},
	}

	results, err := runner.Run(ctx, operations)

	if err != nil {
		t.Fatalf("pre-launch cancellation: %v", err)
	}

	if executor.executed.Load() != 0 {
		t.Fatalf("unexpected executor activity: %d", executor.executed.Load())
	}

	for index, result := range results {
		if result.OperationID != operations[index].ID || result.State != domain.StateCancelled || !errors.Is(result.Err, context.Canceled) {
			t.Errorf(
				"result[%d] = %+v, want cancelled %q",
				index, result, operations[index].ID,
			)
		}
	}

	if active, queued, used := controller.Snapshot(); active != 0 || queued != 0 || used != 0 {
		t.Fatalf(
			"controller after cancellation = (%d,%d,%d)",
			active, queued, used,
		)
	}

	owner, err := controller.Acquire()
	if err != nil {
		t.Fatalf("session not released after cancellation: %v", err)
	}

	if err := owner.Release(); err != nil {
		t.Fatal(err)
	}
}

// --- cancellation and uncooperative executors ---

type nonCooperativeExecutor struct {
	started  chan struct{}
	canceled chan struct{}
	release  chan struct{}
}

func (executor *nonCooperativeExecutor) Execute(ctx context.Context, _ domain.Operation) error {
	close(executor.started)
	<-ctx.Done()
	close(executor.canceled)
	<-executor.release
	return nil // Deliberately ignores ctx.Err()
}

func setup(t *testing.T) (*admission.Controller, *Runner, *nonCooperativeExecutor, func()) {
	t.Helper()

	controller, err := admission.New(domain.Limits{
		MaxConcurrent: 1,
		MaxCapacity:   1,
		MaxQueueSize:  0,
	})
	if err != nil {
		t.Fatal(err)
	}

	executor := &nonCooperativeExecutor{
		started:  make(chan struct{}),
		canceled: make(chan struct{}),
		release:  make(chan struct{}),
	}

	var once sync.Once
	unblock := func() {
		once.Do(func() {
			close(executor.release)
		})
	}

	t.Cleanup(unblock) // Never leave an executor blocked if an assertion fails

	runner, err := New(controller, executor)
	if err != nil {
		t.Fatal(err)
	}

	return controller, runner, executor, unblock
}

func wait(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatalf("timeout pending %s", what)
	}
}

func assertReserved(t *testing.T, controller *admission.Controller) {
	t.Helper()

	active, queued, used := controller.Snapshot()
	if active != 1 || queued != 0 || used != 1 {
		t.Fatalf(
			"premature reservation: active=%d queued=%d used=%d; esperado 1,0,1",
			active, queued, used,
		)
	}
}

func assertAvailable(t *testing.T, controller *admission.Controller) {
	t.Helper()

	active, queued, used := controller.Snapshot()
	if active != 0 || queued != 0 || used != 0 {
		t.Fatalf(
			"residual state: active=%d queued=%d used=%d",
			active, queued, used,
		)
	}

	owner, err := controller.Acquire()
	if err != nil {
		t.Fatalf("the property was not released: %v", err)
	}

	if err := owner.Release(); err != nil {
		t.Fatalf("the new session could not be closed: %v", err)
	}
}

type outcome struct {
	results []Result
	err     error
}

func start(runner *Runner, ctx context.Context) <-chan outcome {
	finished := make(chan outcome, 1)
	go func() {
		results, err := runner.Run(ctx, []domain.Operation{{
			ID:       "aud01-a",
			ClientID: "test",
			Type:     "test",
			Cost:     1,
		}})
		finished <- outcome{results: results, err: err}
	}()
	return finished
}

func assertNotFinished(t *testing.T, finished <-chan outcome) {
	t.Helper()

	select {
	case result := <-finished:
		t.Fatalf("Run ended before the executor returned control: %+v", result)
	case <-time.After(30 * time.Millisecond):
		// Note: This is a limited observation that is still pending; it does not verify the absence of deadlocks
	}
}

func finish(t *testing.T, finished <-chan outcome) {
	t.Helper()

	select {
	case outcome := <-finished:
		if outcome.err != nil {
			t.Fatalf("Run returned an error: %v", outcome.err)
		}
		if len(outcome.results) != 1 || outcome.results[0].State != domain.StateCompleted {
			t.Fatalf("unexpected result: %+v", outcome.results)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Runner did not complete after the executor was unlocked")
	}
}

func TestRunnerWaitsForNonCooperativeExecutor(t *testing.T) {
	controller, runner, executor, unblock := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := start(runner, ctx)
	wait(t, executor.started, "start of the executor")
	cancel()
	wait(t, executor.canceled, "cancellation noted by the executor")

	assertReserved(t, controller)
	assertNotFinished(t, finished)
	if _, err := runner.Run(context.Background(), nil); !errors.Is(err, ErrRunnerBusy) {
		t.Fatalf("Second execution: error=%v; expected ErrRunnerBusy", err)
	}
	if err := runner.SetTimeout(time.Second); !errors.Is(err, ErrRunnerBusy) {
		t.Fatalf("SetTimeout during execution: error=%v; expected ErrRunnerBusy", err)
	}
	if _, err := controller.Submit(domain.Operation{
		ID:       "external",
		ClientID: "test",
		Type:     "test", Cost: 1,
	}); !errors.Is(err, admission.ErrControllerBusy) {
		t.Fatalf("External submit: error=%v; expected ErrControllerBusy", err)
	}

	unblock()
	finish(t, finished)
	assertAvailable(t, controller)
}

func TestRunnerTimeoutRequiresExecutorCooperation(t *testing.T) {
	controller, runner, executor, unblock := setup(t)
	if err := runner.SetTimeout(20 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	finished := start(runner, context.Background())
	wait(t, executor.started, "start of the executor")
	wait(t, executor.canceled, "executor's deadline")

	assertReserved(t, controller)
	assertNotFinished(t, finished)
	unblock()
	finish(t, finished)
	assertAvailable(t, controller)
}

// --- semantics and error propagation ---

func TestRunnerDeadlineIsOperationFailure(t *testing.T) {
	controller, err := admission.New(domain.Limits{
		MaxConcurrent: 1,
		MaxCapacity:   1,
		MaxQueueSize:  0,
	})
	if err != nil {
		t.Fatal(err)
	}

	runner, err := New(controller, &stubExecutor{err: context.DeadlineExceeded})
	if err != nil {
		t.Fatal(err)
	}

	results, runErr := runner.Run(context.Background(), []domain.Operation{{
		ID:       "timeout",
		ClientID: "test",
		Type:     "work",
		Cost:     1,
	}})

	if runErr != nil {
		t.Fatalf("Run must keep the individual failure separate: %v", runErr)
	}

	if len(results) != 1 ||
		results[0].Decision != admission.Started ||
		results[0].State != domain.StateFailed ||
		!errors.Is(results[0].Err, context.DeadlineExceeded) {
		t.Fatalf("unexpected timeout error: %+v", results)
	}
}

func TestRunnerBatchCancellationDoesNotFailRun(t *testing.T) {
	controller, err := admission.New(domain.Limits{
		MaxConcurrent: 1,
		MaxCapacity:   1,
		MaxQueueSize:  1,
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
	results, runErr := runner.Run(ctx, []domain.Operation{
		{ID: "a", ClientID: "test", Type: "work", Cost: 1},
		{ID: "b", ClientID: "test", Type: "work", Cost: 1},
	})

	if runErr != nil {
		t.Fatalf("the requested cancellation should not be a motor error: %v", runErr)
	}

	if len(results) != 2 {
		t.Fatalf("results=%d, expected 2", len(results))
	}

	for _, got := range results {
		if got.State != domain.StateCancelled || !errors.Is(got.Err, context.Canceled) {
			t.Errorf("operation %q: %+v", got.OperationID, got)
		}
	}
}

func TestRunnerRejectedOperationHasNoExecutionState(t *testing.T) {
	controller, err := admission.New(domain.Limits{
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

	results, runErr := runner.Run(context.Background(), []domain.Operation{
		{ID: "a", ClientID: "test", Type: "work", Cost: 1},
		{ID: "b", ClientID: "test", Type: "work", Cost: 1},
	})

	if runErr != nil {
		t.Fatal(runErr)
	}

	if len(results) != 2 ||
		results[1].OperationID != "b" ||
		results[1].Decision != admission.Rejected ||
		results[1].State != "" ||
		results[1].Err != nil {
		t.Fatalf("unexpected rejection: %+v", results)
	}
}

func TestRunnerInternalErrorPreservesIndividualResults(t *testing.T) {
	controller, err := admission.NewWithSelector(domain.Limits{
		MaxConcurrent: 2,
		MaxCapacity:   2,
		MaxQueueSize:  1,
	}, invalidIndexSelector{})

	if err != nil {
		t.Fatal(err)
	}

	runner, err := New(controller, cleanupTestExecutor{})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	results, runErr := runner.Run(ctx, []domain.Operation{
		{ID: "a", ClientID: "test", Type: "work", Cost: 1},
		{ID: "b", ClientID: "test", Type: "work", Cost: 1},
		{ID: "c", ClientID: "test", Type: "work", Cost: 1},
	})

	if runErr == nil || !strings.Contains(runErr.Error(), "scheduler returned invalid index") {
		t.Fatalf("internal scheduler error: %v", runErr)
	}

	if len(results) != 3 {
		t.Fatalf("results=%d, expected 3", len(results))
	}

	if results[0].OperationID != "a" ||
		results[0].State != domain.StateCompleted ||
		results[0].Err != nil {
		t.Fatalf("the completed result was lost: %+v", results[0])
	}

	if results[1].OperationID != "b" ||
		results[1].State != domain.StateCancelled ||
		!errors.Is(results[1].Err, context.Canceled) {
		t.Fatalf("the executor's error was lost: %+v", results[1])
	}

	if results[2].OperationID != "c" || results[2].State != domain.StateCancelled || results[2].Err == nil {
		t.Fatalf("the reason for the cancellation was lost during cleanup: %+v", results[2])
	}

	if active, queued, used := controller.Snapshot(); active != 0 || queued != 0 || used != 0 {
		t.Fatalf(
			"controller not recovered: active=%d queued=%d used=%d",
			active, queued, used,
		)
	}
}
