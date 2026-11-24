package continuous

import (
	"context"
	"errors"
	"fmt"
	"pondus-gateway/internal/control/admission"
	"pondus-gateway/internal/domain"
	"pondus-gateway/internal/execution"
	"strings"
	"sync"
	"testing"
	"time"
)

type blockingExecutor struct {
	started      chan string
	release      chan struct{}
	mutex        sync.Mutex
	calls        map[string]int
	ignoreCancel bool
}

func (executor *blockingExecutor) Execute(ctx context.Context, operation domain.Operation) error {
	executor.mutex.Lock()
	executor.calls[operation.ID]++
	executor.mutex.Unlock()

	executor.started <- operation.ID

	if executor.ignoreCancel {
		<-executor.release
		return ctx.Err()
	}

	select {
	case <-executor.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func newBlockingExecutor(bufferSize int, ignoreCancellation bool) *blockingExecutor {
	return &blockingExecutor{
		started:      make(chan string, bufferSize),
		release:      make(chan struct{}),
		calls:        make(map[string]int),
		ignoreCancel: ignoreCancellation,
	}
}

func newTestCoordinator(
	t *testing.T,
	limits domain.Limits,
	executor execution.Executor,
) (*Coordinator, *admission.Controller) {
	t.Helper()

	controller, err := admission.New(limits)
	if err != nil {
		t.Fatal(err)
	}

	coordinator, err := New(controller, executor)
	if err != nil {
		t.Fatal(err)
	}

	if err := coordinator.Start(); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		if err := coordinator.Close(ctx); err != nil {
			t.Errorf("Coordinator.Close() = %v", err)
		}
	})

	return coordinator, controller
}

func testOperation(operationID string, cost uint64) domain.Operation {
	return domain.Operation{
		ID:       operationID,
		ClientID: "client-test",
		Type:     "sleep",
		Cost:     cost,
	}
}

func assertCoordinatorSnapshot(
	t *testing.T,
	controller *admission.Controller,
	wantActive int,
	wantQueued int,
	wantUsed uint64,
) {
	t.Helper()

	active, queued, used := controller.Snapshot()
	if active != wantActive || queued != wantQueued || used != wantUsed {
		t.Fatalf(
			"Snapshot() = (active=%d, queued=%d, used=%d), want (%d, %d, %d)",
			active, queued, used, wantActive, wantQueued, wantUsed,
		)
	}
}

func assertOperationStarted(t *testing.T, started <-chan string, wantOperationID string) {
	t.Helper()

	select {
	case operationID := <-started:
		if operationID != wantOperationID {
			t.Fatalf("started operation = %q, want %q", operationID, wantOperationID)
		}
	case <-time.After(time.Second):
		t.Fatalf("operation %q did not start", wantOperationID)
	}
}

func awaitOperationResult(t *testing.T, results <-chan Result) Result {
	t.Helper()

	select {
	case result, open := <-results:
		if !open {
			t.Fatal("result channel closed without a result")
		}
		return result
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for an operation result")
	}

	return Result{}
}

func TestCoordinatorContinuousAdmissionAndPromotion(t *testing.T) {
	executor := newBlockingExecutor(8, false)
	coordinator, controller := newTestCoordinator(t, domain.Limits{
		MaxConcurrent: 1,
		MaxCapacity:   2,
		MaxQueueSize:  2,
	}, executor)

	firstOperation := testOperation("first-operation", 2)
	secondOperation := testOperation("second-operation", 1)
	thirdOperation := testOperation("third-operation", 1)
	lastOperation := testOperation("rejected-operation", 1)

	firstDecision, firstResults, err := coordinator.Submit("owner-first", firstOperation)
	if err != nil || firstDecision != admission.Started {
		t.Fatalf("Submit(first) = (%s, %v), want Started", firstDecision, err)
	}
	assertOperationStarted(t, executor.started, firstOperation.ID)

	secondDecision, secondResults, err := coordinator.Submit("owner-second", secondOperation)
	if err != nil || secondDecision != admission.Queued {
		t.Fatalf("Submit(second) = (%s, %v), want Queued", secondDecision, err)
	}

	thirdDecision, thirdResults, err := coordinator.Submit("owner-first", thirdOperation)
	if err != nil || thirdDecision != admission.Queued {
		t.Fatalf("Submit(third) = (%s, %v), want Queued", thirdDecision, err)
	}

	rejectedDecision, rejectedResults, err := coordinator.Submit("owner-extra", lastOperation)
	if err != nil || rejectedDecision != admission.Rejected || rejectedResults != nil {
		t.Fatalf(
			"Submit(extra) = (%s, %v, %v), want Rejected and no result channel",
			rejectedDecision, rejectedResults, err,
		)
	}

	assertCoordinatorSnapshot(t, controller, 1, 2, 2)

	close(executor.release)

	firstResult := awaitOperationResult(t, firstResults)
	if firstResult.State != domain.StateCompleted || firstResult.OwnerID != "owner-first" {
		t.Fatalf("first result = %+v", firstResult)
	}

	assertOperationStarted(t, executor.started, secondOperation.ID)
	secondResult := awaitOperationResult(t, secondResults)
	if secondResult.State != domain.StateCompleted || secondResult.Decision != admission.Queued {
		t.Fatalf("second result = %+v", secondResult)
	}

	assertOperationStarted(t, executor.started, thirdOperation.ID)
	thirdResult := awaitOperationResult(t, thirdResults)
	if thirdResult.State != domain.StateCompleted {
		t.Fatalf("third result = %+v", thirdResult)
	}

	executor.mutex.Lock()
	defer executor.mutex.Unlock()

	for _, operationID := range []string{firstOperation.ID, secondOperation.ID, thirdOperation.ID} {
		if executor.calls[operationID] != 1 {
			t.Fatalf("Execute(%q) called %d times, want 1", operationID, executor.calls[operationID])
		}
	}
}

func TestCoordinatorCloseKeepsReservationUntilExecutionReturns(t *testing.T) {
	executor := newBlockingExecutor(2, true)
	coordinator, controller := newTestCoordinator(t, domain.Limits{
		MaxConcurrent: 1,
		MaxCapacity:   1,
		MaxQueueSize:  2,
	}, executor)

	runningOperation := testOperation("running-operation", 1)
	pendingOperation := testOperation("pending-operation", 1)

	_, runningResults, err := coordinator.Submit("running-owner", runningOperation)
	if err != nil {
		t.Fatal(err)
	}
	assertOperationStarted(t, executor.started, runningOperation.ID)

	_, pendingResults, err := coordinator.Submit("pending-owner", pendingOperation)
	if err != nil {
		t.Fatal(err)
	}

	closeContext, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	if err := coordinator.Close(closeContext); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close() = %v, want context.DeadlineExceeded", err)
	}

	assertCoordinatorSnapshot(t, controller, 1, 0, 1)

	pendingResult := awaitOperationResult(t, pendingResults)
	if pendingResult.State != domain.StateCancelled {
		t.Fatalf("pending result = %+v, want Cancelled", pendingResult)
	}

	if _, _, err := coordinator.Submit("new-owner", testOperation("new-operation", 1)); !errors.Is(err, ErrClosed) {
		t.Fatalf("Submit() after Close() = %v, want ErrClosed", err)
	}

	close(executor.release)

	runningResult := awaitOperationResult(t, runningResults)
	if runningResult.State != domain.StateCancelled {
		t.Fatalf("running result = %+v, want Cancelled", runningResult)
	}

	if err := coordinator.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	assertCoordinatorSnapshot(t, controller, 0, 0, 0)
}

func TestCoordinatorConcurrentSubmitGlobalCapacity(t *testing.T) {
	const totalOperations = 10

	executor := newBlockingExecutor(32, false)
	coordinator, controller := newTestCoordinator(t, domain.Limits{
		MaxConcurrent: 2,
		MaxCapacity:   2,
		MaxQueueSize:  20,
	}, executor)

	var submissions sync.WaitGroup
	resultChannels := make(chan (<-chan Result), totalOperations)

	for operationIndex := range totalOperations {
		submissions.Add(1)

		go func(index int) {
			defer submissions.Done()

			operationID := fmt.Sprintf("operation-%d", index)
			ownerID := fmt.Sprintf("owner-%d", index)

			_, results, err := coordinator.Submit(ownerID, testOperation(operationID, 1))
			if err != nil {
				t.Errorf("Submit(%q) = %v", operationID, err)
				return
			}

			resultChannels <- results
		}(operationIndex)
	}

	submissions.Wait()
	assertCoordinatorSnapshot(t, controller, 2, 8, 2)

	close(executor.release)

	for range totalOperations {
		results := <-resultChannels
		result := awaitOperationResult(t, results)
		if result.State != domain.StateCompleted {
			t.Fatalf("operation result = %+v, want Completed", result)
		}
	}
}

func TestCoordinatorStartAndValidation(t *testing.T) {
	if _, err := New(nil, nil); !errors.Is(err, ErrRequired) {
		t.Fatalf("New(nil, nil) = %v, want ErrRequired", err)
	}

	executor := newBlockingExecutor(1, false)
	controller, err := admission.New(domain.Limits{
		MaxConcurrent: 1,
		MaxCapacity:   1,
		MaxQueueSize:  1,
	})
	if err != nil {
		t.Fatal(err)
	}

	coordinator, err := New(controller, executor)
	if err != nil {
		t.Fatal(err)
	}

	if _, _, err := coordinator.Submit("owner", testOperation("operation", 1)); !errors.Is(err, ErrNotStarted) {
		t.Fatalf("Submit() before Start() = %v, want ErrNotStarted", err)
	}

	if err := coordinator.Start(); err != nil {
		t.Fatal(err)
	}

	if err := coordinator.Start(); !errors.Is(err, ErrStarted) {
		t.Fatalf("Start() twice = %v, want ErrStarted", err)
	}

	if _, _, err := coordinator.Submit("", testOperation("operation", 1)); !errors.Is(err, ErrOwnerID) {
		t.Fatalf("Submit() without owner = %v, want ErrOwnerID", err)
	}

	if err := coordinator.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCoordinatorUnreadResultAndIDReuse(t *testing.T) {
	executor := newBlockingExecutor(4, false)
	coordinator, _ := newTestCoordinator(t, domain.Limits{
		MaxConcurrent: 1,
		MaxCapacity:   1,
		MaxQueueSize:  2,
	}, executor)

	operation := testOperation("reusable-operation", 1)

	_, firstResults, err := coordinator.Submit("first-owner", operation)
	if err != nil {
		t.Fatal(err)
	}

	assertOperationStarted(t, executor.started, operation.ID)
	close(executor.release)

	deadline := time.After(time.Second)
	var secondResults <-chan Result

	for {
		_, results, submitErr := coordinator.Submit("second-owner", operation)
		if submitErr == nil {
			secondResults = results
			break
		}

		if !errors.Is(submitErr, admission.ErrDuplicate) {
			t.Fatalf("Submit() for reused ID = %v, want ErrDuplicate until first completion", submitErr)
		}

		select {
		case <-deadline:
			t.Fatal("first execution did not release its operation ID")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	assertOperationStarted(t, executor.started, operation.ID)

	secondResult := awaitOperationResult(t, secondResults)
	if secondResult.State != domain.StateCompleted || secondResult.OwnerID != "second-owner" {
		t.Fatalf("second result = %+v", secondResult)
	}

	firstResult := awaitOperationResult(t, firstResults)
	if firstResult.State != domain.StateCompleted || firstResult.OwnerID != "first-owner" {
		t.Fatalf("first result = %+v", firstResult)
	}
}

type panickingExecutor struct{}

func (panickingExecutor) Execute(context.Context, domain.Operation) error {
	panic("intentional")
}

func TestCoordinatorExecutorPanicBecomesOperationFailure(t *testing.T) {
	controller, err := admission.New(domain.Limits{
		MaxConcurrent: 1,
		MaxCapacity:   1,
		MaxQueueSize:  0,
	})
	if err != nil {
		t.Fatal(err)
	}

	coordinator, err := New(controller, panickingExecutor{})
	if err != nil {
		t.Fatal(err)
	}

	if err := coordinator.Start(); err != nil {
		t.Fatal(err)
	}

	_, results, err := coordinator.Submit("owner", testOperation("panic-operation", 1))
	if err != nil {
		t.Fatal(err)
	}

	result := awaitOperationResult(t, results)
	if result.State != domain.StateFailed || result.Err == nil {
		t.Fatalf("panic result = %+v, want Failed with error", result)
	}

	if err := coordinator.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCoordinatorCancelOwnerPreservesOtherPendingOperations(t *testing.T) {
	executor := newBlockingExecutor(4, true)
	coordinator, controller := newTestCoordinator(t, domain.Limits{
		MaxConcurrent: 1,
		MaxCapacity:   2,
		MaxQueueSize:  3,
	}, executor)

	runningOperation := testOperation("owner-one-running", 2)
	otherOwnerOperation := testOperation("owner-two-pending", 1)
	cancelledOperation := testOperation("owner-one-pending", 1)

	runningDecision, runningResults, err := coordinator.Submit("owner-one", runningOperation)
	if err != nil || runningDecision != admission.Started {
		t.Fatalf("Submit(running) = (%s, %v), want (started, nil)", runningDecision, err)
	}
	assertOperationStarted(t, executor.started, runningOperation.ID)

	otherDecision, otherResults, err := coordinator.Submit("owner-two", otherOwnerOperation)
	if err != nil || otherDecision != admission.Queued {
		t.Fatalf("Submit(other owner) = (%s, %v), want (queued, nil)", otherDecision, err)
	}

	cancelledDecision, cancelledResults, err := coordinator.Submit("owner-one", cancelledOperation)
	if err != nil || cancelledDecision != admission.Queued {
		t.Fatalf("Submit(pending cancellation) = (%s, %v), want (queued, nil)", cancelledDecision, err)
	}

	if err := coordinator.CancelOwner("owner-one"); err != nil {
		t.Fatal(err)
	}

	cancelledResult := awaitOperationResult(t, cancelledResults)
	if cancelledResult.State != domain.StateCancelled || !errors.Is(cancelledResult.Err, context.Canceled) {
		t.Fatalf("cancelled pending result = %+v, want cancellation", cancelledResult)
	}

	assertCoordinatorSnapshot(t, controller, 1, 1, 2)

	select {
	case result := <-runningResults:
		t.Fatalf("active reservation released before executor returned: %+v", result)
	default:
	}

	if err := coordinator.CancelOwner("owner-one"); err != nil {
		t.Fatalf("repeated CancelOwner() = %v, want nil", err)
	}

	decision, results, err := coordinator.Submit("owner-one", testOperation("rejected-after-cancel", 1))
	if decision != admission.Rejected || results != nil || !errors.Is(err, ErrOwnerClosed) {
		t.Fatalf("Submit(closed owner) = (%s, %v, %v), want (rejected, nil, ErrOwnerClosed)", decision, results, err)
	}

	close(executor.release)

	runningResult := awaitOperationResult(t, runningResults)
	if runningResult.State != domain.StateCancelled {
		t.Fatalf("running result = %s, want cancelled", runningResult.State)
	}

	assertOperationStarted(t, executor.started, otherOwnerOperation.ID)
	otherResult := awaitOperationResult(t, otherResults)
	if otherResult.State != domain.StateCompleted {
		t.Fatalf("other owner result = %s, want completed", otherResult.State)
	}

	assertCoordinatorSnapshot(t, controller, 0, 0, 0)
}

func TestCoordinatorCancelOwnerDoesNotCancelOtherActiveExecutions(t *testing.T) {
	executor := newBlockingExecutor(4, false)
	coordinator, controller := newTestCoordinator(t, domain.Limits{
		MaxConcurrent: 2,
		MaxCapacity:   2,
		MaxQueueSize:  1,
	}, executor)

	cancelledOperation := testOperation("cancelled-active", 1)
	independentOperation := testOperation("independent-active", 1)

	cancelledDecision, cancelledResults, err := coordinator.Submit("disconnected-owner", cancelledOperation)
	if err != nil || cancelledDecision != admission.Started {
		t.Fatalf("Submit(cancelled) = (%s, %v), want started", cancelledDecision, err)
	}
	assertOperationStarted(t, executor.started, cancelledOperation.ID)

	independentDecision, independentResults, err := coordinator.Submit("connected-owner", independentOperation)
	if err != nil || independentDecision != admission.Started {
		t.Fatalf("Submit(independent) = (%s, %v), want started", independentDecision, err)
	}
	assertOperationStarted(t, executor.started, independentOperation.ID)

	if err := coordinator.CancelOwner("disconnected-owner"); err != nil {
		t.Fatal(err)
	}

	cancelledResult := awaitOperationResult(t, cancelledResults)
	if cancelledResult.State != domain.StateCancelled {
		t.Fatalf("cancelled owner state = %s, want cancelled", cancelledResult.State)
	}

	assertCoordinatorSnapshot(t, controller, 1, 0, 1)

	select {
	case result := <-independentResults:
		t.Fatalf("other owner was interrupted: %+v", result)
	default:
	}

	close(executor.release)

	independentResult := awaitOperationResult(t, independentResults)
	if independentResult.State != domain.StateCompleted {
		t.Fatalf("independent owner state = %s, want completed", independentResult.State)
	}

	assertCoordinatorSnapshot(t, controller, 0, 0, 0)
}

func TestCoordinatorCancelOwnerLifecycleValidation(t *testing.T) {
	executor := newBlockingExecutor(2, false)
	controller, err := admission.New(domain.Limits{
		MaxConcurrent: 1,
		MaxCapacity:   1,
		MaxQueueSize:  1,
	})
	if err != nil {
		t.Fatal(err)
	}

	coordinator, err := New(controller, executor)
	if err != nil {
		t.Fatal(err)
	}

	if err := coordinator.CancelOwner("owner-before-start"); !errors.Is(err, ErrNotStarted) {
		t.Fatalf("CancelOwner(before Start) = %v, want ErrNotStarted", err)
	}
	if err := coordinator.CancelOwner(""); !errors.Is(err, ErrOwnerID) {
		t.Fatalf("CancelOwner(empty owner) = %v, want ErrOwnerID", err)
	}
	if err := coordinator.Start(); err != nil {
		t.Fatal(err)
	}

	if err := coordinator.CancelOwner("owner-without-operations"); err != nil {
		t.Fatalf("CancelOwner(no operations) = %v, want nil", err)
	}

	decision, results, err := coordinator.Submit("owner-without-operations", testOperation("rejected-operation", 1))
	if decision != admission.Rejected || results != nil || !errors.Is(err, ErrOwnerClosed) {
		t.Fatalf("Submit(cancelled owner) = (%s, %v, %v), want ErrOwnerClosed", decision, results, err)
	}

	closeContext, cancelClose := context.WithTimeout(context.Background(), time.Second)
	defer cancelClose()
	if err := coordinator.Close(closeContext); err != nil {
		t.Fatal(err)
	}

	if err := coordinator.CancelOwner("owner-after-close"); !errors.Is(err, ErrClosed) {
		t.Fatalf("CancelOwner(after Close) = %v, want ErrClosed", err)
	}
}

type gatedPromotionSelector struct {
	entered chan struct{}
	proceed chan struct{}
	once    sync.Once
}

func (selector *gatedPromotionSelector) Next(pending []domain.Operation, available uint64) int {
	selector.once.Do(func() {
		close(selector.entered)
		<-selector.proceed
	})
	if len(pending) > 0 && pending[0].Cost <= available {
		return 0
	}
	return -1
}

func TestCoordinatorCloseDuringPromotionDoesNotLaunchPendingOperation(t *testing.T) {
	selector := &gatedPromotionSelector{
		entered: make(chan struct{}),
		proceed: make(chan struct{}),
	}

	controller, err := admission.NewWithSelector(domain.Limits{
		MaxConcurrent: 1,
		MaxCapacity:   1,
		MaxQueueSize:  1,
	}, selector)

	if err != nil {
		t.Fatal(err)
	}

	executor := newBlockingExecutor(2, false)
	coordinator, err := New(controller, executor)
	if err != nil {
		t.Fatal(err)
	}

	if err := coordinator.Start(); err != nil {
		t.Fatal(err)
	}

	runningOperation := testOperation("running-before-close", 1)
	pendingOperation := testOperation("pending-at-close", 1)

	_, runningResults, err := coordinator.Submit("first-owner", runningOperation)
	if err != nil {
		t.Fatal(err)
	}
	assertOperationStarted(t, executor.started, runningOperation.ID)

	_, pendingResults, err := coordinator.Submit("second-owner", pendingOperation)
	if err != nil {
		t.Fatal(err)
	}

	close(executor.release)
	select {
	case <-selector.entered:
	case <-time.After(time.Second):
		t.Fatal("scheduler did not begin pending promotion")
	}

	closeContext, cancelClose := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancelClose()
	if err := coordinator.Close(closeContext); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close() during promotion = %v, want deadline exceeded", err)
	}

	close(selector.proceed)

	pendingResult := awaitOperationResult(t, pendingResults)
	if pendingResult.State != domain.StateCancelled {
		t.Fatalf("pending operation result = %+v, want cancelled", pendingResult)
	}
	if runningResult := awaitOperationResult(t, runningResults); runningResult.State != domain.StateCompleted {
		t.Fatalf("completed running operation = %+v, want completed", runningResult)
	}

	select {
	case startedOperationID := <-executor.started:
		t.Fatalf("operation %q launched after Close() request", startedOperationID)
	default:
	}
	if err := coordinator.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertCoordinatorSnapshot(t, controller, 0, 0, 0)
}

type invalidPromotionSelector struct{}

func (invalidPromotionSelector) Next(pending []domain.Operation, available uint64) int {
	return len(pending)
}

func TestCoordinatorSchedulerFailureCancelsPendingAndReleasesCapacity(t *testing.T) {
	controller, err := admission.NewWithSelector(domain.Limits{
		MaxConcurrent: 1,
		MaxCapacity:   1,
		MaxQueueSize:  2,
	}, invalidPromotionSelector{})
	if err != nil {
		t.Fatal(err)
	}

	executor := newBlockingExecutor(2, false)
	coordinator, err := New(controller, executor)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Start(); err != nil {
		t.Fatal(err)
	}

	runningOperation := testOperation("running-before-scheduler-error", 1)
	pendingOperation := testOperation("pending-at-scheduler-error", 1)

	runningDecision, runningResults, err := coordinator.Submit("running-owner", runningOperation)
	if err != nil || runningDecision != admission.Started {
		t.Fatalf("Submit(running) = (%s, %v), want Started", runningDecision, err)
	}
	assertOperationStarted(t, executor.started, runningOperation.ID)

	pendingDecision, pendingResults, err := coordinator.Submit("pending-owner", pendingOperation)
	if err != nil || pendingDecision != admission.Queued {
		t.Fatalf("Submit(pending) = (%s, %v), want Queued", pendingDecision, err)
	}

	close(executor.release)

	runningResult := awaitOperationResult(t, runningResults)
	if runningResult.State != domain.StateCompleted {
		t.Fatalf("running result = %+v, want Completed", runningResult)
	}

	pendingResult := awaitOperationResult(t, pendingResults)
	if pendingResult.State != domain.StateCancelled || pendingResult.Err == nil {
		t.Fatalf("pending result = %+v, want Cancelled with scheduler error", pendingResult)
	}

	closeContext, cancelClose := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelClose()
	if err := coordinator.Close(closeContext); err == nil || !strings.Contains(err.Error(), "invalid index") {
		t.Fatalf("Close() = %v, want scheduler invalid-index error", err)
	}

	assertCoordinatorSnapshot(t, controller, 0, 0, 0)
}

func TestCoordinatorConcurrentCloseCallsPreserveSingleCleanup(t *testing.T) {
	executor := newBlockingExecutor(2, true)
	controller, err := admission.New(domain.Limits{
		MaxConcurrent: 1,
		MaxCapacity:   1,
		MaxQueueSize:  1,
	})
	if err != nil {
		t.Fatal(err)
	}

	coordinator, err := New(controller, executor)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Start(); err != nil {
		t.Fatal(err)
	}

	operation := testOperation("operation-during-concurrent-close", 1)
	decision, results, err := coordinator.Submit("closing-owner", operation)
	if err != nil || decision != admission.Started {
		t.Fatalf("Submit() = (%s, %v), want Started", decision, err)
	}
	assertOperationStarted(t, executor.started, operation.ID)

	const closingCallers = 8
	var closeRequests sync.WaitGroup
	for range closingCallers {
		closeRequests.Add(1)
		go func() {
			defer closeRequests.Done()
			closeContext, cancelClose := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancelClose()
			if closeError := coordinator.Close(closeContext); !errors.Is(closeError, context.DeadlineExceeded) {
				t.Errorf("Close() = %v, want context.DeadlineExceeded", closeError)
			}
		}()
	}
	closeRequests.Wait()

	assertCoordinatorSnapshot(t, controller, 1, 0, 1)
	close(executor.release)

	result := awaitOperationResult(t, results)
	if result.State != domain.StateCancelled {
		t.Fatalf("result = %+v, want Cancelled", result)
	}

	if err := coordinator.Close(context.Background()); err != nil {
		t.Fatalf("final Close() = %v, want nil", err)
	}

	assertCoordinatorSnapshot(t, controller, 0, 0, 0)
}
