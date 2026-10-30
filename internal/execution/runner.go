package execution

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"pondus-gateway/internal/admission"
	"pondus-gateway/internal/core"
)

// Result represents the outcome of accepting and executing an operation.
//
// It contains the operation identifier, the acceptance decision, the status
// reached, and the associated error, if any.
type Result struct {
	OperationID string
	Decision    admission.Decision
	State       core.State
	Err         error
}

// Runner coordinates the admission, execution, and completion of a batch
// of operations using an admission controller and an executor.
//
// It manages state transitions, concurrent executions, cancellations, and
// the release of reserved resources.
type Runner struct {
	mutex      sync.Mutex
	running    bool
	controller *admission.Controller
	executor   Executor
	timeout    time.Duration
}

var (
	ErrRunnerBusy = errors.New("runner is already running")
	ErrNoProgress = errors.New("runner cannot make progress: pending operations without active executions")
)

// New creates a Runner using the admission controller and executor provided.
//
// Returns a pointer to the initialized Runner or an error if any of the required
// components is nil.
func New(controller *admission.Controller, executor Executor) (*Runner, error) {
	if controller == nil || executor == nil {
		return nil, errors.New("controller and executor are required")
	}
	return &Runner{controller: controller, executor: executor}, nil
}

// SetTimeout sets the maximum execution time for each operation.
//
// It only allows you to modify the configuration when the Runner is idle.
//
// Returns ErrRunnerBusy if a batch is currently running, an error if the
// duration is negative, or nil if the configuration is updated successfully.
//
// Access to the configuration is protected by mutual exclusion.
func (runner *Runner) SetTimeout(duration time.Duration) error {
	if duration < 0 {
		return errors.New("negative timeout")
	}

	runner.mutex.Lock()
	defer runner.mutex.Unlock()

	if runner.running {
		return ErrRunnerBusy
	}

	runner.timeout = duration
	return nil
}

// completion represents the completion of a single execution.
type completion struct {
	id  string
	err error
}

// runSession maintains the mutable state of a single Runner execution.
//
// It stores the results, lifecycles, pending operations and logs needed
// to manage reservations.
//
// Only the goroutine that executes Run modifies the session's maps and
// results.
//
// The executing goroutines report their completions via the finished channel.
type runSession struct {
	runner      *Runner
	ctx         context.Context
	runCtx      context.Context
	timeout     time.Duration
	results     []Result
	positions   map[string]int
	lifecycles  map[string]*core.Lifecycle
	started     []core.Operation
	outstanding int
	finished    chan completion
	reserved    map[string]struct{}
	inflight    map[string]struct{}
	cancelled   bool
	doneCh      <-chan struct{}
}

// Run accepts and executes a batch of operations, returning their results
// in the same order in which they were received.
//
// It allows only one concurrent execution per Runner and requires that the
// admission controller contain no active or pending operations at the start.
//
// Each operation undergoes the admission process and, if admitted, begins
// execution or remains on hold until it can be promoted.
//
// It manages state transitions, completions, context cancellation, and resource
// release.
//
// Returns the batch results and an error if a failure occurs during session
// coordination.
//
// Individual admission or execution errors are logged in the corresponding result
// and do not necessarily imply a Run error.
//
// In the event of internal errors, it attempts to cancel executions, wait for them
// to complete, and release existing reservations.
func (runner *Runner) Run(ctx context.Context, operations []core.Operation) (results []Result, runErr error) {
	runner.mutex.Lock()
	if runner.running {
		runner.mutex.Unlock()
		return nil, ErrRunnerBusy
	}

	runner.running = true
	timeout := runner.timeout
	runner.mutex.Unlock()
	defer func() { runner.mutex.Lock(); runner.running = false; runner.mutex.Unlock() }()

	active, queued, _ := runner.controller.Snapshot()
	if active != 0 || queued != 0 {
		return nil, errors.New("controller must be empty")
	}

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	session := newRunSession(runner, ctx, runCtx, timeout, len(operations))
	results = session.results
	session.admit(operations)

	if err := ctx.Err(); err != nil {
		return results, session.cancelBeforeStart(err)
	}

	// Cleanup in case of internal errors is performed before launching goroutines
	defer func() {
		if runErr != nil {
			runErr = session.cleanup(runErr, cancelRun)
		}
	}()

	for _, operation := range session.started {
		if err := session.launch(operation); err != nil {
			return results, err
		}
	}
	return results, session.wait()
}

// newRunSession initializes the state of a run session.
//
// It takes the Runner, the execution contexts, the maximum time per operation
// and the number of operations in the batch.
//
// It initializes the results, operation logs, lifecycles and the channel used
// to receive completions.
//
// Returns a pointer to the created session, with no operations currently running
// and no pending completions.
func newRunSession(runner *Runner, ctx, runCtx context.Context, timeout time.Duration, count int) *runSession {
	return &runSession{
		runner: runner, ctx: ctx, runCtx: runCtx, timeout: timeout,
		results: make([]Result, count), positions: make(map[string]int, count),
		lifecycles: make(map[string]*core.Lifecycle, count),
		started:    make([]core.Operation, 0), finished: make(chan completion, count),
		reserved: make(map[string]struct{}), inflight: make(map[string]struct{}),
		doneCh: ctx.Done(),
	}
}

// admit submits the operations in the batch to the admission controller.
//
// Records the decision and error for each operation in the corresponding
// position in the results.
//
// For accepted operations, initializes their lifecycle in the pending state
// and increments the counter for operations pending completion.
//
// Operations accepted for immediate execution are added to the register of
// initial operations and reserved capacities.
//
// Rejected operations do not receive a lifecycle nor are they counted as
// pending completion.
func (session *runSession) admit(operations []core.Operation) {
	for index, operation := range operations {
		decision, err := session.runner.controller.Submit(operation)
		session.results[index] = Result{OperationID: operation.ID, Decision: decision, Err: err}

		if decision == admission.Rejected {
			continue
		}

		session.positions[operation.ID] = index
		session.lifecycles[operation.ID] = core.NewLifecycle()
		session.results[index].State = core.StatePending
		session.outstanding++

		if decision == admission.Started {
			session.started = append(session.started, operation)
			session.reserved[operation.ID] = struct{}{}
		}
	}
}

// transition attempts to change the state of a supported operation.
//
// It uses the lifecycle associated with its identifier to validate
// the requested transition.
//
// If the transition is valid, it updates the state and error stored
// in the corresponding result.
//
// Returns an error if the transition is not allowed or nil if it is
// successful.
//
// Requires that the operation have a lifecycle and a position previously
// registered in the session.
func (session *runSession) transition(id string, state core.State, reason error) error {
	lifecycle := session.lifecycles[id]
	if err := lifecycle.TransitionTo(state); err != nil {
		return err
	}

	index := session.positions[id]
	session.results[index].State = lifecycle.State()
	session.results[index].Err = reason

	return nil
}

// cancelPending cancels all operations remaining in the admission queue.
//
// Removes pending operations from the controller, updates their lifecycles
// to the Cancelled state, and logs the reason for cancellation in their results.
//
// Decreases the counter of operations pending completion for each cancellation
// performed.
//
// Returns an error if any state transition fails or nil if all operations are
// canceled successfully.
func (session *runSession) cancelPending(reason error) error {
	for _, operation := range session.runner.controller.CancelPending() {
		if err := session.transition(operation.ID, core.StateCancelled, reason); err != nil {
			return err
		}
		session.outstanding--
	}
	return nil
}

// cancelBeforeStart cancels the accepted operations before their executors start.
//
// First cancels the pending operations, then the operations accepted for immediate
// execution.
//
// Updates their lifecycles to the “Cancelled” state, logs the reason for cancellation
// and releases reserved capacity without promoting new operations.
//
// Returns an error if any state transition or capacity release fails, or nil if the
// process completes successfully.
func (session *runSession) cancelBeforeStart(reason error) error {
	if err := session.cancelPending(reason); err != nil {
		return err
	}

	for _, operation := range session.started {
		if err := session.transition(operation.ID, core.StateCancelled, reason); err != nil {
			return err
		}

		if err := session.runner.controller.FinishWithoutPromotion(operation.ID); err != nil {
			return err
		}

		session.outstanding--
	}

	return nil
}

// launch initiates the asynchronous execution of a supported operation.
//
// Transitions to the Running state and registers the operation among the
// ongoing executions.
//
// Starts a goroutine that executes the operation and signals its completion
// via the finished channel.
//
// Returns an error if the transition to the Running state fails or nil if
// the goroutine starts successfully.
//
// Does not wait for the operation to complete.
func (session *runSession) launch(operation core.Operation) error {
	if err := session.transition(operation.ID, core.StateRunning, session.results[session.positions[operation.ID]].Err); err != nil {
		return err
	}

	session.inflight[operation.ID] = struct{}{}

	go func() {
		session.finished <- completion{
			id:  operation.ID,
			err: session.execute(operation),
		}
	}()

	return nil
}

// execute performs an operation using the configured executor.
//
// It uses the session context and, when a timeout is configured,
// creates a derived context with that timeout.
//
// It retrieves any non-null panics that occurred during execution
// and converts them into errors so they can be handled.
//
// Returns the error generated by the executor, the error associated
// with a recovered panic or nil if execution completes successfully.
//
// Cancels the derived context upon completion, when applicable.
func (session *runSession) execute(operation core.Operation) (err error) {
	workCtx := session.runCtx
	cancel := func() {}

	if session.timeout > 0 {
		workCtx, cancel = context.WithTimeout(session.runCtx, session.timeout)
	}

	defer cancel()

	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("executor panic: %v", recovered)
		}
	}()

	return session.runner.executor.Execute(workCtx, operation)
}

// completionState determines the final state of an operation based
// on the error returned by its executor.
//
// Returns:
//   - StateCancelled if the error corresponds to context.Canceled.
//   - StateFailed if any other error occurs.
//   - StateCompleted if the execution completes without error.
func completionState(err error) core.State {
	if errors.Is(err, context.Canceled) {
		return core.StateCancelled
	}

	if err != nil {
		return core.StateFailed
	}

	return core.StateCompleted
}

// observeCancellation checks whether the original session context
// has been canceled or has expired.
//
// When it detects a cancellation for the first time, it marks the
// session as canceled, disables new cancellation notifications and
// cancels pending operations.
//
// It does not directly terminate operations that are already running;
// their completion continues to be monitored.
//
// It returns an error if the cancellation of any pending operation fails
// or nil if no further action is required.
func (session *runSession) observeCancellation() error {
	if session.cancelled || session.ctx.Err() == nil {
		return nil
	}

	session.cancelled = true
	session.doneCh = nil

	return session.cancelPending(session.ctx.Err())
}

// wait monitors the completion of supported operations until there
// are no operations remaining to be completed.
//
// Listens for notifications from executors and for the cancellation
// of the original session context.
//
// Processes each completion via complete, updating the results and
// coordinating any promotions.
//
// Returns nil when all supported operations have been processed or
// an error if handling a completion or cancellation fails.
func (session *runSession) wait() error {
	for session.outstanding > 0 {
		if err := session.observeCancellation(); err != nil {
			return err
		}

		if session.outstanding == 0 {
			break
		}

		select {
		case <-session.doneCh:
			if err := session.observeCancellation(); err != nil {
				return err
			}
		case done := <-session.finished:
			if err := session.complete(done); err != nil {
				return err
			}
		}
	}

	return nil
}

// complete processes the notification that an operation has completed.
//
// Removes the operation from the list of ongoing executions, determines
// its final status, and updates the associated result.
//
// If the session is canceled, it releases the operation's capacity without
// promoting new executions.
//
// Otherwise, it terminates the operation via the controller, records the
// new reservations, and initiates the promoted operations.
//
// It decrements the counter of operations pending termination and checks
// whether the controller can continue progressing.
//
// Returns ErrNoProgress if there are still pending operations without active
// executions that can free up capacity or an error if any transition, release,
// reservation or promotion fails.
//
// Returns nil when the completion is processed successfully.
func (session *runSession) complete(done completion) error {
	delete(session.inflight, done.id)
	if err := session.transition(done.id, completionState(done.err), done.err); err != nil {
		return err
	}

	if err := session.observeCancellation(); err != nil {
		return err
	}

	if session.cancelled {
		if err := session.runner.controller.FinishWithoutPromotion(done.id); err != nil {
			return err
		}

		delete(session.reserved, done.id)
	} else {
		promoted, err := session.runner.controller.Finish(done.id)
		if err == nil {
			delete(session.reserved, done.id)
		}

		for _, operation := range promoted {
			session.reserved[operation.ID] = struct{}{}
		}

		if err != nil {
			return err
		}

		for _, operation := range promoted {
			if err := session.launch(operation); err != nil {
				return err
			}
		}
	}
	session.outstanding--

	if session.outstanding > 0 && !session.cancelled {
		active, queued, _ := session.runner.controller.Snapshot()
		if active == 0 && queued > 0 {
			if err := session.cancelPending(ErrNoProgress); err != nil {
				return err
			}
			return ErrNoProgress
		}
	}

	return nil
}

// cleanup cleans up resources after an internal error occurs during a session.
//
// Cancels the execution context and removes operations that remain pending in
// the controller.
//
// Waits for ongoing executions to complete, updates their results and releases
// registered reservations without promoting new operations.
//
// Attempts to update the lifecycles of pending operations to the Cancelled state.
//
// When additional errors occur during cleanup, it combines them with the original
// error using errors.Join.
//
// Returns the original error, possibly combined with errors encountered during
// cleanup.
//
// The completion of this method depends on the executing goroutines finishing and
// reporting their results.
func (session *runSession) cleanup(runErr error, cancelRun context.CancelFunc) error {
	cancelRun()
	for _, operation := range session.runner.controller.CancelPending() {
		index := session.positions[operation.ID]
		session.results[index].Err = runErr

		if lifecycle := session.lifecycles[operation.ID]; lifecycle != nil {
			if err := lifecycle.TransitionTo(core.StateCancelled); err != nil {
				runErr = errors.Join(runErr, err)
			}
			session.results[index].State = lifecycle.State()
		}
	}

	for len(session.inflight) > 0 {
		done := <-session.finished
		if _, exists := session.inflight[done.id]; !exists {
			continue
		}

		delete(session.inflight, done.id)
		index := session.positions[done.id]
		session.results[index].Err = done.err
		lifecycle := session.lifecycles[done.id]

		if err := lifecycle.TransitionTo(completionState(done.err)); err != nil {
			runErr = errors.Join(runErr, err)
		}

		session.results[index].State = lifecycle.State()
	}

	for id := range session.reserved {
		if err := session.runner.controller.FinishWithoutPromotion(id); err != nil && !errors.Is(err, admission.ErrNotActive) {
			runErr = errors.Join(runErr, err)
		}

		lifecycle := session.lifecycles[id]
		if lifecycle != nil && lifecycle.State() == core.StatePending {
			if err := lifecycle.TransitionTo(core.StateCancelled); err != nil {
				runErr = errors.Join(runErr, err)
			}

			index := session.positions[id]
			session.results[index].State = lifecycle.State()
			session.results[index].Err = runErr
		}
	}
	return runErr
}
