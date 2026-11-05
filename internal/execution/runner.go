package execution

import (
	"context"
	"errors"
	"fmt"
	"pondus-gateway/internal/admission"
	"pondus-gateway/internal/core"
	"sync"
	"time"
)

var (
	ErrRunnerBusy                 = errors.New("runner is already running")
	ErrNoProgress                 = errors.New("runner cannot make progress: pending operations without active executions")
	ErrControllerExecutorRequired = errors.New("controller and executor are required")
	ErrNegativeTimeout            = errors.New("negative timeout")
)

// Result contains the admission decision and, if the operation was admitted, its execution.
//
// A rejected operation retains Decision=Rejected, an empty State, and the error returned by
// the admission (which may be nil); it does not start a lifecycle.
//
// For admitted operations, State and Err describe their individual outcomes.
type Result struct {
	OperationID string
	Decision    admission.Decision
	State       core.State
	Err         error
}

// Runner coordinates the admission and concurrent execution
// of operations, managing their lifecycles, cancellations and
// reserved resources.
//
// It supports only one simultaneous batch execution.
type Runner struct {
	mutex      sync.Mutex
	running    bool
	controller *admission.Controller
	executor   Executor
	timeout    time.Duration
}

// completion captures the result of
// an operation executed asynchronously.
type completion struct {
	id  string
	err error
}

// newCurrentSession initializes the state and resources
// needed to coordinate a Runner execution.
type currentSession struct {
	runner     *Runner
	controller *admission.Session
	ctx        context.Context
	runCtx     context.Context
	timeout    time.Duration
	results    []Result
	positions  map[string]int
	lifecycles map[string]*core.Lifecycle
	started    []core.Operation
	// It includes both pending operations and active accepted operations that
	// have not yet been resolved by the Runner.
	// It should not be confused with the number of goroutines currently running.
	outstanding int
	finished    chan completion
	reserved    map[string]struct{}
	inflight    map[string]struct{}
	cancelled   bool
	doneCh      <-chan struct{}
}

// New creates a Runner with the specified admission controller and executor.
//
// Both components must be non-nil.
func New(controller *admission.Controller, executor Executor) (*Runner, error) {
	if controller == nil || executor == nil {
		return nil, ErrControllerExecutorRequired
	}
	return &Runner{controller: controller, executor: executor}, nil
}

// SetTimeout sets the timeout per operation, only when Runner is idle.
//
// A value of zero disables the timeout. When the timeout expires, the
//
// Execute context is canceled, but Runner continues to wait for the
// executor to return.
func (runner *Runner) SetTimeout(duration time.Duration) error {
	if duration < 0 {
		return ErrNegativeTimeout
	}

	runner.mutex.Lock()
	defer runner.mutex.Unlock()

	if runner.running {
		return ErrRunnerBusy
	}

	runner.timeout = duration
	return nil
}

// Run coordinates a batch and returns its results in the order they were received.
//
// It maintains exclusive ownership of the Controller until the active executors finish
// and their reservations are released, even if the context is canceled.
//
// Therefore, Run may become blocked if an executor ignores the cancellation.
//
// Individual executor failures are reflected in Result.Err and do not by themselves cause
// a Run error. Its return error indicates failures in coordination or cleanup; it may coexist
// with valid partial results.
//
// Canceling the batch does not necessarily imply a Run return error.
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

	owner, err := runner.controller.Acquire()
	if err != nil {
		return nil, err
	}

	defer func() {
		runErr = errors.Join(runErr, owner.Release())
	}()

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	session := newCurrentSession(runner, owner, ctx, runCtx, timeout, len(operations))
	results = session.results
	session.admit(operations)

	if err := ctx.Err(); err != nil {
		return results, session.cancelBeforeStart(err)
	}

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

// currentSession maintains the state of a Runner execution.
//
// Only the Run goroutine modifies its maps and results.
//
// The execution goroutines signal their completion via finished.
func newCurrentSession(
	runner *Runner,
	controller *admission.Session,
	ctx context.Context,
	runCtx context.Context,
	timeout time.Duration,
	count int,
) *currentSession {
	return &currentSession{
		runner: runner, controller: controller, ctx: ctx, runCtx: runCtx, timeout: timeout,
		results: make([]Result, count), positions: make(map[string]int, count),
		lifecycles: make(map[string]*core.Lifecycle, count),
		started:    make([]core.Operation, 0), finished: make(chan completion, count),
		reserved: make(map[string]struct{}), inflight: make(map[string]struct{}),
		doneCh: ctx.Done(),
	}
}

// admit processes the batch intake and records the initial results.
//
// It identifies the operations that can begin immediately and keeps
// track of all accepted operations.
func (session *currentSession) admit(operations []core.Operation) {
	for index, operation := range operations {
		decision, err := session.controller.Submit(operation)
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

// transition updates the lifecycle and the result of an operation.
//
// It requires that its identifier be registered in the session.
func (session *currentSession) transition(id string, state core.State, reason error) error {
	lifecycle := session.lifecycles[id]
	if err := lifecycle.TransitionTo(state); err != nil {
		return err
	}

	index := session.positions[id]
	session.results[index].State = lifecycle.State()
	session.results[index].Err = reason

	return nil
}

// cancelPending cancels pending operations and updates their results
// with the reason for cancellation, without affecting active executions.
func (session *currentSession) cancelPending(reason error) error {
	pending, err := session.controller.CancelPending()
	if err != nil {
		return err
	}

	for _, operation := range pending {
		if err := session.transition(operation.ID, core.StateCancelled, reason); err != nil {
			return err
		}
		session.outstanding--
	}

	return nil
}

// cancelBeforeStart cancels the supported operations before starting
// execution and releases their reservations without promoting new operations.
func (session *currentSession) cancelBeforeStart(reason error) error {
	if err := session.cancelPending(reason); err != nil {
		return err
	}

	for _, operation := range session.started {
		if err := session.transition(operation.ID, core.StateCancelled, reason); err != nil {
			return err
		}

		if err := session.controller.FinishWithoutPromotion(operation.ID); err != nil {
			return err
		}

		session.outstanding--
	}

	return nil
}

// launch starts an operation asynchronously and logs its execution
// so that its completion can be monitored later.
func (session *currentSession) launch(operation core.Operation) error {
	if err := session.transition(
		operation.ID,
		core.StateRunning,
		session.results[session.positions[operation.ID]].Err,
	); err != nil {
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

// execute applies the configured individual timeout and converts
// the panics retrieved from the executor into errors.
func (session *currentSession) execute(operation core.Operation) (err error) {
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

// observeCancellation processes the cancellation of the context only once,
// removing pending operations without directly interrupting active executions.
func (session *currentSession) observeCancellation() error {
	if session.cancelled || session.ctx.Err() == nil {
		return nil
	}

	session.cancelled = true
	session.doneCh = nil

	return session.cancelPending(session.ctx.Err())
}

// wait monitors completions and cancellations until
// all accepted operations are resolved or a coordination
// error is encountered.
func (session *currentSession) wait() error {
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

// complete processes the completion of an operation and frees up its capacity.
//
// If the session is still active, it initiates the promoted operations; if it
// is canceled, it prevents new promotions.
func (session *currentSession) complete(done completion) error {
	delete(session.inflight, done.id)
	if err := session.transition(done.id, completionState(done.err), done.err); err != nil {
		return err
	}

	if err := session.observeCancellation(); err != nil {
		return err
	}

	if session.cancelled {
		if err := session.controller.FinishWithoutPromotion(done.id); err != nil {
			return err
		}

		delete(session.reserved, done.id)
	} else {
		promoted, err := session.controller.Finish(done.id)
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
		active, queued, _ := session.controller.Snapshot()
		if active == 0 && queued > 0 {
			if err := session.cancelPending(ErrNoProgress); err != nil {
				return err
			}
			return ErrNoProgress
		}
	}

	return nil
}

// cleanup aborts execution, waits for active goroutines and releases
// resources after an internal error.
//
// Combines additional errors using errors.Join.
//
// It depends on the executors finishing in order to complete.
func (session *currentSession) cleanup(runErr error, cancelRun context.CancelFunc) error {
	cancelRun()
	pending, err := session.controller.CancelPending()
	if err != nil {
		runErr = errors.Join(runErr, err)
	}

	for _, operation := range pending {
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
		if err := session.controller.FinishWithoutPromotion(id); err != nil && !errors.Is(err, admission.ErrNotActive) {
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

// completionState classifies the Execute error: Canceled implies Cancelled,
// any other error (including DeadlineExceeded) implies Failed and nil implies
// Completed.
//
// The context alone does not determine the final state.
func completionState(err error) core.State {
	if errors.Is(err, context.Canceled) {
		return core.StateCancelled
	}

	if err != nil {
		return core.StateFailed
	}

	return core.StateCompleted
}
