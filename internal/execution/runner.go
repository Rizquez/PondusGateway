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

// Result contains the admission decision and the status reached
// by an operation, along with any errors that may have occurred.
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

type completion struct {
	id  string
	err error
}

type currentSession struct {
	runner      *Runner
	controller  *admission.Session
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

// New creates a Runner with the specified admission controller and executor.
//
// Both components must be non-nil.
func New(controller *admission.Controller, executor Executor) (*Runner, error) {
	if controller == nil || executor == nil {
		return nil, ErrControllerExecutorRequired
	}
	return &Runner{controller: controller, executor: executor}, nil
}

// SetTimeout sets the maximum time per operation.
//
// A value of zero disables the limit and the setting can only
// be changed when the Runner is idle.
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

// Run executes a batch and returns its results in the order they were entered.
//
// Requires a controller with no active or pending operations.
// Individual errors are logged in Result.Err and do not necessarily indicate a
// Run error.
//
// In the event of internal errors, it attempts to cancel the executions, wait
// for them to complete, and release the reservations.
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

func (session *currentSession) observeCancellation() error {
	if session.cancelled || session.ctx.Err() == nil {
		return nil
	}

	session.cancelled = true
	session.doneCh = nil

	return session.cancelPending(session.ctx.Err())
}

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

func completionState(err error) core.State {
	if errors.Is(err, context.Canceled) {
		return core.StateCancelled
	}

	if err != nil {
		return core.StateFailed
	}

	return core.StateCompleted
}
