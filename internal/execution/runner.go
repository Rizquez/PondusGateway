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

type Result struct {
	OperationID string
	Decision    admission.Decision
	State       core.State
	Err         error
}

type Runner struct {
	mutex      sync.Mutex
	running    bool
	controller *admission.Controller
	executor   Executor
	timeout    time.Duration
}

func New(controller *admission.Controller, executor Executor) (*Runner, error) {
	if controller == nil || executor == nil {
		return nil, errors.New("controller and executor are required")
	}

	return &Runner{
		controller: controller,
		executor:   executor,
	}, nil
}

var ErrRunnerBusy = errors.New("runner is already running")

var ErrNoProgress = errors.New("runner cannot make progress: pending operations without active executions")

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

func (runner *Runner) Run(ctx context.Context, operations []core.Operation) (results []Result, runErr error) {

	runner.mutex.Lock()

	if runner.running {
		runner.mutex.Unlock()
		return nil, ErrRunnerBusy
	}

	runner.running = true
	timeout := runner.timeout

	runner.mutex.Unlock()

	defer func() {
		runner.mutex.Lock()
		runner.running = false
		runner.mutex.Unlock()
	}()

	active, queued, _ := runner.controller.Snapshot()

	if active != 0 || queued != 0 {
		return nil, errors.New("controller must be empty")
	}

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	results = make([]Result, len(operations))
	positions := make(map[string]int, len(operations))
	lifecycles := make(map[string]*core.Lifecycle, len(operations))
	started := make([]core.Operation, 0)
	outstanding := 0

	for i, operation := range operations {
		decision, err := runner.controller.Submit(operation)

		results[i] = Result{
			OperationID: operation.ID,
			Decision:    decision,
			Err:         err,
		}

		if decision == admission.Rejected {
			continue
		}

		positions[operation.ID] = i

		lifecycles[operation.ID] = core.NewLifecycle()
		results[i].State = core.StatePending

		outstanding++

		if decision == admission.Started {
			started = append(started, operation)
		}
	}

	cancelPending := func(reason error) error {
		for _, operation := range runner.controller.CancelPending() {
			index := positions[operation.ID]

			lifecycle := lifecycles[operation.ID]

			if err := lifecycle.TransitionTo(core.StateCancelled); err != nil {
				return err
			}

			results[index].Err = reason
			results[index].State = lifecycle.State()

			outstanding--
		}

		return nil
	}

	if err := ctx.Err(); err != nil {
		if cancelErr := cancelPending(err); cancelErr != nil {
			return results, cancelErr
		}

		for _, operation := range started {
			index := positions[operation.ID]

			lifecycle := lifecycles[operation.ID]

			if stateErr := lifecycle.TransitionTo(core.StateCancelled); stateErr != nil {
				return results, stateErr
			}

			results[index].Err = err
			results[index].State = lifecycle.State()

			if finishErr := runner.controller.FinishWithoutPromotion(
				operation.ID,
			); finishErr != nil {
				return results, finishErr
			}

			outstanding--
		}

		return results, nil
	}

	type completion struct {
		id  string
		err error
	}

	finished := make(chan completion, len(operations))

	reserved := make(map[string]struct{})
	inflight := make(map[string]struct{})

	// If an internal error occurs, the process is canceled and
	// all work still in progress is retrieved
	defer func() {
		if runErr == nil {
			return
		}

		cancelRun()

		// Transactions that have not yet begun are rolled back
		for _, op := range runner.controller.CancelPending() {
			index := positions[op.ID]
			results[index].Err = runErr

			if lifecycle := lifecycles[op.ID]; lifecycle != nil {
				if err := lifecycle.TransitionTo(
					core.StateCancelled,
				); err != nil {
					runErr = errors.Join(runErr, err)
				}
				results[index].State = lifecycle.State()
			}
		}

		// All registered participants are expected to attend
		for len(inflight) > 0 {
			done := <-finished

			if _, exists := inflight[done.id]; !exists {
				continue
			}

			delete(inflight, done.id)

			index := positions[done.id]
			results[index].Err = done.err

			lifecycle := lifecycles[done.id]
			state := core.StateFailed

			if errors.Is(done.err, context.Canceled) {
				state = core.StateCancelled
			} else if done.err == nil {
				state = core.StateCompleted
			}

			if err := lifecycle.TransitionTo(state); err != nil {
				runErr = errors.Join(runErr, err)
			}

			results[index].State = lifecycle.State()
		}

		// All launched executors have finished
		// The remaining reservations can now be released
		for id := range reserved {
			if err := runner.controller.FinishWithoutPromotion(id); err != nil {
				if !errors.Is(err, admission.ErrNotActive) {
					runErr = errors.Join(runErr, err)
				}
			}

			lifecycle := lifecycles[id]
			if lifecycle != nil &&
				lifecycle.State() == core.StatePending {

				if err := lifecycle.TransitionTo(
					core.StateCancelled,
				); err != nil {
					runErr = errors.Join(runErr, err)
				}

				index := positions[id]
				results[index].State = lifecycle.State()
				results[index].Err = runErr
			}
		}
	}()

	// Started operations already have capacity reserved
	for _, operation := range started {
		reserved[operation.ID] = struct{}{}
	}

	launch := func(operation core.Operation) error {
		index := positions[operation.ID]
		lifecycle := lifecycles[operation.ID]

		if err := lifecycle.TransitionTo(
			core.StateRunning,
		); err != nil {
			return err
		}

		results[index].State = lifecycle.State()
		inflight[operation.ID] = struct{}{}

		go func() {
			workCtx := runCtx
			cancel := func() {}

			if timeout > 0 {
				workCtx, cancel = context.WithTimeout(
					runCtx,
					timeout,
				)
			}
			defer cancel()

			var err error

			func() {
				defer func() {
					if recovered := recover(); recovered != nil {
						err = fmt.Errorf(
							"executor panic: %v",
							recovered,
						)
					}
				}()

				err = runner.executor.Execute(
					workCtx,
					operation,
				)
			}()

			finished <- completion{
				id:  operation.ID,
				err: err,
			}
		}()

		return nil
	}

	for _, operation := range started {
		if err := launch(operation); err != nil {
			return results, err
		}
	}

	cancelled := false
	doneCh := ctx.Done()

	for outstanding > 0 {
		if !cancelled && ctx.Err() != nil {
			cancelled = true
			doneCh = nil
			if err := cancelPending(ctx.Err()); err != nil {
				return results, err
			}
		}

		if outstanding == 0 {
			break
		}

		select {
		case <-doneCh:
			if !cancelled {
				cancelled = true
				doneCh = nil
				if err := cancelPending(ctx.Err()); err != nil {
					return results, err
				}
			}

		case done := <-finished:
			delete(inflight, done.id)
			index := positions[done.id]
			lifecycle := lifecycles[done.id]

			results[index].Err = done.err

			finalState := core.StateCompleted

			switch {
			case errors.Is(done.err, context.Canceled):
				finalState = core.StateCancelled

			case done.err != nil:
				finalState = core.StateFailed
			}

			if err := lifecycle.TransitionTo(
				finalState,
			); err != nil {
				return results, err
			}

			results[index].State = lifecycle.State()

			if !cancelled && ctx.Err() != nil {
				cancelled = true
				doneCh = nil

				if err := cancelPending(ctx.Err()); err != nil {
					return results, err
				}
			}

			if cancelled {
				if err := runner.controller.FinishWithoutPromotion(
					done.id,
				); err != nil {
					return results, err
				}
				delete(reserved, done.id)
			} else {
				promoted, err := runner.controller.Finish(done.id)

				// This operation has already released its reservation
				// if Finish has progressed to the promotion
				if err == nil {
					delete(reserved, done.id)
				}

				for _, operation := range promoted {
					reserved[operation.ID] = struct{}{}
				}

				if err != nil {
					return results, err
				}

				for _, operation := range promoted {
					if err := launch(operation); err != nil {
						return results, err
					}
				}
			}

			outstanding--

			// If there are pending operations but no active ones,
			// no further completion messages will be received from the executor
			if outstanding > 0 && !cancelled {
				active, queued, _ := runner.controller.Snapshot()

				if active == 0 && queued > 0 {
					if err := cancelPending(ErrNoProgress); err != nil {
						return results, err
					}

					return results, ErrNoProgress
				}
			}
		}
	}

	return results, nil
}
