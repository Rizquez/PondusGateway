package continuous

import (
	"context"
	"errors"
	"fmt"
	"pondus-gateway/internal/control/admission"
	"pondus-gateway/internal/domain"
	"pondus-gateway/internal/execution"
	"sync"
)

var (
	ErrRequired    = errors.New("controller and executor are required")
	ErrStarted     = errors.New("coordinator has already started")
	ErrNotStarted  = errors.New("coordinator has not started")
	ErrClosed      = errors.New("coordinator is closing")
	ErrOwnerID     = errors.New("owner ID cannot be empty")
	ErrOwnerClosed = errors.New("owner no longer accepts operations")
)

// Result represents the final outcome of an operation,
// including its owner, admission decision and status.
type Result struct {
	OperationID string
	OwnerID     string
	Decision    admission.Decision
	State       domain.State
	Err         error
}

// The Coordinator manages the admission and ongoing execution
// of operations from different owners through a single controller.
//
// It centralizes state changes and allows operations from one owner
// to be canceled without interrupting those of the others.
type Coordinator struct {
	controller    *admission.Controller
	executor      execution.Executor
	mutex         sync.Mutex
	started       bool
	requests      chan submission
	cancellations chan ownerCancellation
	completed     chan completion
	stop          chan struct{}
	done          chan struct{}
	stopOnce      sync.Once
	closeErr      error
}

// submission refers to an application for admission and the channel
// used to communicate the response.
type submission struct {
	ownerID   string
	operation domain.Operation
	reply     chan submissionResponse
}

// ownerCancellation requests the closure of an owner and allows errors
// related to its cancellation to be reported.
type ownerCancellation struct {
	ownerID string
	reply   chan error
}

// submissionResponse communicates the admission decision and provides
// a result channel for accepted operations.
type submissionResponse struct {
	decision admission.Decision
	result   <-chan Result
	err      error
}

// completion reports the result of an asynchronous execution
// to the coordination loop.
type completion struct {
	id  string
	err error
}

// operationEntry keeps track of a supported operation, including its owner,
// result and cancellation function.
type operationEntry struct {
	operation domain.Operation
	ownerID   string
	decision  admission.Decision
	result    chan Result
	cancel    context.CancelFunc
	started   bool
}

// internalState maintains the internal state of the coordination loop.
//
// Its records are managed by a single goroutine, including owners
// that no longer support operations.
type internalState struct {
	coordinator  *Coordinator
	owner        *admission.Session
	entries      map[string]*operationEntry
	closedOwners map[string]struct{}
	closing      bool
	stopCh       <-chan struct{}
	err          error
}

// New creates a coordinator with the specified controller and executor.
func New(controller *admission.Controller, executor execution.Executor) (*Coordinator, error) {
	if controller == nil || executor == nil {
		return nil, ErrRequired
	}

	return &Coordinator{
		controller:    controller,
		executor:      executor,
		requests:      make(chan submission),
		cancellations: make(chan ownerCancellation),
		completed:     make(chan completion),
		stop:          make(chan struct{}),
		done:          make(chan struct{}),
	}, nil
}

// Start takes exclusive ownership of the controller and begins continuous request processing.
func (coordinator *Coordinator) Start() error {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	if coordinator.started {
		return ErrStarted
	}

	owner, err := coordinator.controller.Acquire()
	if err != nil {
		return err
	}

	coordinator.started = true
	go coordinator.run(owner)
	return nil
}

// Submit requests the admission of a transaction associated with an owner.
//
// Returns the admission decision and, if accepted, a channel that will receive
// exactly one final result before it is closed.
//
// Rejects transactions from owners that have already been closed.
func (coordinator *Coordinator) Submit(ownerID string, operation domain.Operation) (admission.Decision, <-chan Result, error) {
	if ownerID == "" {
		return admission.Rejected, nil, ErrOwnerID
	}

	coordinator.mutex.Lock()
	started := coordinator.started
	coordinator.mutex.Unlock()
	if !started {
		return admission.Rejected, nil, ErrNotStarted
	}

	reply := make(chan submissionResponse, 1)
	select {
	case <-coordinator.stop:
		return admission.Rejected, nil, ErrClosed
	case <-coordinator.done:
		return admission.Rejected, nil, ErrClosed
	case coordinator.requests <- submission{ownerID: ownerID, operation: operation, reply: reply}:
	}

	outcome := <-reply
	return outcome.decision, outcome.result, outcome.err
}

// CancelOwner prevents the owner from performing new operations and cancels
// its pending operations, requesting that active operations be canceled.
//
// It does not affect other owners and allows repeated calls.
func (coordinator *Coordinator) CancelOwner(ownerID string) error {
	if ownerID == "" {
		return ErrOwnerID
	}

	coordinator.mutex.Lock()
	started := coordinator.started
	coordinator.mutex.Unlock()
	if !started {
		return ErrNotStarted
	}

	reply := make(chan error, 1)
	select {
	case <-coordinator.stop:
		return ErrClosed
	case <-coordinator.done:
		return ErrClosed
	case coordinator.cancellations <- ownerCancellation{ownerID: ownerID, reply: reply}:
	}

	return <-reply
}

// Close requests that the coordinator be closed and waits for the process to complete.
//
// Cancels pending operations and requests that active operations be canceled.
//
// The context limits the wait, but does not interrupt the closure process that has already begun.
func (coordinator *Coordinator) Close(ctx context.Context) error {
	coordinator.mutex.Lock()
	started := coordinator.started
	coordinator.mutex.Unlock()
	if !started {
		return ErrNotStarted
	}

	coordinator.stopOnce.Do(func() { close(coordinator.stop) })
	select {
	case <-coordinator.done:
		return coordinator.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// run processes requests, cancellations and terminations sequentially,
// maintaining exclusive ownership of the controller.
//
// Before terminating, it waits for registered operations to complete and
// releases the controller session.
func (coodinator *Coordinator) run(owner *admission.Session) {
	state := &internalState{
		coordinator:  coodinator,
		owner:        owner,
		entries:      make(map[string]*operationEntry),
		closedOwners: make(map[string]struct{}),
		stopCh:       coodinator.stop,
	}

	defer func() {
		coodinator.closeErr = errors.Join(state.err, owner.Release())
		close(coodinator.done)
	}()

	for {
		if state.closing && len(state.entries) == 0 {
			return
		}

		select {
		case <-state.stopCh:
			state.abort(context.Canceled)
		case request := <-coodinator.requests:
			state.submit(request)
		case cancellation := <-coodinator.cancellations:
			state.cancelOwner(cancellation)
		case done := <-coodinator.completed:
			state.complete(done)
		}
	}
}

// publish returns the final result of an operation, closes its channel
// and deletes its tracking record.
func (state *internalState) publish(entry *operationEntry, status domain.State, err error) {
	entry.result <- Result{
		OperationID: entry.operation.ID,
		OwnerID:     entry.ownerID,
		Decision:    entry.decision,
		State:       status,
		Err:         err,
	}
	close(entry.result)
	delete(state.entries, entry.operation.ID)
}

// launch initiates the asynchronous execution of a supported operation.
//
// It registers its cancellation function and reports completio to the coordinator,
// converting any recovered panics into errors.
func (state *internalState) launch(entry *operationEntry) {
	entry.started = true
	ctx, cancel := context.WithCancel(context.Background())
	entry.cancel = cancel
	go func(op domain.Operation) {
		var err error
		defer func() {
			if value := recover(); value != nil {
				err = fmt.Errorf("executor panic: %v", value)
			}
			state.coordinator.completed <- completion{id: op.ID, err: err}
		}()
		err = state.coordinator.executor.Execute(ctx, op)
	}(entry.operation)
}

// observeStop checks whether the coordinator has been requested to shut down
// and triggers its cancellation before continuing to process operations.
func (state *internalState) observeStop() {
	select {
	case <-state.coordinator.stop:
		state.abort(context.Canceled)
	default:
	}
}

// abort initiates the termination of processing once, cancels pending
// operations and requests the cancellation of active ones.
//
// It accumulates the errors that occur during the cancellation.
func (state *internalState) abort(reason error) {
	if state.closing {
		return
	}

	state.closing = true
	state.stopCh = nil
	pending, err := state.owner.CancelPending()
	state.err = errors.Join(state.err, err)

	for _, operation := range pending {
		if entry := state.entries[operation.ID]; entry != nil {
			state.publish(entry, domain.StateCancelled, reason)
		}
	}

	for _, entry := range state.entries {
		if entry.cancel != nil {
			entry.cancel()
		}
	}
}

// cancelOwner closes an owner to new admissions and cancels its pending
// transactions, requesting that active ones be canceled.
//
// It preserves the transactions of other owners and reports cancellation
// errors without closing the coordinator.
func (state *internalState) cancelOwner(request ownerCancellation) {
	select {
	case <-state.coordinator.stop:
		state.abort(context.Canceled)
	default:
	}

	if state.closing {
		request.reply <- ErrClosed
		return
	}

	if _, exists := state.closedOwners[request.ownerID]; exists {
		request.reply <- nil
		return
	}

	state.closedOwners[request.ownerID] = struct{}{}

	var cancelErr error
	for _, current := range state.entries {
		if current.ownerID != request.ownerID {
			continue
		}

		if current.started {
			current.cancel()
			continue
		}

		_, err := state.owner.CancelPendingByID(current.operation.ID)
		if err != nil {
			cancelErr = errors.Join(cancelErr, err)
			continue
		}

		state.publish(current, domain.StateCancelled, context.Canceled)
	}
	request.reply <- cancelErr
}

// submit processes an admission request and logs the operations that have
// been accepted for tracking.
//
// It immediately marks accepted requests as Started and rejects requests
// during closure or from closed owners.
func (state *internalState) submit(request submission) {
	select {
	case <-state.coordinator.stop:
		state.abort(context.Canceled)
	default:
	}

	if state.closing {
		request.reply <- submissionResponse{decision: admission.Rejected, err: ErrClosed}
		return
	}

	if _, closed := state.closedOwners[request.ownerID]; closed {
		request.reply <- submissionResponse{decision: admission.Rejected, err: ErrOwnerClosed}
		return
	}

	decision, err := state.owner.Submit(request.operation)
	if err != nil || decision == admission.Rejected {
		request.reply <- submissionResponse{decision: decision, err: err}
		return
	}

	entry := &operationEntry{
		operation: request.operation,
		ownerID:   request.ownerID,
		decision:  decision,
		result:    make(chan Result, 1),
	}

	state.entries[request.operation.ID] = entry
	if decision == admission.Started {
		state.launch(entry)
	}

	request.reply <- submissionResponse{
		decision: decision,
		result:   entry.result,
	}
}

// complete processes the completion of an operation and releases its lock.
//
// It promotes new operations as long as the coordinator remains active.
//
// During shutdown, it releases the promoted operations without executing them.
//
// In the event of coordination errors, it initiates a general rollback.
func (state *internalState) complete(done completion) {
	entry := state.entries[done.id]
	if entry == nil {
		return
	}

	if entry.cancel != nil {
		entry.cancel()
	}

	state.observeStop()

	var promoted []domain.Operation
	var err error
	if state.closing {
		err = state.owner.FinishWithoutPromotion(done.id)
	} else {
		promoted, err = state.owner.Finish(done.id)
	}

	status := domain.StateCompleted
	if errors.Is(done.err, context.Canceled) {
		status = domain.StateCancelled
	} else if done.err != nil {
		status = domain.StateFailed
	}

	state.publish(entry, status, done.err)

	if err != nil {
		state.err = errors.Join(state.err, err)
		for _, op := range promoted {
			if pending := state.entries[op.ID]; pending != nil {
				pending.started = true
				_ = state.owner.FinishWithoutPromotion(op.ID)
				state.publish(pending, domain.StateCancelled, err)
			}
		}

		state.abort(err)
		return
	}

	state.observeStop()

	if state.closing {
		for _, operation := range promoted {
			if next := state.entries[operation.ID]; next != nil {
				releaseErr := state.owner.FinishWithoutPromotion(operation.ID)
				state.err = errors.Join(state.err, releaseErr)
				state.publish(next, domain.StateCancelled, context.Canceled)
			}
		}

		return
	}

	for _, operation := range promoted {
		if next := state.entries[operation.ID]; next != nil {
			state.launch(next)
		}
	}
}
