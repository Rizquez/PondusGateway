package admission

import (
	"errors"
	"fmt"
	"pondus-gateway/internal/budget"
	"pondus-gateway/internal/core"
	"pondus-gateway/internal/scheduler"

	"sync"
)

var (
	ErrDuplicate = errors.New("operation already admitted")
	ErrNotActive = errors.New("operation is not active")
)

// Represents the outcome of an application for admission.
type Decision string

const (
	Started  Decision = "started"
	Queued   Decision = "queued"
	Rejected Decision = "rejected"
)

// Manages the acceptance of operations based on the established
// concurrency, capacity, and queue capacity limits.
//
// It keeps track of active, pending, and known operations, preventing
// duplicate acceptances and managing capacity reserves.
//
// It protects its internal state using a mutex to allow secure access
// from multiple goroutines.
type Controller struct {
	mutex    sync.Mutex
	limits   core.Limits
	capacity *budget.Capacity
	selector scheduler.Selector
	active   map[string]core.Operation
	pending  []core.Operation
	known    map[string]struct{}
}

// Creates a queue controller using the FIFO scheduler as the default
// strategy for selecting pending operations.
//
// Delegates the initialization and validation of the controller
// to NewWithSelector.
//
// Returns the initialized controller or an error if the configuration
// provided is invalid.
func New(limits core.Limits) (*Controller, error) {
	return NewWithSelector(limits, scheduler.FIFO{})
}

// Creates an admission controller with the specified configuration
// limits and scheduler.
//
// It validates the specified limits and checks that the scheduler
// is not nil before initializing the capacity manager.
//
// The controller is initialized with no active or pending operations,
// with the full capacity available and empty records.
//
// Returns a pointer to the created controller or an error if the limits
// are invalid, the scheduler is nil, or the capacity manager initialization
// fails.
func NewWithSelector(limits core.Limits, selector scheduler.Selector) (*Controller, error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}

	if selector == nil {
		return nil, errors.New("scheduler cannot be nil")
	}

	max, err := budget.New(limits.MaxCapacity)
	if err != nil {
		return nil, err
	}

	return &Controller{
		limits:   limits,
		capacity: max,
		selector: selector,
		active:   make(map[string]core.Operation),
		pending:  nil,
		known:    make(map[string]struct{}),
	}, nil
}

// Creates an admission controller using the CapacityFit strategy with
// starvation protection.
//
// The maxBypasses parameter sets the limit on the number of bypasses
// used by the StarvationGuard scheduler to protect pending operations.
//
// Initializes the scheduler using NewStarvationGuard and delegates the
// creation of the controller to NewWithSelector.
//
// Returns a pointer to the created controller or an error if the scheduler
// initialization or controller validation fails.
func NewWithStarvationGuard(limits core.Limits, maxBypasses uint64) (*Controller, error) {
	starvation, err := scheduler.NewStarvationGuard(scheduler.CapacityFit{}, maxBypasses)
	if err != nil {
		return nil, err
	}

	return NewWithSelector(limits, starvation)
}

// Evaluates a transaction and determines whether it can be executed, placed on
// hold or rejected.
//
// The transaction must satisfy the validation constraints, have an unregistered
// identifier, and not exceed the maximum capacity.
//
// A transaction begins immediately when there are no pending transactions,
// concurrency capacity is available, and sufficient capacity is available.
//
// If it cannot begin, it is added to the queue provided there is space.
// Accepted operations are registered to prevent duplicate identifiers.
//
// Returns:
//   - Started if the operation begins immediately.
//   - Queued if the operation is added to the queue.
//   - Rejected if the operation cannot be accepted.
//
// It also returns an error if validation fails, the identifier is duplicated,
// the cost exceeds the maximum capacity or the reservation fails.
//
// Rejection due to lack of space in the queue returns Rejected and nil.
//
// Rejected operations are not logged in the controller.
func (controller *Controller) Submit(operation core.Operation) (Decision, error) {
	if err := operation.Validate(); err != nil {
		return Rejected, err
	}

	controller.mutex.Lock()
	defer controller.mutex.Unlock()

	if _, exists := controller.known[operation.ID]; exists {
		return Rejected, ErrDuplicate
	}

	if operation.Cost > controller.limits.MaxCapacity {
		return Rejected, fmt.Errorf("operation cost exceeds maximum capacity")
	}

	_, available := controller.capacity.Snapshot()

	if len(controller.pending) == 0 && len(controller.active) < controller.limits.MaxConcurrent && operation.Cost <= available {

		if err := controller.capacity.Reserve(operation.ID, operation.Cost); err != nil {
			return Rejected, err
		}

		controller.active[operation.ID] = operation
		controller.known[operation.ID] = struct{}{}

		return Started, nil
	}

	if len(controller.pending) >= controller.limits.MaxQueueSize {
		return Rejected, nil
	}

	controller.pending = append(controller.pending, operation)
	controller.known[operation.ID] = struct{}{}

	return Queued, nil
}

// Terminates an active operation, releases its reserved capacity  and attempts
// to promote pending operations to execution.
//
// The selection of pending operations is delegated to the configured scheduler,
// respecting concurrency and capacity limits.
//
// Returns the promoted operations, in the order in which they were selected or
// an error if the operation is not active, capacity release or reservation fails
// or the scheduler returns an invalid index.
//
// If an error occurs during promotions, it returns the operations promoted up to
// that point along with the corresponding error.
func (controller *Controller) Finish(id string) ([]core.Operation, error) {
	return controller.finish(id, true)
}

// Terminates an active operation and releases its reserved capacity without
// promoting pending operations.
//
// Removes the operation from the active and known operation records, leaving
// the waiting queue intact.
//
// Returns ErrNotActive if the operation is not active an error if capacity
// release fails or nil if the operation completes successfully.
func (controller *Controller) FinishWithoutPromotion(id string) error {
	_, err := controller.finish(id, false)
	return err
}

// Completes an active operation and optionally controls the promotion of pending
// operations.
//
// Releases the reserved capacity and removes the operation from the active and known
// operation records.
//
// If promote is true, it attempts to promote operations using the configured scheduler
// as long as concurrency capacity exists. If false, it terminates without processing the
// waiting queue.
//
// Returns the promoted operations or an error if any of the release, selection or reservation
// operations fail.
//
// If an error occurs during promotions, it preserves the changes made and returns the operations
// promoted up to that point.
//
// All state modifications are performed under mutual exclusion.
func (controller *Controller) finish(id string, promote bool) ([]core.Operation, error) {
	controller.mutex.Lock()
	defer controller.mutex.Unlock()

	if _, exists := controller.active[id]; !exists {
		return nil, ErrNotActive
	}

	if err := controller.capacity.Release(id); err != nil {
		return nil, err
	}

	delete(controller.active, id)
	delete(controller.known, id)

	if !promote {
		return nil, nil
	}

	var promoted []core.Operation

	for len(controller.pending) > 0 && len(controller.active) < controller.limits.MaxConcurrent {

		_, available := controller.capacity.Snapshot()
		index := controller.selector.Next(controller.pending, available)

		if index == -1 {
			break
		}

		if index < 0 || index >= len(controller.pending) {
			return promoted, fmt.Errorf(
				"scheduler returned invalid index: %d",
				index,
			)
		}

		operation := controller.pending[index]

		if err := controller.capacity.Reserve(operation.ID, operation.Cost); err != nil {
			return promoted, err
		}

		copy(controller.pending[index:], controller.pending[index+1:])

		controller.pending[len(controller.pending)-1] = core.Operation{}
		controller.pending = controller.pending[:len(controller.pending)-1]

		controller.active[operation.ID] = operation
		promoted = append(promoted, operation)
	}

	return promoted, nil
}

// Cancels all pending operations in the controller.
//
// Returns a copy of the pending operations in the order in which
// they were stored before cancellation.
//
// Removes their identifiers from the register of known operations
// and empties the queue, without modifying active operations or their
// capacity reservations.
//
// If there are no pending operations, it returns nil.
//
// The operation is performed under mutual exclusion to ensure state
// consistency in the face of concurrent accesses.
func (controller *Controller) CancelPending() []core.Operation {
	controller.mutex.Lock()
	defer controller.mutex.Unlock()

	cancelled := append(
		[]core.Operation(nil),
		controller.pending...,
	)

	for _, op := range cancelled {
		delete(controller.known, op.ID)
	}

	for i := range controller.pending {
		controller.pending[i] = core.Operation{}
	}

	controller.pending = nil

	return cancelled
}

// Takes a snapshot of the controller's current state.
//
// Returns the number of active operations, the number of operations pending
// in the queue and the total amount of reserved capacity.
//
// The query is performed with mutual exclusion to ensure the consistency of
// the returned values.
//
// Does not modify the controller's state.
func (controller *Controller) Snapshot() (active, queued int, used uint64) {
	controller.mutex.Lock()
	defer controller.mutex.Unlock()

	used, _ = controller.capacity.Snapshot()

	return len(controller.active), len(controller.pending), used
}
