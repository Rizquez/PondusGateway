package admission

import (
	"errors"
	"fmt"
	"pondus-gateway/internal/budget"
	"pondus-gateway/internal/operation"
	"pondus-gateway/internal/scheduler"
	"pondus-gateway/internal/settings"
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
// concurrency, budget, and queue capacity limits.
//
// It keeps track of active, pending, and known operations,
// preventing duplicate acceptances and managing budget reserves.
//
// It protects its internal state using a mutex to allow
// secure access from multiple goroutines.
type Controller struct {
	mutex    sync.Mutex
	limits   settings.Limits
	budget   *budget.Manager
	selector scheduler.Selector
	active   map[string]operation.Operation
	pending  []operation.Operation
	known    map[string]struct{}
}

// Creates a queue controller using the FIFO scheduler as the
// default strategy for selecting pending operations.
//
// Delegates the initialization and validation of the controller
// to NewWithSelector.
//
// Returns the initialized controller or an error if the configuration
// provided is invalid.
func New(limits settings.Limits) (*Controller, error) {
	return NewWithSelector(limits, scheduler.FIFO{})
}

// Creates an admission controller with the specified configuration
// limits and scheduler.
//
// It validates the specified limits and checks that the scheduler
// is not nil before initializing the budget manager.
//
// The controller is initialized with no active or pending operations,
// with the full budget available and empty records.
//
// Returns a pointer to the created controller or an error if the limits
// are invalid, the scheduler is nil, or the budget manager initialization
// fails.
func NewWithSelector(limits settings.Limits, selector scheduler.Selector) (*Controller, error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}

	if selector == nil {
		return nil, errors.New("scheduler cannot be nil")
	}

	max, err := budget.New(limits.MaxBudget)
	if err != nil {
		return nil, err
	}

	return &Controller{
		limits:   limits,
		budget:   max,
		selector: selector,
		active:   make(map[string]operation.Operation),
		pending:  nil,
		known:    make(map[string]struct{}),
	}, nil
}

// Evaluates a transaction and determines whether it can be executed, placed on
// hold or rejected.
//
// The transaction must satisfy the validation constraints, have an unregistered
// identifier, and not exceed the maximum budget.
//
// A transaction begins immediately when there are no pending transactions,
// concurrency capacity is available, and sufficient budget is available.
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
// the cost exceeds the maximum budget or the reservation fails.
//
// Rejection due to lack of space in the queue returns Rejected and nil.
//
// Rejected operations are not logged in the controller.
func (controller *Controller) Submit(operation operation.Operation) (Decision, error) {
	if err := operation.Validate(); err != nil {
		return Rejected, err
	}

	controller.mutex.Lock()
	defer controller.mutex.Unlock()

	if _, exists := controller.known[operation.ID]; exists {
		return Rejected, ErrDuplicate
	}

	if operation.Cost > controller.limits.MaxBudget {
		return Rejected, fmt.Errorf("operation cost exceeds maximum budget")
	}

	_, available := controller.budget.Snapshot()

	if len(controller.pending) == 0 && len(controller.active) < controller.limits.MaxConcurrent && operation.Cost <= available {

		if err := controller.budget.Reserve(operation.ID, operation.Cost); err != nil {
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

// Finish terminates an active operation, releases its budget allocation and
// attempts to promote pending operations to execution.
//
// The selection of pending operations is delegated to the configured scheduler,
// which determines the next operation to execute based on the available budget.
//
// Promotions continue as long as there are pending operations, concurrency capacity
// is available, and the scheduler selects an operation.
//
// Returns the operations promoted during the call, in the order in which they were
// selected, or an error if:
//   - The specified operation is not active.
//   - Budget release or reservation fails.
//   - The planner returns an index outside the allowed range.
//   - The selected operation exceeds the available budget.
//
// If the scheduler returns -1, processing ends without an error.
//
// If an error occurs during promotions, it returns the operations promoted up to that
// point along with the corresponding error.
//
// Access to the controller’s state is protected by mutual exclusion.
func (controller *Controller) Finish(id string) ([]operation.Operation, error) {
	controller.mutex.Lock()
	defer controller.mutex.Unlock()

	if _, exists := controller.active[id]; !exists {
		return nil, ErrNotActive
	}

	if err := controller.budget.Release(id); err != nil {
		return nil, err
	}

	delete(controller.active, id)
	delete(controller.known, id)

	var promoted []operation.Operation

	for len(controller.pending) > 0 &&
		len(controller.active) < controller.limits.MaxConcurrent {

		_, available := controller.budget.Snapshot()

		index := controller.selector.Next(
			controller.pending,
			available,
		)

		if index == -1 {
			break
		}

		if index < 0 || index >= len(controller.pending) {
			return promoted, fmt.Errorf(
				"scheduler returned invalid index: %d",
				index,
			)
		}

		pending := controller.pending[index]

		if pending.Cost > available {
			return promoted, fmt.Errorf("scheduler selected operation exceeding available budget")
		}

		if err := controller.budget.Reserve(pending.ID, pending.Cost); err != nil {
			return promoted, err
		}

		copy(
			controller.pending[index:],
			controller.pending[index+1:],
		)

		controller.pending[len(controller.pending)-1] = operation.Operation{}
		controller.pending = controller.pending[:len(controller.pending)-1]

		controller.active[pending.ID] = pending
		promoted = append(promoted, pending)
	}

	return promoted, nil
}

// Takes a snapshot of the controller's current state.
//
// Returns the number of active operations, the number of operations pending
// in the queue and the total amount of reserved budget.
//
// The query is performed with mutual exclusion to ensure the consistency of
// the returned values.
//
// Does not modify the controller's state.
func (controller *Controller) Snapshot() (active, queued int, used uint64) {
	controller.mutex.Lock()
	defer controller.mutex.Unlock()

	used, _ = controller.budget.Snapshot()

	return len(controller.active), len(controller.pending), used
}
