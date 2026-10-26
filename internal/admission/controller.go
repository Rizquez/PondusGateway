package admission

import (
	"errors"
	"fmt"
	"service-gateway/internal/budget"
	"service-gateway/internal/operation"
	"service-gateway/internal/settings"
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
	mutex   sync.Mutex
	limits  settings.Limits
	budget  *budget.Manager
	active  map[string]operation.Operation
	pending []operation.Operation
	known   map[string]struct{}
}

// Creates an admission controller with the specified configuration limits.
//
// Validates the limits and creates a budget manager with the specified maximum
// capacity.
//
// Returns an initialized Controller with no active or pending operations or an
// error if the configuration or budget initialization fails.
func New(limits settings.Limits) (*Controller, error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}

	max, err := budget.New(limits.MaxBudget)
	if err != nil {
		return nil, err
	}

	return &Controller{
		limits: limits,
		budget: max,
		active: make(map[string]operation.Operation),
		known:  make(map[string]struct{}),
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

// Terminates an active operation, releases its budget reservation and
// attempts to promote pending operations to execution.
//
// Pending operations are processed in FIFO order.
//
// Each promotion requires sufficient concurrency and budget.
//
// If the first pending operation does not have sufficient budget,
// processing stops without evaluating subsequent operations.
//
// Returns the operations promoted during the call, in the order they were
// admitted, or an error if the specified operation is not active or if a
// failure occurs while releasing or reserving budget.
//
// If a promotion fails, it returns the operations promoted up to that point
// along with the corresponding error.
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

		pending := controller.pending[0]

		_, available := controller.budget.Snapshot()

		if pending.Cost > available {
			break
		}

		if err := controller.budget.Reserve(pending.ID, pending.Cost); err != nil {
			return promoted, err
		}

		controller.pending[0] = operation.Operation{}
		controller.pending = controller.pending[1:]

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
