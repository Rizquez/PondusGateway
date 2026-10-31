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

// Decision represents the outcome of an application for admission.
type Decision string

const (
	Started  Decision = "started"
	Queued   Decision = "queued"
	Rejected Decision = "rejected"
)

// The controller manages the acceptance of transactions within concurrency,
// capacity, and queue limits. It prevents duplicate identifiers among active
// and pending transactions and enables safe concurrent access.
type Controller struct {
	mutex    sync.Mutex
	limits   core.Limits
	capacity *budget.Capacity
	selector scheduler.Selector
	active   map[string]core.Operation
	pending  []core.Operation
	known    map[string]struct{}
}

// New creates a controller with FIFO as the default scheduling strategy.
func New(limits core.Limits) (*Controller, error) {
	return NewWithSelector(limits, scheduler.FIFO{})
}

// NewWithSelector creates a controller with the specified limits and selector.
// Requires valid limits and a non-null selector.
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

// NewWithStarvationGuard creates a controller with CapacityFit and
// starvation protection, limited by maxBypasses.
func NewWithStarvationGuard(limits core.Limits, maxBypasses uint64) (*Controller, error) {
	starvation, err := scheduler.NewStarvationGuard(scheduler.CapacityFit{}, maxBypasses)
	if err != nil {
		return nil, err
	}

	return NewWithSelector(limits, starvation)
}

// Submit accepts an operation for immediate execution or queuing,
// depending on available capacity and configured limits.
//
// New operations do not take precedence over those already pending.
// Identifiers remain reserved while the operation is active or in
// the queue.
//
// Returns Rejected with an error if the data is invalid or duplicated.
// If the queue is full, it returns Rejected without an error.
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

	if len(controller.pending) == 0 &&
		len(controller.active) < controller.limits.MaxConcurrent &&
		operation.Cost <= available {
		return controller.start(operation)
	}

	if len(controller.pending) >= controller.limits.MaxQueueSize {
		return Rejected, nil
	}

	controller.enqueue(operation)
	return Queued, nil
}

// start requires the controller's mutex to be acquired.
func (controller *Controller) start(operation core.Operation) (Decision, error) {
	if err := controller.capacity.Reserve(operation.ID, operation.Cost); err != nil {
		return Rejected, err
	}

	controller.active[operation.ID] = operation
	controller.known[operation.ID] = struct{}{}

	return Started, nil
}

// enqueue requires a validated operation, space in the queue
// and the controller's mutex acquired.
func (controller *Controller) enqueue(operation core.Operation) {
	controller.pending = append(controller.pending, operation)
	controller.known[operation.ID] = struct{}{}
}

// Finish releases an active transaction and commits pending ones
// according to the configured strategy.
//
// If a commit fails, it retains the previous changes and returns
// the transactions that have already been committed along with
// the error.
func (controller *Controller) Finish(id string) ([]core.Operation, error) {
	return controller.finish(id, true)
}

// FinishWithoutPromotion releases an active operation without processing the queue.
func (controller *Controller) FinishWithoutPromotion(id string) error {
	_, err := controller.finish(id, false)
	return err
}

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

	return controller.promotePending()
}

// promotePending requires the acquired mutex and delegates the selection
// to the scheduler. It stops promotions when the scheduler returns -1.
//
// Promotions performed before an error occur are not rolled back.
func (controller *Controller) promotePending() ([]core.Operation, error) {
	var promoted []core.Operation

	for len(controller.pending) > 0 && len(controller.active) < controller.limits.MaxConcurrent {
		_, available := controller.capacity.Snapshot()
		index := controller.selector.Next(controller.pending, available)

		if index == -1 {
			break
		}

		if index < 0 || index >= len(controller.pending) {
			return promoted, fmt.Errorf("scheduler returned invalid index: %d", index)
		}

		operation := controller.pending[index]
		if err := controller.capacity.Reserve(operation.ID, operation.Cost); err != nil {
			return promoted, err
		}

		controller.removePending(index)
		controller.active[operation.ID] = operation
		promoted = append(promoted, operation)
	}

	return promoted, nil
}

// removePending requires a valid index and the acquired mutex.
//
// It preserves the relative order of the remaining operations.
func (controller *Controller) removePending(index int) {
	copy(controller.pending[index:], controller.pending[index+1:])
	last := len(controller.pending) - 1
	controller.pending[last] = core.Operation{}
	controller.pending = controller.pending[:last]
}

// CancelPending removes all pending operations and releases
// their identifiers, without affecting active operations.
//
// Returns the removed operations in their original order.
func (controller *Controller) CancelPending() []core.Operation {
	controller.mutex.Lock()
	defer controller.mutex.Unlock()

	cancelled := append(
		[]core.Operation(nil),
		controller.pending...,
	)

	for _, operation := range cancelled {
		delete(controller.known, operation.ID)
	}

	for index := range controller.pending {
		controller.pending[index] = core.Operation{}
	}

	controller.pending = nil

	return cancelled
}

// Snapshot returns a consistent view of the number of
// active and pending operations, as well as reserved
// capacity.
func (controller *Controller) Snapshot() (active int, queued int, used uint64) {
	controller.mutex.Lock()
	defer controller.mutex.Unlock()

	used, _ = controller.capacity.Snapshot()

	return len(controller.active), len(controller.pending), used
}
