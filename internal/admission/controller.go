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

type Decision string

const (
	Started  Decision = "started"
	Queued   Decision = "queued"
	Rejected Decision = "rejected"
)

type Controller struct {
	mutex   sync.Mutex
	limits  settings.Limits
	budget  *budget.Manager
	active  map[string]operation.Operation
	pending []operation.Operation
	known   map[string]struct{}
}

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
		return Rejected, fmt.Errorf(
			"operation cost exceeds maximum budget",
		)
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

// Finish releases a reservation and processes pending transactions using the FIFO method
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

func (controller *Controller) Snapshot() (active, queued int, used uint64) {
	controller.mutex.Lock()
	defer controller.mutex.Unlock()

	used, _ = controller.budget.Snapshot()

	return len(controller.active), len(controller.pending), used
}
