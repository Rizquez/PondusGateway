package budget

import (
	"errors"
	"sync"
)

var (
	ErrInvalidCost  = errors.New("cost must be greater than zero")
	ErrInvalidID    = errors.New("reservation ID cannot be empty")
	ErrDuplicate    = errors.New("reservation already exists")
	ErrInsufficient = errors.New("insufficient budget")
	ErrNotFound     = errors.New("reservation not found")
)

// Manager manages a limited budget using individually identified reservations,
// tracking used and available capacity.
//
// It allows reservations to be made and released securely in the face of concurrent
// access, protecting the internal state using a mutex.
//
// Mutex protects concurrent access to used and reservations, ensuring that checks
// and modifications to the reservations are performed atomically.
type Manager struct {
	mutex        sync.Mutex
	max          uint64
	used         uint64
	reservations map[string]uint64
}

// Creates a budget manager with the specified maximum capacity.
func New(max uint64) (*Manager, error) {
	if max == 0 {
		return nil, errors.New("max budget must be greater than zero")
	}

	return &Manager{
		max:          max,
		reservations: make(map[string]uint64),
	}, nil
}

// Reserve attempts to reserve a portion of the budget for the specified
// identifier.
//
// The identifier cannot be empty, and the cost must be greater than zero.
//
// Each identifier can have only one active reservation.
//
// The reservation is only recorded if there is sufficient budget.
//
// Availability checking and budget updating are performed mutually exclusive
// to ensure concurrent safety.
//
// Returns:
//   - ErrInvalidID if the identifier is empty.
//   - ErrInvalidCost if the cost is zero.
//   - ErrDuplicate if the identifier already has an active reservation.
//   - ErrInsufficient if the available budget is insufficient.
//   - nil if the reservation is successful.
//
// If the reservation fails, the budget and existing reservations remain
// unchanged.
func (manager *Manager) Reserve(id string, cost uint64) error {
	if id == "" {
		return ErrInvalidID
	}

	if cost == 0 {
		return ErrInvalidCost
	}

	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	// Prevents a reservation from being made twice with the same identifier
	// and allows the exact amount that was reserved to be released later
	if _, exists := manager.reservations[id]; exists {
		return ErrDuplicate
	}

	// Compare the cost with the remaining budget to avoid
	// a possible overflow when adding uint64 values
	if cost > manager.max-manager.used {
		return ErrInsufficient
	}

	manager.reservations[id] = cost
	manager.used += cost

	return nil
}

// Frees up the budget associated with an existing reservation.
//
// It searches for the reservation by its identifier and if it exists,
// removes it from the record and deducts its cost from the used budget.
//
// The operation is performed under mutual exclusion to ensure state
// consistency in the face of concurrent access.
//
// Returns ErrNotFound if the identifier does not correspond to an active
// reservation, or nil if the budget is successfully released.
//
// If the reservation does not exist, the administrator’s status remains
// unchanged.
func (manager *Manager) Release(id string) error {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	cost, exists := manager.reservations[id]
	if !exists {
		return ErrNotFound
	}

	delete(manager.reservations, id)
	manager.used -= cost

	return nil
}

// Takes a snapshot of the current budget status.
func (manager *Manager) Snapshot() (used, available uint64) {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	return manager.used, manager.max - manager.used
}
