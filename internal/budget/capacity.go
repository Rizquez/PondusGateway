package budget

import (
	"errors"
	"sync"
)

var (
	ErrInvalidCost  = errors.New("cost must be greater than zero")
	ErrInvalidID    = errors.New("reservation ID cannot be empty")
	ErrDuplicate    = errors.New("reservation already exists")
	ErrInsufficient = errors.New("insufficient capacity")
	ErrNotFound     = errors.New("reservation not found")
)

// Capacity manages individually identified reservations without exceeding
// the configured maximum capacity. Its operations are safe against concurrent
// access.
type Capacity struct {
	mutex        sync.Mutex
	max          uint64
	used         uint64
	reservations map[string]uint64
}

// New creates a capacity manager with a maximum value greater than zero.
func New(max uint64) (*Capacity, error) {
	if max == 0 {
		return nil, errors.New("max capacity must be greater than zero")
	}

	return &Capacity{
		max:          max,
		reservations: make(map[string]uint64),
	}, nil
}

// Reserve capacity for a unique identifier.
//
// The availability check and the reservation registration
// are atomic with respect to other administrator operations.
//
// If it fails, the state remains unchanged.
func (capacity *Capacity) Reserve(id string, cost uint64) error {
	if id == "" {
		return ErrInvalidID
	}

	if cost == 0 {
		return ErrInvalidCost
	}

	capacity.mutex.Lock()
	defer capacity.mutex.Unlock()

	if _, exists := capacity.reservations[id]; exists {
		return ErrDuplicate
	}

	// Prevent uint64 overflow when checking the remaining capacity
	if cost > capacity.max-capacity.used {
		return ErrInsufficient
	}

	capacity.reservations[id] = cost
	capacity.used += cost

	return nil
}

// Release frees exactly the capacity associated with a reservation.
//
// If the identifier does not exist, it returns ErrNotFound without
// changing the state.
func (capacity *Capacity) Release(id string) error {
	capacity.mutex.Lock()
	defer capacity.mutex.Unlock()

	cost, exists := capacity.reservations[id]
	if !exists {
		return ErrNotFound
	}

	delete(capacity.reservations, id)
	capacity.used -= cost

	return nil
}

// Snapshot consistently returns the used and available capacity.
func (capacity *Capacity) Snapshot() (used, available uint64) {
	capacity.mutex.Lock()
	defer capacity.mutex.Unlock()

	return capacity.used, capacity.max - capacity.used
}
