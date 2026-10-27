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

// Manages a limited capacity using individually identified reservations, tracking
// used and available capacity.
//
// It allows reservations to be made and released securely in the face of concurrent
// access, protecting the internal state using a mutex.
//
// Mutex protects concurrent access to used and reservations, ensuring that checks
// and modifications to the reservations are performed atomically.
type Capacity struct {
	mutex        sync.Mutex
	max          uint64
	used         uint64
	reservations map[string]uint64
}

// Creates a capacity capacity with the specified maximum capacity.
func New(max uint64) (*Capacity, error) {
	if max == 0 {
		return nil, errors.New("max capacity must be greater than zero")
	}

	return &Capacity{
		max:          max,
		reservations: make(map[string]uint64),
	}, nil
}

// Attempts to reserve a portion of the capacity for the specified identifier.
//
// The identifier cannot be empty, and the cost must be greater than zero.
//
// Each identifier can have only one active reservation.
//
// The reservation is only recorded if there is sufficient capacity.
//
// Availability checking and capacity updating are performed mutually exclusive
// to ensure concurrent safety.
//
// Returns:
//   - ErrInvalidID if the identifier is empty.
//   - ErrInvalidCost if the cost is zero.
//   - ErrDuplicate if the identifier already has an active reservation.
//   - ErrInsufficient if the available capacity is insufficient.
//   - nil if the reservation is successful.
//
// If the reservation fails, the capacity and existing reservations remain
// unchanged.
func (capacity *Capacity) Reserve(id string, cost uint64) error {
	if id == "" {
		return ErrInvalidID
	}

	if cost == 0 {
		return ErrInvalidCost
	}

	capacity.mutex.Lock()
	defer capacity.mutex.Unlock()

	// Prevents a reservation from being made twice with the same identifier
	// and allows the exact amount that was reserved to be released later
	if _, exists := capacity.reservations[id]; exists {
		return ErrDuplicate
	}

	// Compare the cost with the remaining capacity to avoid
	// a possible overflow when adding uint64 values
	if cost > capacity.max-capacity.used {
		return ErrInsufficient
	}

	capacity.reservations[id] = cost
	capacity.used += cost

	return nil
}

// Frees up the capacity associated with an existing reservation.
//
// It searches for the reservation by its identifier and if it exists,
// removes it from the record and deducts its cost from the used capacity.
//
// The operation is performed under mutual exclusion to ensure state
// consistency in the face of concurrent access.
//
// Returns ErrNotFound if the identifier does not correspond to an active
// reservation, or nil if the capacity is successfully released.
//
// If the reservation does not exist, the administrator’s status remains
// unchanged.
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

// Takes a snapshot of the current capacity status.
func (capacity *Capacity) Snapshot() (used, available uint64) {
	capacity.mutex.Lock()
	defer capacity.mutex.Unlock()

	return capacity.used, capacity.max - capacity.used
}
