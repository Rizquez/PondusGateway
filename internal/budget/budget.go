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

type Manager struct {
	/*
		Multiple routines could reserve capacity simultaneously;
		the mutex protects reads and modifications of used and reservations,
		ensuring that a reservation is checked and logged within a single critical section
	*/
	mutex        sync.Mutex
	max          uint64
	used         uint64
	reservations map[string]uint64
}

func New(max uint64) (*Manager, error) {
	if max == 0 {
		return nil, errors.New("max budget must be greater than zero")
	}

	return &Manager{
		max:          max,
		reservations: make(map[string]uint64),
	}, nil
}

func (manager *Manager) Reserve(id string, cost uint64) error {
	if id == "" {
		return ErrInvalidID
	}

	if cost == 0 {
		return ErrInvalidCost
	}

	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	/*
		It is necessary to prevent a transaction from reserving funds twice
		and ensure that exactly the amount it reserved is released
	*/
	if _, exists := manager.reservations[id]; exists {
		return ErrDuplicate
	}

	/*
		Instead of first adding used + cost, it is necessary compare the result to the remaining capacity
		This prevents potential overflow when adding uint64 integers
	*/
	if cost > manager.max-manager.used {
		return ErrInsufficient
	}

	manager.reservations[id] = cost
	manager.used += cost

	return nil
}

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

/*
It is necessary to query the counters without directly accessing the manager's internal state d
*/
func (manager *Manager) Snapshot() (used, available uint64) {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	return manager.used, manager.max - manager.used
}
