package scheduler

import "pondus-gateway/internal/core"

// Selector defines the contract for selecting pending transactions.
//
// Next must return a valid index or -1 if none can be selected.
type Selector interface {
	Next(pending []core.Operation, available uint64) int
}

// FIFO enforces a strict first-in, first-out policy,
// preventing operations from cutting in line even if
// there is capacity for other operations.
type FIFO struct{}

// Next selects the first operation if there is sufficient capacity.
//
// Returns -1 if the queue is empty or the first operation cannot be
// executed.
func (FIFO) Next(pending []core.Operation, available uint64) int {
	if len(pending) == 0 {
		return -1
	}

	if pending[0].Cost > available {
		return -1
	}

	return 0
}
