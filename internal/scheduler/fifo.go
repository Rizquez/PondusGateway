package scheduler

import "pondus-gateway/internal/core"

// Selector determines which pending operation can begin
// using the available capacity.
type Selector interface {
	Next(pending []core.Operation, available uint64) int
}

// FIFO implements first-in, first-out selection.
type FIFO struct{}

// Next takes the queue of operations and the available capacity.
//
// Returns:
//   - 0: Selects the first operation.
//   - -1: No operation can be selected under this policy.
func (FIFO) Next(pending []core.Operation, available uint64) int {
	if len(pending) == 0 {
		return -1
	}

	if pending[0].Cost > available {
		return -1
	}

	return 0
}
