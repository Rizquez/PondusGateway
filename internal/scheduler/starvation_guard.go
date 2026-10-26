package scheduler

import (
	"errors"
	"pondus-gateway/internal/core"
	"sync"
)

// Implements a scheduler that limits preemptions of the first pending
// operation to reduce the risk of starvation.
//
// It uses an underlying selector to choose operations when the first
// one does not have sufficient capacity.
//
// It maintains a headway counter for the first pending operation and
// blocks new selections when the set limit is reached.
//
// It protects its internal state using a mutex to allow safe access
// from multiple goroutines.
type StarvationGuard struct {
	mutex       sync.Mutex
	selector    Selector
	maxBypasses uint64
	headID      string
	bypasses    uint64
}

// Creates a scheduler with protection against unlimited bypasses
// of the first pending operation.
//
// It takes a selector that determines the candidate operations and
// a maximum number of allowed bypasses.
//
// The selector cannot be nil, and maxBypasses must be greater than
// zero.
//
// Returns a pointer to the initialized scheduler or an error if any
// of the parameters do not meet the constraints.
func NewStarvationGuard(selector Selector, maxBypasses uint64) (*StarvationGuard, error) {
	if selector == nil {
		return nil, errors.New("selector cannot be nil")
	}

	if maxBypasses == 0 {
		return nil, errors.New("max bypasses must be greater than zero")
	}

	return &StarvationGuard{
		selector:    selector,
		maxBypasses: maxBypasses,
	}, nil
}

// Next selects a pending operation based on available capacity and the configured
// advance limit.
//
// The first operation takes priority when its cost does not exceed the available
// capacity.
//
// If there is insufficient capacity, it delegates the selection to the underlying
// scheduler as long as the maximum number of advances has not been reached.
//
// The counter increments when the selector returns a valid index after the first
// position. It resets when the identifier of the first operation changes, when that
// operation can be executed or when the queue becomes empty.
//
// Returns:
//   - 0 if the first operation has sufficient capacity.
//   - An index provided by the underlying selector when selection is permitted.
//   - -1 if the queue is empty or the advance limit has been reached.
//
// Selection and counter updates are performed under mutual exclusion.
//
// TODO: The lock limits preemptions, but does not guarantee a maximum wait time;
// the execution of the locked operation still depends on sufficient capacity becoming
// available.
func (guard *StarvationGuard) Next(pending []core.Operation, available uint64) int {
	guard.mutex.Lock()
	defer guard.mutex.Unlock()

	if len(pending) == 0 {
		guard.headID = ""
		guard.bypasses = 0
		return -1
	}

	// Resets the counter when the first pending operation changes
	if guard.headID != pending[0].ID {
		guard.headID = pending[0].ID
		guard.bypasses = 0
	}

	// Prioritize the first operation when sufficient capacity is available
	if pending[0].Cost <= available {
		guard.bypasses = 0
		return 0
	}

	// Prevents further advances when the limit is reached
	if guard.bypasses >= guard.maxBypasses {
		return -1
	}

	index := guard.selector.Next(pending, available)

	if index > 0 && index < len(pending) {
		guard.bypasses++
	}

	return index
}
