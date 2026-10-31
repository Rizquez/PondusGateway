package scheduler

import (
	"errors"
	"pondus-gateway/internal/core"
	"sync"
)

// StarvationGuard limits the number of times the first pending
// operation can be preempted to reduce the risk of starvation.
// It is safe under concurrent access.
//
// It does not guarantee a maximum wait time: the protected operation
// still depends on sufficient capacity becoming available.
type StarvationGuard struct {
	mutex       sync.Mutex
	selector    Selector
	maxBypasses uint64
	headID      string
	bypasses    uint64
}

// NewStarvationGuard creates a protected selector with a bypass limit.
//
// Requires a non-null selector and a maxBypasses value greater than zero.
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

// Next prioritizes the first operation when sufficient capacity is available.
// Otherwise, it defers the selection until the advance limit is reached.
//
// The counter is reset when the first operation changes; it may be executed,
// or the queue becomes empty. It returns -1 if a selection cannot be made.
func (starvation *StarvationGuard) Next(pending []core.Operation, available uint64) int {
	starvation.mutex.Lock()
	defer starvation.mutex.Unlock()

	if len(pending) == 0 {
		starvation.headID = ""
		starvation.bypasses = 0
		return -1
	}

	if starvation.headID != pending[0].ID {
		starvation.headID = pending[0].ID
		starvation.bypasses = 0
	}

	if pending[0].Cost <= available {
		starvation.bypasses = 0
		return 0
	}

	if starvation.bypasses >= starvation.maxBypasses {
		return -1
	}

	index := starvation.selector.Next(pending, available)

	if index > 0 && index < len(pending) {
		starvation.bypasses++
	}

	return index
}
