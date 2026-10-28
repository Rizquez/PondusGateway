package scheduler

import "pondus-gateway/internal/core"

// CapacityFit selects the first pending operation whose cost does not
// exceed the available capacity.
//
// Returns the index of the selected operation or -1 when no operation
// can be executed.
type CapacityFit struct{}

// Next iterates through the pending operations in order and returns
// the index of the first one that fits within the available capacity.
//
// It does not modify the operations or the slice it receives.
func (CapacityFit) Next(pending []core.Operation, available uint64) int {
	for index, operation := range pending {
		if operation.Cost <= available {
			return index
		}
	}

	return -1
}
