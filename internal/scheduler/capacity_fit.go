package scheduler

import "pondus-gateway/internal/operation"

// CapacityFit selects the first pending operation
// whose cost does not exceed the available budget.
//
// Returns the index of the selected operation or -1
// when no operation can be executed.
type CapacityFit struct{}

// Next iterates through the pending operations in order
// and returns the index of the first one that fits within
// the available budget.
//
// It does not modify the operations or the slice it receives.
//
// TODO: Its time complexity is O(n) in the worst case, where n
// is the number of pending operations, because it may need to
// traverse the entire queue. The auxiliary memory is O(1).
func (CapacityFit) Next(pending []operation.Operation, available uint64) int {
	for index, operation := range pending {
		if operation.Cost <= available {
			return index
		}
	}

	return -1
}
