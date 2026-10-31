package scheduler

import "pondus-gateway/internal/core"

// CapacityFit allows pending operations to be brought
// forward when previous ones cannot be executed due to
// a lack of capacity, without guaranteeing that starvation
// will not occur.
type CapacityFit struct{}

// Next returns the index of the first operation that fits within
// the available capacity or -1 if none meet the condition.
func (CapacityFit) Next(pending []core.Operation, available uint64) int {
	for index, operation := range pending {
		if operation.Cost <= available {
			return index
		}
	}

	return -1
}
