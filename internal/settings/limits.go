package settings

import "errors"

// Defines the capacity and consumption constraints
// used to control the execution of operations.
type Limits struct {
	MaxConcurrent int
	MaxBudget     uint64
	MaxQueueSize  int
}

// Checks that the configuration limits comply with
// the established constraints.
//
// The required conditions are:
//   - MaxConcurrent must be greater than zero.
//   - MaxBudget must be greater than zero.
//   - MaxQueueSize cannot be negative; a value of zero is allowed.
//
// Returns an error describing the first violated constraint,
// or nil if all limits are valid.
//
// Does not modify the configuration values.
func (limits Limits) Validate() error {
	if limits.MaxConcurrent <= 0 {
		return errors.New("max concurrent must be greater than zero")
	}

	if limits.MaxBudget == 0 {
		return errors.New("max budget must be greater than zero")
	}

	if limits.MaxQueueSize < 0 {
		return errors.New("max queue size cannot be negative")
	}

	return nil
}
