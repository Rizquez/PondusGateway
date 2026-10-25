package settings

import "errors"

type Limits struct {
	MaxConcurrent int
	MaxBudget     uint64
	MaxQueueSize  int
}

func (limits Limits) Validate() error {
	if limits.MaxConcurrent <= 0 {
		return errors.New(
			"max concurrent must be greater than zero",
		)
	}

	if limits.MaxBudget == 0 {
		return errors.New(
			"max budget must be greater than zero",
		)
	}

	if limits.MaxQueueSize < 0 {
		return errors.New(
			"max queue size cannot be negative",
		)
	}

	return nil
}
