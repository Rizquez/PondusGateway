package domain

import "errors"

var (
	ErrMaxConcurrent = errors.New("max concurrent must be greater than zero")
	ErrMaxCapacity   = errors.New("max capacity must be greater than zero")
	ErrMaxQueue      = errors.New("max queue size cannot be negative")
)

// Limits defines the concurrency, capacity and queue
// constraints applied to transaction admission.
type Limits struct {
	MaxConcurrent int
	MaxCapacity   uint64
	MaxQueueSize  int
}

// Validate checks that the limits are valid.
// MaxQueueSize can be zero, which prevents operations from being queued.
func (limits Limits) Validate() error {
	if limits.MaxConcurrent <= 0 {
		return ErrMaxConcurrent
	}

	if limits.MaxCapacity == 0 {
		return ErrMaxCapacity
	}

	if limits.MaxQueueSize < 0 {
		return ErrMaxQueue
	}

	return nil
}
