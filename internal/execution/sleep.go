package execution

import (
	"context"
	"errors"
	"pondus-gateway/internal/core"
	"time"
)

var ErrInvalidDuration = errors.New("duration must be greater than zero")

// SleepExecutor implements an executor that simulates
// a workload by waiting for a configurable duration.
type SleepExecutor struct {
	Duration time.Duration
}

// Execute simulates the execution of an operation by waiting for the duration
// specified in Duration.
//
// The configured duration must be greater than zero.
//
// The received operation does not affect the executor's behavior.
//
// Checks the context's state before starting the wait and uses a timer to control
// its duration.
//
// Returns:
//   - ErrInvalidDuration if the duration is less than or equal to zero.
//   - The context’s error if cancellation or expiration is detected.
//   - nil if the timer expires before cancellation is detected.
//
// The wait can be interrupted by the context without the full duration having elapsed.
func (executor SleepExecutor) Execute(ctx context.Context, _ core.Operation) error {
	if executor.Duration <= 0 {
		return ErrInvalidDuration
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	timer := time.NewTimer(executor.Duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()

	case <-timer.C:
		return nil
	}
}
