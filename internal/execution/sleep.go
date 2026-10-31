package execution

import (
	"context"
	"errors"
	"pondus-gateway/internal/core"
	"time"
)

var ErrInvalidDuration = errors.New("duration must be greater than zero")

// SleepExecutor simulates a workload using a configurable wait.
type SleepExecutor struct {
	Duration time.Duration
}

// Execute waits for the configured duration without using the operation's
// data.
//
// The wait can be interrupted by the context. If the cancellation
// coincides with the timer expiring, its priority is not guaranteed.
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
