package execution

import (
	"context"
	"errors"
	"pondus-gateway/internal/core"
	"time"
)

var ErrInvalidDuration = errors.New("duration must be greater than zero")

type SleepExecutor struct {
	Duration time.Duration
}

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
