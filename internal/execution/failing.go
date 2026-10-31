package execution

import (
	"context"
	"errors"
	"pondus-gateway/internal/core"
)

var ErrMissingFailure = errors.New("failure error must not be nil")

// FailingExecutor simulates an execution
// failure by returning a configurable error.
type FailingExecutor struct {
	Err error
}

// Execute prioritizes validating Err, followed by canceling
// the context, before returning the configured error.
func (executor FailingExecutor) Execute(ctx context.Context, _ core.Operation) error {
	if executor.Err == nil {
		return ErrMissingFailure
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	return executor.Err
}
