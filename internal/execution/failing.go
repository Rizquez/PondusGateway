package execution

import (
	"context"
	"errors"
	"pondus-gateway/internal/core"
)

var ErrMissingFailure = errors.New("failure error must not be nil")

// FailingExecutor implements an executor that simulates the failure
// of an operation by returning a configurable error.
type FailingExecutor struct {
	Err error
}

// Execute simulates the failure of an operation by returning the error configured
// in the executor.
//
// The Err field must contain an error other than nil.
//
// The received operation does not affect the executor's behavior.
//
// Checks the context's status before returning the configured error, giving priority
// to cancellation errors or time-out errors.
//
// Returns:
//   - ErrMissingFailure if no error has been configured.
//   - The context error if the operation has already been canceled or has timed out.
//   - The error configured in Err in all other cases.
//
// It does not perform any additional work or modify the received operation.
func (executor FailingExecutor) Execute(ctx context.Context, _ core.Operation) error {
	if executor.Err == nil {
		return ErrMissingFailure
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	return executor.Err
}
