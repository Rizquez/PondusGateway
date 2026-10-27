package execution

import (
	"context"
	"errors"
	"pondus-gateway/internal/core"
)

var ErrMissingFailure = errors.New("failure error must not be nil")

type FailingExecutor struct {
	Err error
}

func (executor FailingExecutor) Execute(ctx context.Context, _ core.Operation) error {
	if executor.Err == nil {
		return ErrMissingFailure
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	return executor.Err
}
