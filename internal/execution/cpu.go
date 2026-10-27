package execution

import (
	"context"
	"crypto/sha256"
	"errors"
	"pondus-gateway/internal/core"
	"runtime"
)

var ErrInvalidIterations = errors.New("iterations must be greater than zero")

type CPUExecutor struct {
	Iterations uint64
}

func (executor CPUExecutor) Execute(ctx context.Context, _ core.Operation) error {
	if executor.Iterations == 0 {
		return ErrInvalidIterations
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	var digest [32]byte

	for i := uint64(0); i < executor.Iterations; i++ {
		if i%256 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}

		digest = sha256.Sum256(digest[:])
	}

	runtime.KeepAlive(digest)

	return ctx.Err()
}
