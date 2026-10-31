package execution

import (
	"context"
	"crypto/sha256"
	"errors"
	"pondus-gateway/internal/core"
	"runtime"
)

var ErrInvalidIterations = errors.New("iterations must be greater than zero")

// CPUExecutor simulates a CPU-intensive load
// by performing sequential SHA-256 hash calculations.
type CPUExecutor struct {
	Iterations uint64
}

// Execute performs the configured number of iterations without using
// the operation data.
//
// Cancellation is cooperative: it checks the context every 256 iterations
// and upon completion, so the interruption may not be immediate.
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
