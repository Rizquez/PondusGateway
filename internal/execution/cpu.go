package execution

import (
	"context"
	"crypto/sha256"
	"errors"
	"pondus-gateway/internal/core"
	"runtime"
)

var ErrInvalidIterations = errors.New("iterations must be greater than zero")

// CPUExecutor implements an executor that simulates a CPU-intensive
// workload by performing repeated SHA-256 hash calculations.
type CPUExecutor struct {
	Iterations uint64
}

// Execute performs a CPU workload by sequentially computing SHA-256 hashes for
// the configured number of iterations.
//
// The number of iterations must be greater than zero.
//
// The received operation does not affect the calculations performed.
//
// Checks the context status before starting the calculations, periodically every
// 256 iterations, and upon completion of execution.
//
// Returns:
//   - ErrInvalidIterations if the number of iterations is zero.
//   - The context’s error if it is canceled or times out.
//   - nil if all iterations complete without errors.
//
// Cancellation is cooperative and is detected at the context’s checkpoints, though
// not necessarily immediately.
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
