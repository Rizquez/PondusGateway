package execution

import (
	"context"
	"pondus-gateway/internal/core"
)

// Executor defines the contract for executing operations.
//
// Implementations must respect context cancellation whenever
// possible.
type Executor interface {
	Execute(ctx context.Context, operation core.Operation) error
}
