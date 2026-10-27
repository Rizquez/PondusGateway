package execution

import (
	"context"
	"pondus-gateway/internal/core"
)

// Defines the contract for executing operations.
//
// Execute receives the operation to be processed and returns an
// error when execution fails.
//
// Implementations should handle the cancellation of the context
// whenever possible.
type Executor interface {
	Execute(ctx context.Context, operation core.Operation) error
}
