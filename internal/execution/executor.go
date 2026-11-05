package execution

import (
	"context"
	"pondus-gateway/internal/core"
)

// Executor defines the contract for executing an operation.
//
// Execute must respect the cancellation of the context and
// terminate in a timely manner so that Runner can reclaim
// the reservations.
//
// The context does not interrupt a blocked execution on its
// own: Runner waits for Execute to return.
//
// The error returned determines the final status of the operation.
// If Execute returns nil, it is considered complete, even if the
// context has expired.
type Executor interface {
	Execute(ctx context.Context, operation core.Operation) error
}
