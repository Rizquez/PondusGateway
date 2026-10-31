package core

// State represents the status of an
// operation throughout its lifecycle.
type State string

const (
	StatePending   State = "pending"
	StateRunning   State = "running"
	StateCompleted State = "completed"
	StateFailed    State = "failed"
	StateCancelled State = "cancelled"
)

// CanTransitionTo checks whether a transition
// to the specified state is allowed.
//
// Final states do not allow new transitions.
func (state State) CanTransitionTo(next State) bool {
	switch state {
	case StatePending:
		return next == StateRunning || next == StateCancelled

	case StateRunning:
		return next == StateCompleted || next == StateFailed || next == StateCancelled

	default:
		return false
	}
}
