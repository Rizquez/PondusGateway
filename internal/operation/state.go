package operation

type State string

const (
	StatePending   State = "pending"
	StateRunning   State = "running"
	StateCompleted State = "completed"
	StateFailed    State = "failed"
	StateCancelled State = "cancelled"
)

/*
From the Pending status, it can start or cancel
From the Running status, it can complete, fail or cancel
Final statuses do not allow for new transitions
*/
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
