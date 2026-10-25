package operation

import "fmt"

type State string

const (
	StatePending   State = "pending"
	StateRunning   State = "running"
	StateCompleted State = "completed"
	StateFailed    State = "failed"
	StateCancelled State = "cancelled"
)

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

type Lifecycle struct {
	state State
}

func NewLifecycle() *Lifecycle {
	return &Lifecycle{state: StatePending}
}

func (lifecycle *Lifecycle) State() State {
	return lifecycle.state
}

func (lifecycle *Lifecycle) TransitionTo(next State) error {
	if !lifecycle.state.CanTransitionTo(next) {
		return fmt.Errorf(
			"invalid operation state transition: %s -> %s",
			lifecycle.state, next,
		)
	}

	lifecycle.state = next
	return nil
}
