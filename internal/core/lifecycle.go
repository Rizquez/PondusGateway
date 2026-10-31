package core

import "fmt"

// Lifecycle controls the state of an
// operation and its valid transitions.
type Lifecycle struct {
	state State
}

// NewLifecycle creates a lifecycle in the Pending state.
func NewLifecycle() *Lifecycle {
	return &Lifecycle{state: StatePending}
}

func (lifecycle *Lifecycle) State() State {
	return lifecycle.state
}

// TransitionTo changes the state if the transition is allowed;
// if it is invalid, it returns an error without changing the current state.
func (lifecycle *Lifecycle) TransitionTo(state State) error {
	if !lifecycle.state.CanTransitionTo(state) {
		return fmt.Errorf("invalid operation state transition: %s -> %s", lifecycle.state, state)
	}

	lifecycle.state = state
	return nil
}
