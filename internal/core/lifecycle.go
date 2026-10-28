package core

import "fmt"

// Lifecycle manages the lifecycle of an operation, maintaining
// its current state and controlling the allowed transitions.
type Lifecycle struct {
	state State
}

// NewLifecycle creates a new lifecycle with the initial status Pending.
func NewLifecycle() *Lifecycle {
	return &Lifecycle{state: StatePending}
}

// State returns the current state of the lifecycle.
func (lifecycle *Lifecycle) State() State {
	return lifecycle.state
}

// TransitionTo attempts to change the current state to the specified state.
//
// The transition must comply with the rules defined by CanTransitionTo.
//
// If the transition is allowed, it updates the state and returns nil.
//
// Otherwise, it returns an error that identifies the source and destination
// states, without modifying the current state.
func (lifecycle *Lifecycle) TransitionTo(state State) error {
	if !lifecycle.state.CanTransitionTo(state) {
		return fmt.Errorf("invalid operation state transition: %s -> %s", lifecycle.state, state)
	}

	lifecycle.state = state
	return nil
}
