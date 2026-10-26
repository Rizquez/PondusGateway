package core

import "fmt"

// Manages the lifecycle of an operation, maintaining
// its current state and controlling the allowed transitions.
type Lifecycle struct {
	state State
}

// Creates a new lifecycle with the initial status Pending.
func NewLifecycle() *Lifecycle {
	return &Lifecycle{state: StatePending}
}

// Returns the current state of the lifecycle.
func (lifecycle *Lifecycle) State() State {
	return lifecycle.state
}

// Attempts to change the current state to the specified state.
//
// The transition must comply with the rules defined by CanTransitionTo.
//
// If the transition is allowed, it updates the state and returns nil.
//
// Otherwise, it returns an error that identifies the source and destination
// states, without modifying the current state.
func (lifecycle *Lifecycle) TransitionTo(next State) error {
	if !lifecycle.state.CanTransitionTo(next) {
		return fmt.Errorf("invalid operation state transition: %s -> %s", lifecycle.state, next)
	}

	lifecycle.state = next
	return nil
}
