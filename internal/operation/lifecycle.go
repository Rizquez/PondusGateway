package operation

import "fmt"

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
