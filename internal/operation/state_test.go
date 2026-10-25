package operation

import "testing"

func TestStateTransitions(test *testing.T) {
	cases := []struct {
		from, to State
		valid    bool
	}{
		{StatePending, StateRunning, true},
		{StatePending, StateCancelled, true},
		{StateRunning, StateCompleted, true},
		{StateRunning, StateFailed, true},
		{StateRunning, StateCancelled, true},

		{StatePending, StateCompleted, false},
		{StateCompleted, StateRunning, false},
		{StateFailed, StateRunning, false},
		{StateCancelled, StateRunning, false},
		{StateRunning, StateRunning, false},
		{State("unknown"), StateRunning, false},
	}

	for _, tc := range cases {
		test.Run(string(tc.from)+"_to_"+string(tc.to),
			func(test *testing.T) {
				got := tc.from.CanTransitionTo(tc.to)

				if got != tc.valid {
					test.Fatalf(
						"got %v, want %v",
						got, tc.valid,
					)
				}
			},
		)
	}
}

func TestLifecycle(test *testing.T) {
	life := NewLifecycle()

	if life.State() != StatePending {
		test.Fatal("new operation must be pending")
	}

	if err := life.TransitionTo(StateRunning); err != nil {
		test.Fatal(err)
	}

	if err := life.TransitionTo(StateCompleted); err != nil {
		test.Fatal(err)
	}

	if err := life.TransitionTo(StateRunning); err == nil {
		test.Fatal("expected invalid transition error")
	}

	if life.State() != StateCompleted {
		test.Fatal("invalid transition changed state")
	}
}

func TestLifecycleTerminalStates(test *testing.T) {
	terminals := []State{
		StateCompleted,
		StateFailed,
		StateCancelled,
	}

	allStates := []State{
		StatePending,
		StateRunning,
		StateCompleted,
		StateFailed,
		StateCancelled,
		State("unknown"),
	}

	for _, terminal := range terminals {
		test.Run(string(terminal), func(test *testing.T) {
			life := NewLifecycle()

			if terminal != StateCancelled {
				if err := life.TransitionTo(StateRunning); err != nil {
					test.Fatal(err)
				}
			}

			if err := life.TransitionTo(terminal); err != nil {
				test.Fatal(err)
			}

			for _, next := range allStates {
				if err := life.TransitionTo(next); err == nil {
					test.Errorf(
						"expected error: %s -> %s",
						terminal, next,
					)
				}

				if life.State() != terminal {
					test.Fatalf(
						"state changed after invalid transition",
					)
				}
			}
		})
	}
}

func TestLifecycleRejectsUnknownState(test *testing.T) {
	life := NewLifecycle()

	err := life.TransitionTo(State("unknown"))

	if err == nil {
		test.Fatal("expected error for unknown state")
	}

	if life.State() != StatePending {
		test.Fatal("invalid transition changed state")
	}
}
