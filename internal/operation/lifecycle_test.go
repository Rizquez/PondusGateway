package operation

import "testing"

func TestLifecycle(t *testing.T) {
	life := NewLifecycle()

	if life.State() != StatePending {
		t.Fatal("new operation must be pending")
	}

	if err := life.TransitionTo(StateRunning); err != nil {
		t.Fatal(err)
	}

	if err := life.TransitionTo(StateCompleted); err != nil {
		t.Fatal(err)
	}

	if err := life.TransitionTo(StateRunning); err == nil {
		t.Fatal("expected invalid transition error")
	}

	if life.State() != StateCompleted {
		t.Fatal("invalid transition changed state")
	}
}

func TestLifecycleTerminalStates(t *testing.T) {
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
		t.Run(string(terminal), func(t *testing.T) {
			life := NewLifecycle()

			if terminal != StateCancelled {
				if err := life.TransitionTo(StateRunning); err != nil {
					t.Fatal(err)
				}
			}

			if err := life.TransitionTo(terminal); err != nil {
				t.Fatal(err)
			}

			for _, next := range allStates {
				if err := life.TransitionTo(next); err == nil {
					t.Errorf(
						"expected error: %s -> %s",
						terminal, next,
					)
				}

				if life.State() != terminal {
					t.Fatalf("state changed after invalid transition")
				}
			}
		})
	}
}

func TestLifecycleRejectsUnknownState(t *testing.T) {
	life := NewLifecycle()

	err := life.TransitionTo(State("unknown"))

	if err == nil {
		t.Fatal("expected error for unknown state")
	}

	if life.State() != StatePending {
		t.Fatal("invalid transition changed state")
	}
}
