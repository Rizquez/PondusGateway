package core

import "testing"

func TestLifecycle(t *testing.T) {
	lifecycle := NewLifecycle()

	if lifecycle.State() != StatePending {
		t.Fatal("new operation must be pending")
	}

	if err := lifecycle.TransitionTo(StateRunning); err != nil {
		t.Fatal(err)
	}

	if err := lifecycle.TransitionTo(StateCompleted); err != nil {
		t.Fatal(err)
	}

	if err := lifecycle.TransitionTo(StateRunning); err == nil {
		t.Fatal("expected invalid transition error")
	}

	if lifecycle.State() != StateCompleted {
		t.Fatal("invalid transition changed state")
	}
}

func TestLifecycleTerminalStates(t *testing.T) {
	terminals := []State{
		StateCompleted,
		StateFailed,
		StateCancelled,
	}

	states := []State{
		StatePending,
		StateRunning,
		StateCompleted,
		StateFailed,
		StateCancelled,
		State("unknown"),
	}

	for _, terminal := range terminals {
		t.Run(string(terminal), func(t *testing.T) {
			lifecycle := NewLifecycle()

			if terminal != StateCancelled {
				if err := lifecycle.TransitionTo(StateRunning); err != nil {
					t.Fatal(err)
				}
			}

			if err := lifecycle.TransitionTo(terminal); err != nil {
				t.Fatal(err)
			}

			for _, state := range states {
				if err := lifecycle.TransitionTo(state); err == nil {
					t.Errorf(
						"expected error: %s -> %s",
						terminal, state,
					)
				}

				if lifecycle.State() != terminal {
					t.Fatalf("state changed after invalid transition")
				}
			}
		})
	}
}

func TestLifecycleRejectsUnknownState(t *testing.T) {
	lifecycle := NewLifecycle()

	err := lifecycle.TransitionTo(State("unknown"))

	if err == nil {
		t.Fatal("expected error for unknown state")
	}

	if lifecycle.State() != StatePending {
		t.Fatal("invalid transition changed state")
	}
}
