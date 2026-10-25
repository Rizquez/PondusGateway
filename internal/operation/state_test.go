package operation

import "testing"

func TestStateTransitions(t *testing.T) {
	tests := []struct {
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

	for _, test := range tests {
		t.Run(string(test.from)+"_to_"+string(test.to),
			func(t *testing.T) {
				got := test.from.CanTransitionTo(test.to)

				if got != test.valid {
					t.Fatalf(
						"got %v, want %v",
						got, test.valid,
					)
				}
			},
		)
	}
}
