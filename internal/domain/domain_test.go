package domain

import "testing"

func TestOperationValidate(t *testing.T) {
	valid := Operation{
		ID:       "operation-1",
		ClientID: "client-1",
		Type:     "compute",
		Cost:     20,
	}

	tests := []struct {
		name      string
		operation Operation
		wantError bool
	}{
		{"valid", valid, false},

		{"empty ID", Operation{
			ClientID: "client-1",
			Type:     "compute",
			Cost:     20,
		}, true},

		{"blank client", Operation{
			ID:       "operation-1",
			ClientID: "   ",
			Type:     "compute",
			Cost:     20,
		}, true},

		{"empty type", Operation{
			ID:       "operation-1",
			ClientID: "client-1",
			Cost:     20,
		}, true},

		{"zero cost", Operation{
			ID:       "operation-1",
			ClientID: "client-1",
			Type:     "compute",
		}, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.operation.Validate()

			if (err != nil) != test.wantError {
				t.Fatalf(
					"Validate() err=%v, wantError=%v",
					err, test.wantError,
				)
			}
		})
	}
}

func TestLimitsValidate(t *testing.T) {
	tests := []struct {
		name    string
		limits  Limits
		wantErr bool
	}{
		{
			name: "valid",
			limits: Limits{
				MaxConcurrent: 8,
				MaxCapacity:   30,
				MaxQueueSize:  100,
			},
			wantErr: false,
		},
		{
			name: "queue disabled",
			limits: Limits{
				MaxConcurrent: 1,
				MaxCapacity:   1,
				MaxQueueSize:  0,
			},
			wantErr: false,
		},
		{
			name: "zero concurrency",
			limits: Limits{
				MaxConcurrent: 0,
				MaxCapacity:   30,
				MaxQueueSize:  100,
			},
			wantErr: true,
		},
		{
			name: "negative concurrency",
			limits: Limits{
				MaxConcurrent: -1,
				MaxCapacity:   30,
				MaxQueueSize:  100,
			},
			wantErr: true,
		},
		{
			name: "zero capacity",
			limits: Limits{
				MaxConcurrent: 8,
				MaxCapacity:   0,
				MaxQueueSize:  100,
			},
			wantErr: true,
		},
		{
			name: "negative queue",
			limits: Limits{
				MaxConcurrent: 8,
				MaxCapacity:   30,
				MaxQueueSize:  -1,
			},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.limits.Validate()

			if (err != nil) != test.wantErr {
				t.Fatalf(
					"Validate() error = %v, wantErr = %v",
					err, test.wantErr,
				)
			}
		})
	}
}

func TestStateTransitions(t *testing.T) {
	states := []State{
		StatePending, StateRunning, StateCompleted,
		StateFailed, StateCancelled, State("unknown"),
	}
	valid := map[[2]State]bool{
		{StatePending, StateRunning}:   true,
		{StatePending, StateCancelled}: true,
		{StateRunning, StateCompleted}: true,
		{StateRunning, StateFailed}:    true,
		{StateRunning, StateCancelled}: true,
	}

	for _, from := range states {
		for _, to := range states {
			want := valid[[2]State{from, to}]
			if got := from.CanTransitionTo(to); got != want {
				t.Errorf(
					"%s -> %s: got %v, want %v",
					from, to, got, want,
				)
			}
		}
	}
}

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
