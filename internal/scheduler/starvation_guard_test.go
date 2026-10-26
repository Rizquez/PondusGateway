package scheduler

import (
	"pondus-gateway/internal/core"
	"testing"
)

func TestStarvationGuard(t *testing.T) {
	guard, err := NewStarvationGuard(CapacityFit{}, 2)

	if err != nil {
		t.Fatal(err)
	}

	pending := []core.Operation{
		{ID: "big", Cost: 8},
		{ID: "small1", Cost: 1},
		{ID: "small2", Cost: 2},
		{ID: "small3", Cost: 1},
	}

	// First pass
	if got := guard.Next(pending, 4); got != 1 {
		t.Fatalf(
			"first selection = %d, want 1",
			got,
		)
	}

	// Simulation of the removal of small1
	pending = append(pending[:1], pending[2:]...)

	// Second pass
	if got := guard.Next(pending, 3); got != 1 {
		t.Fatalf(
			"second selection = %d, want 1",
			got,
		)
	}

	// Simulation of the removal of small2
	pending = append(pending[:1], pending[2:]...)

	// Overtaking big is not allowed
	if got := guard.Next(pending, 3); got != -1 {
		t.Fatalf(
			"guard selection = %d, want -1",
			got,
		)
	}

	// When big is an option, it should be selected
	if got := guard.Next(pending, 8); got != 0 {
		t.Fatalf(
			"head selection = %d, want 0",
			got,
		)
	}

	// The following item is checked
	pending = pending[1:]

	if got := guard.Next(pending, 1); got != 0 {
		t.Fatalf(
			"new head selection = %d, want 0",
			got,
		)
	}
}

func TestStarvationGuardInvalidConfig(t *testing.T) {
	if _, err := NewStarvationGuard(nil, 1); err == nil {
		t.Fatal("expected error for nil selector")
	}

	if _, err := NewStarvationGuard(CapacityFit{}, 0); err == nil {
		t.Fatal("expected error for zero bypasses")
	}
}
