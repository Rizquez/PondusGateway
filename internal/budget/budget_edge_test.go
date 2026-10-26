package budget

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
)

func assertBudget(t *testing.T, manager *Manager, wantUsed, wantAvailable uint64) {
	t.Helper()

	used, available := manager.Snapshot()

	if used != wantUsed || available != wantAvailable {
		t.Fatalf(
			"budget: used=%d, available=%d; want used=%d, available=%d",
			used, available, wantUsed, wantAvailable,
		)
	}
}

func TestMaximumBudget(t *testing.T) {
	manager, err := New(math.MaxUint64)

	if err != nil {
		t.Fatal(err)
	}

	if err := manager.Reserve("a", math.MaxUint64-1); err != nil {
		t.Fatal(err)
	}

	// Only one unit is left
	if err := manager.Reserve("b", 2); !errors.Is(err, ErrInsufficient) {
		t.Fatalf(
			"expected insufficient budget, got %v",
			err,
		)
	}

	if err := manager.Reserve("b", 1); err != nil {
		t.Fatal(err)
	}

	assertBudget(t, manager, math.MaxUint64, 0)

	if err := manager.Release("a"); err != nil {
		t.Fatal(err)
	}

	if err := manager.Release("b"); err != nil {
		t.Fatal(err)
	}

	assertBudget(t, manager, 0, math.MaxUint64)
}

func TestFailedReservationsDoNotChangeBudget(t *testing.T) {
	manager, _ := New(10)

	if err := manager.Reserve("a", 7); err != nil {
		t.Fatal(err)
	}

	attempts := []struct {
		id   string
		cost uint64
		want error
	}{
		{"a", 1, ErrDuplicate},
		{"b", 4, ErrInsufficient},
		{"", 1, ErrInvalidID},
		{"c", 0, ErrInvalidCost},
	}

	for _, attempt := range attempts {
		err := manager.Reserve(attempt.id, attempt.cost)

		if !errors.Is(err, attempt.want) {
			t.Fatalf(
				"Reserve(%q, %d): got %v, want %v",
				attempt.id, attempt.cost, err, attempt.want,
			)
		}

		assertBudget(t, manager, 7, 3)
	}
}

func TestConcurrentReleaseOnlyOnce(t *testing.T) {
	manager, _ := New(10)

	if err := manager.Reserve("a", 8); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	var success atomic.Int64

	for range 100 {
		wg.Go(func() {
			err := manager.Release("a")

			switch {
			case err == nil:
				success.Add(1)

			case errors.Is(err, ErrNotFound):
				// Another goroutine has already released the reservation

			default:
				t.Errorf(
					"unexpected release error: %v",
					err,
				)
			}
		})
	}

	wg.Wait()

	if got := success.Load(); got != 1 {
		t.Fatalf(
			"successful releases = %d, want 1",
			got,
		)
	}

	assertBudget(t, manager, 0, 10)
}

func TestRepeatedReserveReleaseCycles(t *testing.T) {
	manager, _ := New(50)

	for i := range 1000 {
		id := fmt.Sprintf("op-%d", i)

		if err := manager.Reserve(id, 50); err != nil {
			t.Fatal(err)
		}

		assertBudget(t, manager, 50, 0)

		if err := manager.Release(id); err != nil {
			t.Fatal(err)
		}

		assertBudget(t, manager, 0, 50)
	}
}
