package budget

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
)

func assertCapacity(t *testing.T, capacity *Capacity, wantUsed uint64, wantAvailable uint64) {
	t.Helper()

	used, available := capacity.Snapshot()

	if used != wantUsed || available != wantAvailable {
		t.Fatalf(
			"capacity: used=%d, available=%d; want used=%d, available=%d",
			used, available, wantUsed, wantAvailable,
		)
	}
}

func TestMaximumCapacity(t *testing.T) {
	capacity, err := New(math.MaxUint64)

	if err != nil {
		t.Fatal(err)
	}

	if err := capacity.Reserve("a", math.MaxUint64-1); err != nil {
		t.Fatal(err)
	}

	// Only one unit is left
	if err := capacity.Reserve("b", 2); !errors.Is(err, ErrInsufficient) {
		t.Fatalf(
			"expected insufficient capacity, got %v",
			err,
		)
	}

	if err := capacity.Reserve("b", 1); err != nil {
		t.Fatal(err)
	}

	assertCapacity(t, capacity, math.MaxUint64, 0)

	if err := capacity.Release("a"); err != nil {
		t.Fatal(err)
	}

	if err := capacity.Release("b"); err != nil {
		t.Fatal(err)
	}

	assertCapacity(t, capacity, 0, math.MaxUint64)
}

func TestFailedReservationsDoNotChangeCapacity(t *testing.T) {
	capacity, _ := New(10)

	if err := capacity.Reserve("a", 7); err != nil {
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
		err := capacity.Reserve(attempt.id, attempt.cost)

		if !errors.Is(err, attempt.want) {
			t.Fatalf(
				"Reserve(%q, %d): got %v, want %v",
				attempt.id, attempt.cost, err, attempt.want,
			)
		}

		assertCapacity(t, capacity, 7, 3)
	}
}

func TestConcurrentReleaseOnlyOnce(t *testing.T) {
	capacity, _ := New(10)

	if err := capacity.Reserve("a", 8); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	var success atomic.Int64

	for range 100 {
		wg.Go(func() {
			err := capacity.Release("a")

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

	assertCapacity(t, capacity, 0, 10)
}

func TestRepeatedReserveReleaseCycles(t *testing.T) {
	capacity, _ := New(50)

	for i := range 1000 {
		id := fmt.Sprintf("op-%d", i)

		if err := capacity.Reserve(id, 50); err != nil {
			t.Fatal(err)
		}

		assertCapacity(t, capacity, 50, 0)

		if err := capacity.Release(id); err != nil {
			t.Fatal(err)
		}

		assertCapacity(t, capacity, 0, 50)
	}
}
