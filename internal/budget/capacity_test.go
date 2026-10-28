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
		t.Fatalf("Snapshot() = (%d, %d), want (%d, %d)",
			used, available, wantUsed, wantAvailable,
		)
	}
}

func TestCapacityReservations(t *testing.T) {
	capacity, err := New(30)
	if err != nil {
		t.Fatal(err)
	}

	assertCapacity(t, capacity, 0, 30)

	for _, reservation := range []struct {
		id   string
		cost uint64
	}{
		{"a", 10},
		{"b", 15},
	} {
		if err := capacity.Reserve(reservation.id, reservation.cost); err != nil {
			t.Fatal(err)
		}
	}

	assertCapacity(t, capacity, 25, 5)

	if err := capacity.Release("a"); err != nil {
		t.Fatal(err)
	}

	assertCapacity(t, capacity, 15, 15)

	if err := capacity.Release("a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf(
			"Release() = %v, want ErrNotFound",
			err,
		)
	}

	assertCapacity(t, capacity, 15, 15)

	// The identifier can be reused after its reservation is released
	if err := capacity.Reserve("a", 5); err != nil {
		t.Fatal(err)
	}

	assertCapacity(t, capacity, 20, 10)
}

func TestCapacityValidation(t *testing.T) {
	if _, err := New(0); err == nil {
		t.Fatal("New(0) must fail")
	}

	capacity, err := New(10)

	if err != nil {
		t.Fatal(err)
	}

	if err := capacity.Reserve("a", 7); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		id   string
		cost uint64
		want error
	}{
		{"duplicate", "a", 1, ErrDuplicate},
		{"insufficient", "b", 4, ErrInsufficient},
		{"empty ID", "", 1, ErrInvalidID},
		{"zero cost", "c", 0, ErrInvalidCost},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := capacity.Reserve(test.id, test.cost); !errors.Is(err, test.want) {
				t.Fatalf(
					"Reserve(%q, %d) = %v, want %v",
					test.id, test.cost, err, test.want,
				)
			}

			// No rejection should alter the existing reservations
			assertCapacity(t, capacity, 7, 3)
		})
	}
}

func TestCapacityUint64Boundary(t *testing.T) {
	capacity, err := New(math.MaxUint64)
	if err != nil {
		t.Fatal(err)
	}

	if err := capacity.Reserve("a", math.MaxUint64-1); err != nil {
		t.Fatal(err)
	}

	if err := capacity.Reserve("b", 2); !errors.Is(err, ErrInsufficient) {
		t.Fatalf(
			"Reserve() = %v, want ErrInsufficient",
			err,
		)
	}

	if err := capacity.Reserve("b", 1); err != nil {
		t.Fatal(err)
	}

	assertCapacity(t, capacity, math.MaxUint64, 0)

	for _, id := range []string{"a", "b"} {
		if err := capacity.Release(id); err != nil {
			t.Fatal(err)
		}
	}

	assertCapacity(t, capacity, 0, math.MaxUint64)
}

func TestCapacityConcurrentReservations(t *testing.T) {
	capacity, err := New(100)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	var successful atomic.Int64

	for i := range 500 {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			err := capacity.Reserve(fmt.Sprintf("op-%d", i), 1)

			switch {
			case err == nil:
				successful.Add(1)
			case errors.Is(err, ErrInsufficient):
			default:
				t.Errorf(
					"unexpected reservation error: %v",
					err,
				)
			}
		}(i)
	}

	wg.Wait()

	if got := successful.Load(); got != 100 {
		t.Fatalf(
			"successful reservations = %d, want 100",
			got,
		)
	}

	assertCapacity(t, capacity, 100, 0)
}

func TestCapacityConcurrentReleaseOnlyOnce(t *testing.T) {
	capacity, err := New(10)
	if err != nil {
		t.Fatal(err)
	}

	if err := capacity.Reserve("a", 8); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	var successful atomic.Int64

	for range 100 {
		wg.Go(func() {

			err := capacity.Release("a")

			switch {
			case err == nil:
				successful.Add(1)
			case errors.Is(err, ErrNotFound):
			default:
				t.Errorf(
					"unexpected release error: %v",
					err,
				)
			}
		})
	}

	wg.Wait()

	if got := successful.Load(); got != 1 {
		t.Fatalf(
			"successful releases = %d, want 1",
			got,
		)
	}

	assertCapacity(t, capacity, 0, 10)
}
