package capacity

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestCapacity(t *testing.T) {
	capacity, err := New(30)
	if err != nil {
		t.Fatal(err)
	}

	if err := capacity.Reserve("a", 10); err != nil {
		t.Fatal(err)
	}

	if err := capacity.Reserve("b", 15); err != nil {
		t.Fatal(err)
	}

	// Only 5 units left
	if err := capacity.Reserve("c", 8); !errors.Is(err, ErrInsufficient) {
		t.Fatalf(
			"want insufficient capacity: %v",
			err,
		)
	}

	// It cannot make two reservations with the same ID
	if err := capacity.Reserve("a", 1); !errors.Is(err, ErrDuplicate) {
		t.Fatalf(
			"want duplicate reservation: %v",
			err,
		)
	}

	used, available := capacity.Snapshot()

	if used != 25 || available != 5 {
		t.Fatalf(
			"used=%d available=%d",
			used, available,
		)
	}

	if err := capacity.Release("a"); err != nil {
		t.Fatal(err)
	}

	// A released reservation cannot be released again
	if err := capacity.Release("a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf(
			"want reservation not found: %v",
			err,
		)
	}

	used, available = capacity.Snapshot()

	if used != 15 || available != 15 {
		t.Fatalf(
			"used=%d available=%d",
			used, available,
		)
	}
}

func TestInvalid(t *testing.T) {
	if _, err := New(0); err == nil {
		t.Fatal("expected invalid capacity error")
	}

	capacity, _ := New(1)

	if err := capacity.Reserve("", 1); !errors.Is(err, ErrInvalidID) {
		t.Fatal(err)
	}

	if err := capacity.Reserve("a", 0); !errors.Is(err, ErrInvalidCost) {
		t.Fatal(err)
	}

	if err := capacity.Reserve("a", 2); !errors.Is(err, ErrInsufficient) {
		t.Fatal(err)
	}
}

func TestConcurrentReservations(t *testing.T) {
	capacity, err := New(100)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup

	for i := range 500 {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			id := fmt.Sprintf("op-%d", i)
			_ = capacity.Reserve(id, 1)
		}(i)
	}

	wg.Wait()

	used, available := capacity.Snapshot()

	if used != 100 || available != 0 {
		t.Fatalf(
			"used=%d available=%d, want 100 and 0",
			used, available,
		)
	}
}
