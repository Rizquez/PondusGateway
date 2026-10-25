package budget

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestManager(t *testing.T) {
	manager, err := New(30)
	if err != nil {
		t.Fatal(err)
	}

	if err := manager.Reserve("a", 10); err != nil {
		t.Fatal(err)
	}

	if err := manager.Reserve("b", 15); err != nil {
		t.Fatal(err)
	}

	// Only 5 units left
	if err := manager.Reserve("c", 8); !errors.Is(err, ErrInsufficient) {
		t.Fatalf("want insufficient budget: %v", err)
	}

	// It cannot make two reservations with the same ID
	if err := manager.Reserve("a", 1); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("want duplicate reservation: %v", err)
	}

	used, available := manager.Snapshot()

	if used != 25 || available != 5 {
		t.Fatalf("used=%d available=%d", used, available)
	}

	if err := manager.Release("a"); err != nil {
		t.Fatal(err)
	}

	// A released reservation cannot be released again
	if err := manager.Release("a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want reservation not found: %v", err)
	}

	used, available = manager.Snapshot()

	if used != 15 || available != 15 {
		t.Fatalf("used=%d available=%d", used, available)
	}
}

func TestInvalid(t *testing.T) {
	if _, err := New(0); err == nil {
		t.Fatal("expected invalid budget error")
	}

	manager, _ := New(1)

	if err := manager.Reserve("", 1); !errors.Is(err, ErrInvalidID) {
		t.Fatal(err)
	}

	if err := manager.Reserve("a", 0); !errors.Is(err, ErrInvalidCost) {
		t.Fatal(err)
	}

	if err := manager.Reserve("a", 2); !errors.Is(err, ErrInsufficient) {
		t.Fatal(err)
	}
}

func TestConcurrentReservations(t *testing.T) {
	manager, err := New(100)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup

	for i := range 500 {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			id := fmt.Sprintf("op-%d", i)
			_ = manager.Reserve(id, 1)
		}(i)
	}

	wg.Wait()

	used, available := manager.Snapshot()

	if used != 100 || available != 0 {
		t.Fatalf(
			"used=%d available=%d, want 100 and 0",
			used, available,
		)
	}
}
