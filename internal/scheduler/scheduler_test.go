package scheduler

import (
	"pondus-gateway/internal/core"
	"reflect"
	"sync"
	"testing"
)

func TestFIFO(t *testing.T) {
	tests := []struct {
		name      string
		pending   []core.Operation
		available uint64
		want      int
	}{
		{"empty queue", nil, 10, -1},
		{"first fits", []core.Operation{{ID: "a", Cost: 3}, {ID: "b", Cost: 1}}, 4, 0},
		{"first blocks smaller", []core.Operation{{ID: "a", Cost: 8}, {ID: "b", Cost: 1}}, 4, -1},
		{"exact capacity", []core.Operation{{ID: "a", Cost: 5}}, 5, 0},
		{"no capacity", []core.Operation{{ID: "a", Cost: 1}}, 0, -1},
		{"excess capacity", []core.Operation{{ID: "a", Cost: 2}}, 100, 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			before := append([]core.Operation(nil), test.pending...)
			got := (FIFO{}).Next(test.pending, test.available)

			if got != test.want {
				t.Fatalf(
					"Next() = %d, want %d",
					got, test.want,
				)
			}

			if !reflect.DeepEqual(test.pending, before) {
				t.Fatal("FIFO modified the queue")
			}
		})
	}
}

func TestCapacityFit(t *testing.T) {
	tests := []struct {
		name      string
		pending   []core.Operation
		available uint64
		want      int
	}{
		{"empty queue", nil, 10, -1},
		{"none fits", []core.Operation{{ID: "a", Cost: 9}, {ID: "b", Cost: 8}}, 3, -1},
		{"first fits", []core.Operation{{ID: "a", Cost: 2}, {ID: "b", Cost: 1}}, 3, 0},
		{"later fits", []core.Operation{{ID: "a", Cost: 12}, {ID: "b", Cost: 5}, {ID: "c", Cost: 3}}, 8, 1},
		{"exact capacity", []core.Operation{{ID: "a", Cost: 12}, {ID: "b", Cost: 8}}, 8, 1},
		{"zero capacity", []core.Operation{{ID: "a", Cost: 1}}, 0, -1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			before := append([]core.Operation(nil), test.pending...)
			got := (CapacityFit{}).Next(test.pending, test.available)

			if got != test.want {
				t.Fatalf(
					"Next() = %d, want %d",
					got, test.want,
				)
			}

			if !reflect.DeepEqual(test.pending, before) {
				t.Fatal("CapacityFit modified the queue")
			}
		})
	}
}

func TestStarvationGuardValidation(t *testing.T) {
	if _, err := NewStarvationGuard(nil, 1); err == nil {
		t.Fatal("an error due to a nil selector was expected")
	}

	if _, err := NewStarvationGuard(CapacityFit{}, 0); err == nil {
		t.Fatal("an error was expected due to a maximum of zero overtakes")
	}
}

func TestStarvationGuardBypassLimit(t *testing.T) {
	starvation, err := NewStarvationGuard(CapacityFit{}, 2)
	if err != nil {
		t.Fatal(err)
	}

	pending := []core.Operation{
		{ID: "big", Cost: 8},
		{ID: "small1", Cost: 1},
		{ID: "small2", Cost: 2},
		{ID: "small3", Cost: 1},
	}

	if got := starvation.Next(pending, 4); got != 1 {
		t.Fatalf(
			"first pass = %d, want 1",
			got,
		)
	}

	pending = append(pending[:1], pending[2:]...)
	if got := starvation.Next(pending, 3); got != 1 {
		t.Fatalf(
			"second pass = %d, want 1",
			got,
		)
	}

	pending = append(pending[:1], pending[2:]...)
	if got := starvation.Next(pending, 3); got != -1 {
		t.Fatalf(
			"after reaching the limit t= %d, want -1",
			got,
		)
	}

	if got := starvation.Next(pending, 8); got != 0 {
		t.Fatalf(
			"the header takes precedence = %d, want 0",
			got,
		)
	}

	pending = pending[1:]
	if got := starvation.Next(pending, 1); got != 0 {
		t.Fatalf(
			"new header = %d, want 0",
			got,
		)
	}
}

func TestStarvationGuardResetsOnHeadChangeAndEmptyQueue(t *testing.T) {
	starvation, err := NewStarvationGuard(CapacityFit{}, 1)
	if err != nil {
		t.Fatal(err)
	}

	pending := []core.Operation{{ID: "a", Cost: 9}, {ID: "b", Cost: 1}}
	if got := starvation.Next(pending, 1); got != 1 {
		t.Fatalf(
			"initial lead = %d, want 1",
			got,
		)
	}

	if got := starvation.Next(pending, 1); got != -1 {
		t.Fatalf(
			"overtaking limit = %d, want -1",
			got,
		)
	}

	// When the ID of the first operation changes, another counter starts
	pending = []core.Operation{{ID: "c", Cost: 9}, {ID: "d", Cost: 1}}
	if got := starvation.Next(pending, 1); got != 1 {
		t.Fatalf(
			"counter after header change = %d, want 1",
			got,
		)
	}

	if got := starvation.Next(nil, 1); got != -1 {
		t.Fatalf(
			"empty tail = %d, want -1",
			got,
		)
	}

	if got := starvation.Next(pending, 1); got != 1 {
		t.Fatalf(
			"counter after clearing the line = %d, want 1",
			got,
		)
	}
}

func TestStarvationGuardNoCandidate(t *testing.T) {
	starvation, err := NewStarvationGuard(CapacityFit{}, 1)
	if err != nil {
		t.Fatal(err)
	}

	pending := []core.Operation{{ID: "a", Cost: 9}, {ID: "b", Cost: 4}}
	if got := starvation.Next(pending, 1); got != -1 {
		t.Fatalf(
			"no candidate = %d, want -1",
			got,
		)
	}

	// A search without a candidate does not use up the allowed advance
	if got := starvation.Next(pending, 4); got != 1 {
		t.Fatalf(
			"after an unsuccessful search = %d, want 1",
			got,
		)
	}
}

func TestStarvationGuardConcurrentCalls(t *testing.T) {
	starvation, err := NewStarvationGuard(CapacityFit{}, 1)
	if err != nil {
		t.Fatal(err)
	}

	pending := []core.Operation{{ID: "a", Cost: 9}, {ID: "b", Cost: 1}}

	const workers = 32
	results := make(chan int, workers)
	var wg sync.WaitGroup

	for range workers {
		wg.Go(func() {
			results <- starvation.Next(pending, 1)
		})
	}

	wg.Wait()
	close(results)

	selected := 0
	for got := range results {
		switch got {
		case 1:
			selected++
		case -1:
		default:
			t.Fatalf(
				"unexpected index: %d",
				got,
			)
		}
	}

	if selected != 1 {
		t.Fatalf(
			"missed advances = %d, want 1",
			selected,
		)
	}
}
