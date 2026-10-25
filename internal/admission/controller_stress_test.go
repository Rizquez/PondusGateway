package admission

import (
	"errors"
	"fmt"
	"service-gateway/internal/operation"
	"service-gateway/internal/settings"
	"sync"
	"testing"
)

func stressOperation(id string, cost uint64) operation.Operation {
	return operation.Operation{
		ID:       id,
		ClientID: "client-1",
		Type:     "compute",
		Cost:     cost,
	}
}

func TestSaturationAndConcurrentFinish(t *testing.T) {
	const total = 50

	controller, err := New(settings.Limits{
		MaxConcurrent: 5,
		MaxBudget:     5,
		MaxQueueSize:  total - 5,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Record the 50 transactions
	for i := range total {
		id := fmt.Sprintf("op-%d", i)

		decision, err := controller.Submit(stressOperation(id, 1))
		if err != nil {
			t.Fatal(err)
		}

		want := Queued
		if i < 5 {
			want = Started
		}

		if decision != want {
			t.Fatalf("%s: got %s, want %s", id, decision, want)
		}
	}

	active, queued, used := controller.Snapshot()

	if active != 5 || queued != 45 || used != 5 {
		t.Fatalf("initial snapshot: %d %d %d", active, queued, used)
	}

	ids := []string{
		"op-0", "op-1", "op-2", "op-3", "op-4",
	}

	seen := make(map[string]bool)

	for len(ids) > 0 {
		type result struct {
			id       string
			promoted []operation.Operation
			err      error
		}

		results := make(chan result, len(ids))
		var wg sync.WaitGroup

		for _, id := range ids {
			wg.Add(1)

			go func(id string) {
				defer wg.Done()

				promoted, err := controller.Finish(id)

				results <- result{
					id:       id,
					promoted: promoted,
					err:      err,
				}
			}(id)
		}

		wg.Wait()
		close(results)

		ids = nil

		for result := range results {
			if result.err != nil {
				t.Fatal(result.err)
			}

			if seen[result.id] {
				t.Fatalf("finished twice: %s", result.id)
			}

			seen[result.id] = true

			for _, operation := range result.promoted {
				ids = append(ids, operation.ID)
			}
		}

		active, queued, used := controller.Snapshot()

		if active > 5 ||
			queued > 45 ||
			used > 5 ||
			used != uint64(active) {

			t.Fatalf("invalid snapshot: %d %d %d", active, queued, used)
		}
	}

	if len(seen) != total {
		t.Fatalf("finished=%d, want=%d", len(seen), total)
	}

	active, queued, used = controller.Snapshot()

	if active != 0 || queued != 0 || used != 0 {
		t.Fatalf("final snapshot: %d %d %d", active, queued, used)
	}
}

func TestFullQueueAndDuplicateRemainUnchanged(t *testing.T) {
	controller, err := New(settings.Limits{
		MaxConcurrent: 1,
		MaxBudget:     2,
		MaxQueueSize:  1,
	})

	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		id   string
		cost uint64
		want Decision
	}{
		{"a", 2, Started},
		{"b", 1, Queued},
		{"controller", 1, Rejected},
	}

	for _, test := range tests {
		decision, err := controller.Submit(stressOperation(test.id, test.cost))

		if err != nil || decision != test.want {
			t.Fatalf("%s: got %s, err %v", test.id, decision, err)
		}
	}

	// We repeat the ID of an operation in the queue
	decision, err := controller.Submit(stressOperation("b", 1))

	if decision != Rejected || !errors.Is(err, ErrDuplicate) {
		t.Fatalf("duplicate: %s, %v", decision, err)
	}

	// A transaction that exceeds the total budget
	decision, err = controller.Submit(stressOperation("oversized", 3))

	if decision != Rejected || err == nil {
		t.Fatalf("oversized: %s, %v", decision, err)
	}

	active, queued, used := controller.Snapshot()

	if active != 1 || queued != 1 || used != 2 {
		t.Fatalf("unexpected snapshot: %d %d %d", active, queued, used)
	}

	// A pending operation cannot be completed
	if _, err := controller.Finish("b"); !errors.Is(err, ErrNotActive) {
		t.Fatalf("finish queued: %v", err)
	}

	// When A is finished, B should be promoted
	promoted, err := controller.Finish("a")

	if err != nil ||
		len(promoted) != 1 ||
		promoted[0].ID != "b" {

		t.Fatalf("promoted=%v, err=%v", promoted, err)
	}
}

func TestFIFOHeadDoesNotFit(t *testing.T) {
	controller, err := New(settings.Limits{
		MaxConcurrent: 3,
		MaxBudget:     10,
		MaxQueueSize:  3,
	})

	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		id   string
		cost uint64
		want Decision
	}{
		{"a", 6, Started},
		{"b", 4, Started},
		{"big", 8, Queued},
		{"small", 1, Queued},
	}

	for _, test := range tests {
		decision, err := controller.Submit(stressOperation(test.id, test.cost))

		if err != nil || decision != test.want {
			t.Fatalf("%s: got %s, err %v", test.id, decision, err)
		}
	}

	// We released B: 4 units remain available
	promoted, err := controller.Finish("b")

	if err != nil || len(promoted) != 0 {
		t.Fatalf("expected FIFO blocking: %v %v", promoted, err)
	}

	active, queued, used := controller.Snapshot()

	if active != 1 || queued != 2 || used != 6 {
		t.Fatalf("unexpected snapshot: %d %d %d", active, queued, used)
	}

	// Once A is released, both can enter
	promoted, err = controller.Finish("a")

	if err != nil ||
		len(promoted) != 2 ||
		promoted[0].ID != "big" ||
		promoted[1].ID != "small" {

		t.Fatalf("FIFO promotion: %v %v", promoted, err)
	}
}
