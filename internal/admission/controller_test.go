package admission

import (
	"errors"
	"fmt"
	"pondus-gateway/internal/core"
	"pondus-gateway/internal/scheduler"
	"strings"
	"sync"
	"testing"
)

type brokenSelector struct{ calls int }

func (selector *brokenSelector) Next(pending []core.Operation, available uint64) int {
	selector.calls++
	if selector.calls == 1 {
		return 1
	}
	return len(pending)
}

func testOperation(id string, cost uint64) core.Operation {
	return core.Operation{
		ID:       id,
		ClientID: "client-1",
		Type:     "compute",
		Cost:     cost,
	}
}

func assertSnapshot(t *testing.T, controller *Controller, wantActive, wantQueued int, wantUsed uint64) {
	t.Helper()
	active, queued, used := controller.Snapshot()
	if active != wantActive || queued != wantQueued || used != wantUsed {
		t.Fatalf(
			"snapshot = (%d,%d,%d), want (%d,%d,%d)",
			active, queued, used, wantActive, wantQueued, wantUsed,
		)
	}
}

func TestAdmission(t *testing.T) {
	controller, err := New(core.Limits{
		MaxConcurrent: 2,
		MaxCapacity:   10,
		MaxQueueSize:  2,
	})

	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		id   string
		cost uint64
		want Decision
	}{
		{"a", 7, Started},
		{"b", 3, Started},
		{"controller", 4, Queued},
		{"d", 1, Queued},
		{"e", 1, Rejected},
	}

	for _, test := range tests {
		got, err := controller.Submit(testOperation(test.id, test.cost))
		if err != nil || got != test.want {
			t.Fatalf(
				"%s: got %s, err %v",
				test.id, got, err,
			)
		}
	}

	active, queued, used := controller.Snapshot()
	if active != 2 || queued != 2 || used != 10 {
		t.Fatalf(
			"unexpected snapshot: %d %d %d",
			active, queued, used,
		)
	}

	promoted, err := controller.Finish("a")
	if err != nil {
		t.Fatal(err)
	}

	if len(promoted) != 1 || promoted[0].ID != "controller" {
		t.Fatalf("unexpected promoted operations: %+v", promoted)
	}

	active, queued, used = controller.Snapshot()

	if active != 2 || queued != 1 || used != 7 {
		t.Fatalf(
			"unexpected snapshot: %d %d %d",
			active, queued, used,
		)
	}
}

func TestConcurrentAdmission(t *testing.T) {
	controller, err := New(core.Limits{
		MaxConcurrent: 5,
		MaxCapacity:   5,
		MaxQueueSize:  10,
	})

	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup

	for i := range 100 {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			_, err := controller.Submit(testOperation(fmt.Sprint(i), 1))
			if err != nil {
				t.Error(err)
			}
		}(i)
	}

	wg.Wait()

	active, queued, used := controller.Snapshot()
	if active != 5 || queued != 10 || used != 5 {
		t.Fatalf(
			"unexpected snapshot: %d %d %d",
			active, queued, used,
		)
	}
}

func TestStarvationGuardAdmission(t *testing.T) {
	guard, err := scheduler.NewStarvationGuard(scheduler.CapacityFit{}, 2)

	if err != nil {
		t.Fatal(err)
	}

	controller, err := NewWithSelector(core.Limits{
		MaxConcurrent: 4,
		MaxCapacity:   10,
		MaxQueueSize:  5,
	}, guard)

	if err != nil {
		t.Fatal(err)
	}

	operations := []struct {
		id   string
		cost uint64
		want Decision
	}{
		{"a", 6, Started},
		{"b", 4, Started},
		{"big", 8, Queued},
		{"small1", 1, Queued},
		{"small2", 2, Queued},
		{"small3", 1, Queued},
	}

	for _, item := range operations {
		operation := core.Operation{
			ID:       item.id,
			ClientID: "client-1",
			Type:     "compute",
			Cost:     item.cost,
		}

		decision, err := controller.Submit(operation)
		if err != nil || decision != item.want {
			t.Fatalf(
				"Submit(%s): decision=%s, err=%v",
				item.id, decision, err,
			)
		}
	}

	promoted, err := controller.Finish("b")
	if err != nil {
		t.Fatal(err)
	}

	if len(promoted) != 2 || promoted[0].ID != "small1" || promoted[1].ID != "small2" {
		t.Fatalf("unexpected promotions: %+v", promoted)
	}

	active, queued, used := controller.Snapshot()
	if active != 3 || queued != 2 || used != 9 {
		t.Fatalf(
			"snapshot: active=%d queued=%d used=%d",
			active, queued, used,
		)
	}

	promoted, err = controller.Finish("a")
	if err != nil || len(promoted) != 0 {
		t.Fatalf(
			"unexpected promotion before big fits: %+v, %v",
			promoted, err,
		)
	}

	promoted, err = controller.Finish("small1")
	if err != nil {
		t.Fatal(err)
	}

	if len(promoted) != 1 || promoted[0].ID != "big" {
		t.Fatalf("expected big promotion: %+v", promoted)
	}

	active, queued, used = controller.Snapshot()
	if active != 2 || queued != 1 || used != 10 {
		t.Fatalf(
			"final snapshot: active=%d queued=%d used=%d",
			active, queued, used,
		)
	}
}

func TestSaturationAndConcurrentFinish(t *testing.T) {
	const total = 50

	controller, err := New(core.Limits{
		MaxConcurrent: 5,
		MaxCapacity:   5,
		MaxQueueSize:  total - 5,
	})

	if err != nil {
		t.Fatal(err)
	}

	for i := range total {
		id := fmt.Sprintf("op-%d", i)

		decision, err := controller.Submit(testOperation(id, 1))
		if err != nil {
			t.Fatal(err)
		}

		want := Queued
		if i < 5 {
			want = Started
		}

		if decision != want {
			t.Fatalf(
				"%s: got %s, want %s",
				id, decision, want,
			)
		}
	}

	active, queued, used := controller.Snapshot()
	if active != 5 || queued != 45 || used != 5 {
		t.Fatalf(
			"initial snapshot: %d %d %d",
			active, queued, used,
		)
	}

	ids := []string{
		"op-0",
		"op-1",
		"op-2",
		"op-3",
		"op-4",
	}

	seen := make(map[string]bool)

	for len(ids) > 0 {
		type result struct {
			id       string
			promoted []core.Operation
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
		if active > 5 || queued > 45 || used > 5 || used != uint64(active) {
			t.Fatalf(
				"invalid snapshot: %d %d %d",
				active, queued, used,
			)
		}
	}

	if len(seen) != total {
		t.Fatalf(
			"finished=%d, want=%d",
			len(seen), total,
		)
	}

	active, queued, used = controller.Snapshot()
	if active != 0 || queued != 0 || used != 0 {
		t.Fatalf(
			"final snapshot: %d %d %d",
			active, queued, used,
		)
	}
}

func TestFIFOHeadDoesNotFit(t *testing.T) {
	controller, err := New(core.Limits{
		MaxConcurrent: 3,
		MaxCapacity:   10,
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
		decision, err := controller.Submit(testOperation(test.id, test.cost))
		if err != nil || decision != test.want {
			t.Fatalf(
				"%s: got %s, err %v",
				test.id, decision, err,
			)
		}
	}

	promoted, err := controller.Finish("b")
	if err != nil || len(promoted) != 0 {
		t.Fatalf(
			"expected FIFO blocking: %v %v",
			promoted, err,
		)
	}

	active, queued, used := controller.Snapshot()
	if active != 1 || queued != 2 || used != 6 {
		t.Fatalf(
			"unexpected snapshot: %d %d %d",
			active, queued, used,
		)
	}

	promoted, err = controller.Finish("a")
	if err != nil || len(promoted) != 2 || promoted[0].ID != "big" || promoted[1].ID != "small" {
		t.Fatalf(
			"FIFO promotion: %v %v",
			promoted, err,
		)
	}
}

func TestCompareSchedulers(t *testing.T) {
	starvation, err := scheduler.NewStarvationGuard(scheduler.CapacityFit{}, 2)

	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name       string
		selector   scheduler.Selector
		wantIDs    []string
		wantActive int
		wantQueued int
		wantUsed   uint64
	}{
		{
			name:       "FIFO",
			selector:   scheduler.FIFO{},
			wantIDs:    nil,
			wantActive: 1,
			wantQueued: 4,
			wantUsed:   6,
		},
		{
			name:       "CapacityFit",
			selector:   scheduler.CapacityFit{},
			wantIDs:    []string{"small1", "small2", "small3"},
			wantActive: 4,
			wantQueued: 1,
			wantUsed:   10,
		},
		{
			name:       "CapacityFitGuarded",
			selector:   starvation,
			wantIDs:    []string{"small1", "small2"},
			wantActive: 3,
			wantQueued: 2,
			wantUsed:   9,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			controller, err := NewWithSelector(
				core.Limits{
					MaxConcurrent: 4,
					MaxCapacity:   10,
					MaxQueueSize:  4,
				},
				test.selector,
			)

			if err != nil {
				t.Fatal(err)
			}

			requests := []struct {
				id   string
				cost uint64
				want Decision
			}{
				{"a", 6, Started},
				{"b", 4, Started},
				{"big", 8, Queued},
				{"small1", 1, Queued},
				{"small2", 2, Queued},
				{"small3", 1, Queued},
			}

			for _, request := range requests {
				operation := core.Operation{
					ID:       request.id,
					ClientID: "client-1",
					Type:     "compute",
					Cost:     request.cost,
				}

				decision, err := controller.Submit(operation)
				if err != nil || decision != request.want {
					t.Fatalf(
						"Submit(%s) = (%s, %v), want %s",
						request.id, decision, err, request.want,
					)
				}
			}

			promoted, err := controller.Finish("b")
			if err != nil {
				t.Fatal(err)
			}

			if len(promoted) != len(test.wantIDs) {
				t.Fatalf(
					"promoted = %d, want %d",
					len(promoted), len(test.wantIDs),
				)
			}

			for i, operation := range promoted {
				if operation.ID != test.wantIDs[i] {
					t.Errorf(
						"promoted[%d] = %s, want %s",
						i, operation.ID, test.wantIDs[i],
					)
				}
			}

			active, queued, used := controller.Snapshot()
			if active != test.wantActive || queued != test.wantQueued || used != test.wantUsed {
				t.Fatalf(
					"snapshot = (%d, %d, %d), want (%d, %d, %d)",
					active, queued, used, test.wantActive, test.wantQueued, test.wantUsed,
				)
			}

			if active > 4 || queued > 4 || used > 10 {
				t.Fatal("capacity limit violated")
			}
		})
	}
}

func TestControllerConstruction(t *testing.T) {
	valid := core.Limits{MaxConcurrent: 1, MaxCapacity: 3, MaxQueueSize: 1}
	if _, err := New(core.Limits{}); err == nil {
		t.Fatal("expected invalid limits")
	}
	if _, err := NewWithSelector(valid, nil); err == nil {
		t.Fatal("expected nil selector error")
	}
	if _, err := NewWithStarvationGuard(valid, 0); err == nil {
		t.Fatal("expected invalid bypass limit")
	}
	if _, err := NewWithStarvationGuard(valid, 2); err != nil {
		t.Fatal(err)
	}
}

func TestControllerInvalidOperations(t *testing.T) {
	controller, err := New(core.Limits{MaxConcurrent: 1, MaxCapacity: 3, MaxQueueSize: 2})
	if err != nil {
		t.Fatal(err)
	}

	for _, operation := range []core.Operation{
		{
			ID:       "",
			ClientID: "client",
			Type:     "compute",
			Cost:     1,
		},
		testOperation("bad", 0),
		testOperation("oversized", 4),
	} {
		decision, err := controller.Submit(operation)
		if decision != Rejected || err == nil {
			t.Fatalf(
				"Submit(%q) = %s, %v",
				operation.ID, decision, err,
			)
		}
	}
	assertSnapshot(t, controller, 0, 0, 0)
}

func TestCancelPendingAndReuse(t *testing.T) {
	controller, err := New(
		core.Limits{
			MaxConcurrent: 1,
			MaxCapacity:   3,
			MaxQueueSize:  3,
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	submit := func(operation core.Operation, expected Decision) {
		t.Helper()
		got, err := controller.Submit(operation)
		if err != nil || got != expected {
			t.Fatalf(
				"Submit(%s) = %s, %v; want %s",
				operation.ID, got, err, expected,
			)
		}
	}
	submit(testOperation("a", 3), Started)
	submit(testOperation("b", 1), Queued)
	submit(testOperation("c", 1), Queued)

	cancelled := controller.CancelPending()
	if len(cancelled) != 2 || cancelled[0].ID != "b" || cancelled[1].ID != "c" {
		t.Fatalf("cancelled = %+v", cancelled)
	}

	assertSnapshot(t, controller, 1, 0, 3)

	if got := controller.CancelPending(); got != nil {
		t.Fatalf("empty cancellation = %+v", got)
	}

	submit(testOperation("b", 1), Queued)

	if err := controller.FinishWithoutPromotion("a"); err != nil {
		t.Fatal(err)
	}

	assertSnapshot(t, controller, 0, 1, 0)

	if err := controller.FinishWithoutPromotion("a"); !errors.Is(err, ErrNotActive) {
		t.Fatalf("second FinishWithoutPromotion: %v", err)
	}

	submit(testOperation("a", 1), Queued)
	assertSnapshot(t, controller, 0, 2, 0)

	if got := controller.CancelPending(); len(got) != 2 {
		t.Fatalf("cancelled = %+v", got)
	}

	submit(testOperation("a", 1), Started)
}

func TestPromotionErrorPreservesProgress(t *testing.T) {
	selector := &brokenSelector{}
	controller, err := NewWithSelector(
		core.Limits{
			MaxConcurrent: 3,
			MaxCapacity:   4,
			MaxQueueSize:  3,
		},
		selector,
	)

	if err != nil {
		t.Fatal(err)
	}

	for _, operation := range []core.Operation{testOperation("a", 4), testOperation("b", 3), testOperation("c", 1)} {
		decision, err := controller.Submit(operation)
		if err != nil || decision == Rejected {
			t.Fatalf(
				"Submit(%s) = %s, %v",
				operation.ID, decision, err,
			)
		}
	}

	promoted, err := controller.Finish("a")

	if err == nil || !strings.Contains(err.Error(), "invalid index") {
		t.Fatalf("expected invalid index: %v", err)
	}

	if len(promoted) != 1 || promoted[0].ID != "c" {
		t.Fatalf("partial promotions = %+v", promoted)
	}
	assertSnapshot(t, controller, 1, 1, 1)
}
