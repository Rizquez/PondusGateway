package admission

import (
	"errors"
	"fmt"
	"pondus-gateway/internal/core"
	"pondus-gateway/internal/scheduler"
	"sync"
	"testing"
)

func testOperation(id string, cost uint64) core.Operation {
	return core.Operation{
		ID:       id,
		ClientID: "client-1",
		Type:     "compute",
		Cost:     cost,
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
		t.Fatalf(
			"unexpected promoted operations: %+v",
			promoted,
		)
	}

	active, queued, used = controller.Snapshot()

	if active != 2 || queued != 1 || used != 7 {
		t.Fatalf(
			"unexpected snapshot: %d %d %d",
			active, queued, used,
		)
	}
}

func TestAdmissionErrors(t *testing.T) {
	controller, err := New(core.Limits{
		MaxConcurrent: 1,
		MaxCapacity:   3,
		MaxQueueSize:  0,
	})

	if err != nil {
		t.Fatal(err)
	}

	if got, _ := controller.Submit(testOperation("a", 3)); got != Started {
		t.Fatal(got)
	}

	if _, err := controller.Submit(testOperation("a", 1)); !errors.Is(err, ErrDuplicate) {
		t.Fatal(err)
	}

	if got, _ := controller.Submit(testOperation("b", 1)); got != Rejected {
		t.Fatal(got)
	}

	if _, err := controller.Finish("b"); !errors.Is(err, ErrNotActive) {
		t.Fatal(err)
	}

	if _, err := controller.Finish("a"); err != nil {
		t.Fatal(err)
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

func TestCapacityFitAdmission(t *testing.T) {
	controller, err := NewWithSelector(core.Limits{
		MaxConcurrent: 3,
		MaxCapacity:   10,
		MaxQueueSize:  4,
	}, scheduler.CapacityFit{})

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
		{"tiny", 2, Queued},
	}

	for _, test := range tests {
		operation := core.Operation{
			ID:       test.id,
			ClientID: "client-1",
			Type:     "compute",
			Cost:     test.cost,
		}

		decision, err := controller.Submit(operation)
		if err != nil || decision != test.want {
			t.Fatalf(
				"%s: got %s, err=%v, want=%s",
				test.id, decision, err, test.want,
			)
		}
	}

	// When B is released, 4 units remain available
	// Big needs 8, so CapacityFit skips it
	promoted, err := controller.Finish("b")
	if err != nil {
		t.Fatal(err)
	}

	if len(promoted) != 2 {
		t.Fatalf(
			"promoted %d operations, want 2",
			len(promoted),
		)
	}

	if promoted[0].ID != "small" || promoted[1].ID != "tiny" {
		t.Fatalf(
			"unexpected promotion order: %+v",
			promoted,
		)
	}

	active, queued, used := controller.Snapshot()
	if active != 3 || queued != 1 || used != 9 {
		t.Fatalf(
			"snapshot: active=%d queued=%d used=%d",
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

	// 4 units are released
	promoted, err := controller.Finish("b")
	if err != nil {
		t.Fatal(err)
	}

	// Exactly two passing maneuvers are allowed
	if len(promoted) != 2 || promoted[0].ID != "small1" || promoted[1].ID != "small2" {
		t.Fatalf(
			"unexpected promotions: %+v",
			promoted,
		)
	}

	active, queued, used := controller.Snapshot()
	if active != 3 || queued != 2 || used != 9 {
		t.Fatalf(
			"snapshot: active=%d queued=%d used=%d",
			active, queued, used,
		)
	}

	// A is released, but Big still doesn't fit
	promoted, err = controller.Finish("a")
	if err != nil || len(promoted) != 0 {
		t.Fatalf(
			"unexpected promotion before big fits: %+v, %v",
			promoted, err,
		)
	}

	// small1 is released, now big can begin
	promoted, err = controller.Finish("small1")
	if err != nil {
		t.Fatal(err)
	}

	if len(promoted) != 1 || promoted[0].ID != "big" {
		t.Fatalf(
			"expected big promotion: %+v",
			promoted,
		)
	}

	active, queued, used = controller.Snapshot()
	if active != 2 || queued != 1 || used != 10 {
		t.Fatalf(
			"final snapshot: active=%d queued=%d used=%d",
			active, queued, used,
		)
	}
}
