package admission

import (
	"errors"
	"fmt"
	"service-gateway/internal/operation"
	"service-gateway/internal/settings"
	"sync"
	"testing"
)

func testOperation(id string, cost uint64) operation.Operation {
	return operation.Operation{
		ID:       id,
		ClientID: "client-1",
		Type:     "compute",
		Cost:     cost,
	}
}

func TestAdmission(t *testing.T) {
	controller, err := New(settings.Limits{
		MaxConcurrent: 2,
		MaxBudget:     10,
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
			t.Fatalf("%s: got %s, err %v", test.id, got, err)
		}
	}

	active, queued, used := controller.Snapshot()

	if active != 2 || queued != 2 || used != 10 {
		t.Fatalf("unexpected snapshot: %d %d %d",
			active, queued, used)
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
		t.Fatalf("unexpected snapshot: %d %d %d", active, queued, used)
	}
}

func TestAdmissionErrors(t *testing.T) {
	controller, err := New(settings.Limits{
		MaxConcurrent: 1,
		MaxBudget:     3,
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
	controller, err := New(settings.Limits{
		MaxConcurrent: 5,
		MaxBudget:     5,
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
		t.Fatalf("unexpected snapshot: %d %d %d", active, queued, used)
	}
}
