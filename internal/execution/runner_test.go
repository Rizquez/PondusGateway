package execution

import (
	"context"
	"errors"
	"pondus-gateway/internal/admission"
	"pondus-gateway/internal/core"
	"sync/atomic"
	"testing"
	"time"
)

type countingExecutor struct {
	active atomic.Int64
	max    atomic.Int64
}

func (counting *countingExecutor) Execute(ctx context.Context, _ core.Operation) error {
	now := counting.active.Add(1)
	defer counting.active.Add(-1)

	for {
		old := counting.max.Load()
		if now <= old || counting.max.CompareAndSwap(old, now) {
			break
		}
	}

	select {
	case <-time.After(time.Millisecond):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestRunnerConcurrentExecution(t *testing.T) {
	controller, err := admission.New(core.Limits{
		MaxConcurrent: 2,
		MaxCapacity:   3,
		MaxQueueSize:  5,
	})
	if err != nil {
		t.Fatal(err)
	}

	counting := &countingExecutor{}

	runner, err := New(controller, counting)
	if err != nil {
		t.Fatal(err)
	}

	var operations []core.Operation

	for i := range 7 {
		operations = append(operations, core.Operation{
			ID:       string(rune('a' + i)),
			ClientID: "client-1",
			Type:     "compute",
			Cost:     1,
		})
	}

	results, err := runner.Run(context.Background(), operations)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != len(operations) {
		t.Fatalf(
			"results=%d, want=%d",
			len(results), len(operations),
		)
	}

	for i, result := range results {
		if result.Err != nil ||
			result.Decision == admission.Rejected ||
			result.OperationID != operations[i].ID {
			t.Fatalf("unexpected result: %+v", result)
		}
	}

	if counting.max.Load() > 2 {
		t.Fatal("concurrency limit exceeded")
	}

	active, queued, used := controller.Snapshot()

	if active != 0 || queued != 0 || used != 0 {
		t.Fatalf(
			"capacity leak: active=%d queued=%d used=%d",
			active, queued, used,
		)
	}
}

func TestRunnerRejectedOperation(t *testing.T) {
	controller, err := admission.New(core.Limits{
		MaxConcurrent: 1,
		MaxCapacity:   1,
		MaxQueueSize:  0,
	})

	if err != nil {
		t.Fatal(err)
	}

	runner, err := New(controller, &countingExecutor{})
	if err != nil {
		t.Fatal(err)
	}

	operations := []core.Operation{
		{ID: "a", ClientID: "x", Type: "ping", Cost: 1},
		{ID: "b", ClientID: "x", Type: "ping", Cost: 1},
	}

	results, err := runner.Run(context.Background(), operations)
	if err != nil {
		t.Fatal(err)
	}

	if results[0].Decision != admission.Started ||
		results[1].Decision != admission.Rejected {
		t.Fatalf(
			"unexpected decisions: %+v",
			results,
		)
	}
}

func TestRunnerCancelledContext(t *testing.T) {
	controller, err := admission.New(core.Limits{
		MaxConcurrent: 1,
		MaxCapacity:   1,
		MaxQueueSize:  2,
	})

	if err != nil {
		t.Fatal(err)
	}

	runner, err := New(controller, &countingExecutor{})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	operations := []core.Operation{
		{ID: "a", ClientID: "x", Type: "ping", Cost: 1},
		{ID: "b", ClientID: "x", Type: "ping", Cost: 1},
	}

	results, err := runner.Run(ctx, operations)
	if err != nil {
		t.Fatal(err)
	}

	for _, result := range results {
		if !errors.Is(result.Err, context.Canceled) {
			t.Fatalf(
				"operation %s: got %v, want context.Canceled",
				result.OperationID, result.Err,
			)
		}
	}

	active, queued, used := controller.Snapshot()

	if active != 0 || queued != 0 || used != 0 {
		t.Fatal("capacity was not fully released")
	}
}
