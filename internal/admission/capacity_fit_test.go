package admission

import (
	"pondus-gateway/internal/operation"
	"pondus-gateway/internal/scheduler"
	"pondus-gateway/internal/settings"
	"testing"
)

func TestCapacityFitAdmission(t *testing.T) {
	controller, err := NewWithSelector(settings.Limits{
		MaxConcurrent: 3,
		MaxBudget:     10,
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
		operation := operation.Operation{
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

	// When B is released, 4 units remain available.
	// Big needs 8, so CapacityFit skips it.
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
