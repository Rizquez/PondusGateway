package admission

import (
	"pondus-gateway/internal/core"
	"pondus-gateway/internal/scheduler"
	"testing"
)

func TestCompareSchedulers(t *testing.T) {
	guard, err := scheduler.NewStarvationGuard(scheduler.CapacityFit{}, 2)

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
			selector:   guard,
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

			// B is completed and 4 units are released
			promoted, err := controller.Finish("b")
			if err != nil {
				t.Fatal(err)
			}

			// The system checks which transactions selected
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

			// No strategy should exceed the limits
			if active > 4 || queued > 4 || used > 10 {
				t.Fatal("capacity limit violated")
			}
		})
	}
}
