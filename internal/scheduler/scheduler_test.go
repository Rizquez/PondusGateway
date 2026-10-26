package scheduler

import (
	"pondus-gateway/internal/operation"
	"testing"
)

func TestFIFO(t *testing.T) {
	tests := []struct {
		name      string
		pending   []operation.Operation
		available uint64
		want      int
	}{
		{
			name:      "empty queue",
			pending:   nil,
			available: 10,
			want:      -1,
		},
		{
			name: "first operation fits",
			pending: []operation.Operation{
				{ID: "a", Cost: 5},
				{ID: "b", Cost: 3},
			},
			available: 10,
			want:      0,
		},
		{
			name: "first operation does not fit",
			pending: []operation.Operation{
				{ID: "a", Cost: 8},
				{ID: "b", Cost: 1},
			},
			available: 4,
			want:      -1,
		},
		{
			name: "exact available capacity",
			pending: []operation.Operation{
				{ID: "a", Cost: 10},
			},
			available: 10,
			want:      0,
		},
		{
			name: "no capacity available",
			pending: []operation.Operation{
				{ID: "a", Cost: 1},
			},
			available: 0,
			want:      -1,
		},
	}

	fifo := FIFO{}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := fifo.Next(test.pending, test.available)

			if got != test.want {
				t.Fatalf(
					"Next() = %d, want %d",
					got, test.want,
				)
			}
		})
	}
}
