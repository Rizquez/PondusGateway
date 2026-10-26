package scheduler

import (
	"pondus-gateway/internal/core"
	"reflect"
	"testing"
)

func TestCapacityFit(t *testing.T) {
	tests := []struct {
		name      string
		pending   []core.Operation
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
			name: "no operation fits",
			pending: []core.Operation{
				{ID: "a", Cost: 9},
				{ID: "b", Cost: 8},
			},
			available: 3,
			want:      -1,
		},
		{
			name: "first operation fits",
			pending: []core.Operation{
				{ID: "a", Cost: 2},
				{ID: "b", Cost: 1},
			},
			available: 3,
			want:      0,
		},
		{
			name: "later operation fits",
			pending: []core.Operation{
				{ID: "a", Cost: 12},
				{ID: "b", Cost: 5},
				{ID: "c", Cost: 3},
			},
			available: 8,
			want:      1,
		},
		{
			name: "exact capacity",
			pending: []core.Operation{
				{ID: "a", Cost: 12},
				{ID: "b", Cost: 8},
			},
			available: 8,
			want:      1,
		},
		{
			name: "zero available",
			pending: []core.Operation{
				{ID: "a", Cost: 1},
			},
			available: 0,
			want:      -1,
		},
	}

	selector := CapacityFit{}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			before := append(
				[]core.Operation(nil),
				test.pending...,
			)

			got := selector.Next(
				test.pending,
				test.available,
			)

			if got != test.want {
				t.Fatalf(
					"Next() = %d, want %d",
					got, test.want,
				)
			}

			if !reflect.DeepEqual(test.pending, before) {
				t.Fatal("CapacityFit modified pending operations")
			}
		})
	}
}
