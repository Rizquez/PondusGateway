package scheduler

import (
	"pondus-gateway/internal/operation"
	"reflect"
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

func TestFIFOSelection(t *testing.T) {
	tests := []struct {
		name      string
		pending   []operation.Operation
		available uint64
		want      int
	}{
		{
			name:      "empty queue",
			available: 10,
			want:      -1,
		},
		{
			name: "no capacity",
			pending: []operation.Operation{
				{ID: "a", Cost: 1},
			},
			available: 0,
			want:      -1,
		},
		{
			name: "exact fit",
			pending: []operation.Operation{
				{ID: "a", Cost: 5},
			},
			available: 5,
			want:      0,
		},
		{
			name: "first fits",
			pending: []operation.Operation{
				{ID: "a", Cost: 3},
				{ID: "b", Cost: 1},
			},
			available: 4,
			want:      0,
		},
		{
			name: "first blocks smaller",
			pending: []operation.Operation{
				{ID: "a", Cost: 8},
				{ID: "b", Cost: 1},
			},
			available: 4,
			want:      -1,
		},
		{
			name: "large budget",
			pending: []operation.Operation{
				{ID: "a", Cost: 2},
			},
			available: 100,
			want:      0,
		},
	}

	selector := FIFO{}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := selector.Next(
				test.pending,
				test.available,
			)

			if got != test.want {
				t.Fatalf(
					"Next() = %d, want %d",
					got,
					test.want,
				)
			}
		})
	}
}

func TestFIFODoesNotModifyPending(t *testing.T) {
	pending := []operation.Operation{
		{
			ID:       "a",
			ClientID: "client-1",
			Type:     "query",
			Cost:     8,
		},
		{
			ID:       "b",
			ClientID: "client-2",
			Type:     "ping",
			Cost:     1,
		},
	}

	before := append(
		[]operation.Operation(nil),
		pending...,
	)

	selector := FIFO{}

	for _, available := range []uint64{0, 1, 8, 20} {
		selector.Next(pending, available)

		if !reflect.DeepEqual(pending, before) {
			t.Fatalf(
				"FIFO modified the queue with available=%d",
				available,
			)
		}
	}
}

func TestFIFOIsDeterministic(t *testing.T) {
	pending := []operation.Operation{
		{ID: "first", Cost: 7},
		{ID: "second", Cost: 1},
	}

	selector := FIFO{}

	for i := range 100 {
		if got := selector.Next(pending, 7); got != 0 {
			t.Fatalf(
				"iteration %d: got %d, want 0",
				i, got,
			)
		}

		if got := selector.Next(pending, 6); got != -1 {
			t.Fatalf(
				"iteration %d: got %d, want -1",
				i, got,
			)
		}
	}
}
