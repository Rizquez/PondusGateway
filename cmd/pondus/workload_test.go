package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestBuildBatchMultipleClients(t *testing.T) {
	var config FileConfig

	config.Clients = []ClientConfig{
		{ID: "a", Operations: 2, Cost: 8},
		{ID: "b", Operations: 3, Cost: 2},
		{ID: "c", Operations: 1, Cost: 1},
	}

	batch := buildBatch(config)

	wantClients := []string{
		"a", "b", "c",
		"a", "b", "b",
	}

	wantCosts := []uint64{
		8, 2, 1,
		8, 2, 2,
	}

	if len(batch) != len(wantClients) {
		t.Fatalf(
			"got %d operations, want %d",
			len(batch),
			len(wantClients),
		)
	}

	for i, operation := range batch {
		if operation.ClientID != wantClients[i] ||
			operation.Cost != wantCosts[i] {
			t.Fatalf(
				"operation %d: %+v",
				i,
				operation,
			)
		}
	}
}

func TestRunMultipleClients(t *testing.T) {
	content := `
limits:
  max_concurrent: 3
  max_capacity: 10
  max_queue_size: 6

clients:
  - id: a
    operations: 2
    cost: 8
  - id: b
    operations: 2
    cost: 2
  - id: c
    operations: 2
    cost: 1

executor:
  type: sleep
  duration: 1ms

scheduler:
  type: capacity-fit
`

	path := writeTestConfig(t, content)

	var output bytes.Buffer

	if err := runConfigured(path, &output); err != nil {
		t.Fatal(err)
	}

	if got := strings.Count(
		output.String(),
		"state=completed",
	); got != 6 {
		t.Fatalf(
			"completed=%d, want 6\n%s",
			got,
			output.String(),
		)
	}

	for _, client := range []string{"a", "b", "c"} {
		if got := strings.Count(
			output.String(),
			"client="+client+" ",
		); got != 2 {
			t.Errorf(
				"client %s: got %d operations, want 2",
				client, got,
			)
		}
	}
}

func TestInvalidClients(t *testing.T) {
	tests := []struct {
		name    string
		clients []ClientConfig
	}{
		{
			name: "duplicate client",
			clients: []ClientConfig{
				{ID: "a", Operations: 1, Cost: 1},
				{ID: "a", Operations: 1, Cost: 1},
			},
		},
		{
			name: "empty client ID",
			clients: []ClientConfig{
				{ID: "", Operations: 1, Cost: 1},
			},
		},
		{
			name: "invalid cost",
			clients: []ClientConfig{
				{ID: "a", Operations: 1, Cost: 0},
			},
		},
		{
			name: "cost exceeds capacity",
			clients: []ClientConfig{
				{ID: "a", Operations: 1, Cost: 11},
			},
		},
		{
			name: "invalid operation count",
			clients: []ClientConfig{
				{ID: "a", Operations: 0, Cost: 1},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var config FileConfig

			config.Limits.MaxConcurrent = 2
			config.Limits.MaxCapacity = 10
			config.Limits.MaxQueueSize = 5
			config.Clients = tt.clients

			config.Executor.Type = "sleep"
			config.Executor.Duration = "1ms"
			config.Scheduler.Type = "fifo"

			if err := config.validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}
