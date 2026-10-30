package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const validYAML = `
limits:
  max_concurrent: 2
  max_capacity: 2
  max_queue_size: 3

operations:
  count: 5

executor:
  type: sleep
  duration: 1ms

scheduler:
  type: fifo
`

func writeTestConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadConfig(t *testing.T) {
	path := writeTestConfig(t, validYAML)

	config, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}

	if config.Limits.MaxConcurrent != 2 || config.Operations.Count != 5 || config.Scheduler.Type != "fifo" {
		t.Fatalf("unexpected config: %+v", config)
	}
}

func TestLoadConfigInvalid(t *testing.T) {
	tests := []struct {
		name    string
		content string
	}{
		{
			name: "unknown field",
			content: strings.Replace(
				validYAML,
				"max_capacity:",
				"max_capcity:",
				1,
			),
		},
		{
			name: "invalid duration",
			content: strings.Replace(
				validYAML,
				"duration: 1ms",
				"duration: invalid",
				1,
			),
		},
		{
			name: "unsupported scheduler",
			content: strings.Replace(
				validYAML,
				"type: fifo",
				"type: random",
				1,
			),
		},
		{
			name: "too many operations",
			content: strings.Replace(
				validYAML,
				"count: 5",
				"count: 10",
				1,
			),
		},
		{
			name:    "multiple documents",
			content: validYAML + "\n---\n" + validYAML,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := writeTestConfig(t, test.content)

			if _, err := loadConfig(path); err == nil {
				t.Fatal("expected configuration error")
			}
		})
	}
}

func TestRunWithConfig(t *testing.T) {
	path := writeTestConfig(t, validYAML)

	var output bytes.Buffer

	if err := run([]string{"-client", path}, &output); err != nil {
		t.Fatal(err)
	}

	if got := strings.Count(output.String(), "state=completed"); got != 5 {
		t.Fatalf(
			"completed=%d, want 5\n%s",
			got, output.String(),
		)
	}
}

func TestRunRejectsMixedFlags(t *testing.T) {
	path := writeTestConfig(t, validYAML)

	var output bytes.Buffer

	err := run([]string{
		"-client", path,
		"-operations", "10",
	}, &output)

	if err == nil {
		t.Fatal("expected error for mixed flags")
	}
}

func TestRunWithSchedulerStrategies(t *testing.T) {
	tests := []struct {
		name      string
		scheduler string
	}{
		{
			name:      "fifo",
			scheduler: "type: fifo",
		},
		{
			name:      "capacity fit",
			scheduler: "type: capacity-fit",
		},
		{
			name:      "guarded",
			scheduler: "type: capacity-fit-guarded\n  max_bypasses: 2",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			content := strings.Replace(
				validYAML,
				"type: fifo",
				test.scheduler,
				1,
			)

			path := writeTestConfig(t, content)

			var output bytes.Buffer

			if err := runConfigured(path, &output); err != nil {
				t.Fatal(err)
			}

			if got := strings.Count(output.String(), "state=completed"); got != 5 {
				t.Fatalf(
					"completed=%d, want 5\n%s",
					got, output.String(),
				)
			}
		})
	}
}

func TestBuildBatchMultipleClients(t *testing.T) {
	var file FileConfig

	file.Clients = []ClientConfig{
		{ID: "a", Operations: 2, Cost: 8},
		{ID: "b", Operations: 3, Cost: 2},
		{ID: "c", Operations: 1, Cost: 1},
	}

	batch := buildBatch(file)

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
			len(batch), len(wantClients),
		)
	}

	for i, operation := range batch {
		if operation.ClientID != wantClients[i] ||
			operation.Cost != wantCosts[i] {
			t.Fatalf(
				"operation %d: %+v",
				i, operation,
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

	if got := strings.Count(output.String(), "state=completed"); got != 6 {
		t.Fatalf(
			"completed=%d, want 6\n%s",
			got, output.String(),
		)
	}

	for _, client := range []string{"a", "b", "c"} {
		if got := strings.Count(output.String(), "client="+client+" "); got != 2 {
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

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var file FileConfig

			file.Limits.MaxConcurrent = 2
			file.Limits.MaxCapacity = 10
			file.Limits.MaxQueueSize = 5
			file.Clients = test.clients

			file.Executor.Type = "sleep"
			file.Executor.Duration = "1ms"
			file.Scheduler.Type = "fifo"

			if err := file.validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestRun(t *testing.T) {
	var output bytes.Buffer

	err := run([]string{
		"-concurrency=2",
		"-capacity=2",
		"-operations=4",
		"-duration=1ms",
	}, &output)

	if err != nil {
		t.Fatal(err)
	}

	text := output.String()

	for i := 1; i <= 4; i++ {
		id := "op-" + string(rune('0'+i))

		if !strings.Contains(text, id+" |") {
			t.Errorf("missing operation %s", id)
		}
	}

	if got := strings.Count(text, "state=completed"); got != 4 {
		t.Errorf(
			"completed operations = %d, want 4\n%s",
			got, text,
		)
	}
}

func TestRunInvalidArguments(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{
			name: "invalid concurrency",
			args: []string{"-concurrency=0"},
		},
		{
			name: "invalid capacity",
			args: []string{"-capacity=0"},
		},
		{
			name: "negative operations",
			args: []string{"-operations=-1"},
		},
		{
			name: "invalid duration",
			args: []string{"-duration=0s"},
		},
		{
			name: "unknown flag",
			args: []string{"-unknown=1"},
		},
		{
			name: "unexpected argument",
			args: []string{"extra"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer

			if err := run(test.args, &output); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestRunPrintsMetrics(t *testing.T) {
	var output bytes.Buffer

	err := run([]string{
		"-concurrency=2",
		"-capacity=2",
		"-operations=4",
		"-duration=5ms",
	}, &output)

	if err != nil {
		t.Fatal(err)
	}

	text := output.String()

	if !strings.Contains(text, "Completed: 4") {
		t.Fatalf("missing completed metric:\n%s", text)
	}

	if !strings.Contains(text, "Average reserved utilization") {
		t.Fatalf("missing utilization metric:\n%s", text)
	}
}

func TestRunComparison(t *testing.T) {
	path := writeTestConfig(t, validYAML)

	var output bytes.Buffer

	err := run(
		[]string{
			"-client", path,
			"-compare",
		},
		&output,
	)
	if err != nil {
		t.Fatal(err)
	}

	text := output.String()

	for _, strategy := range []string{"fifo", "capacity-fit", "capacity-fit-guarded"} {
		if !strings.Contains(text, strategy) {
			t.Errorf(
				"missing strategy %s:\n%s",
				strategy, text,
			)
		}
	}

	if got := strings.Count(text, "Scheduler comparison"); got != 1 {
		t.Fatalf(
			"comparison header count=%d, want 1",
			got,
		)
	}

	for _, strategy := range []string{"fifo", "capacity-fit", "capacity-fit-guarded"} {
		lines := strings.Split(text, "\n")
		count := 0

		for _, line := range lines {
			if strings.HasPrefix(line, strategy+" ") {
				count++
			}
		}

		if count != 2 {
			t.Errorf(
				"%s appears in %d result rows, want 2",
				strategy, count,
			)
		}
	}
}

func TestCompareRequiresConfig(t *testing.T) {
	var output bytes.Buffer

	err := run([]string{"-compare"}, &output)

	if err == nil {
		t.Fatal("expected error without -client")
	}
}

func TestCompareRejectsMixedFlags(t *testing.T) {
	path := writeTestConfig(t, validYAML)

	var output bytes.Buffer

	err := run([]string{
		"-client", path,
		"-compare",
		"-operations", "5",
	}, &output)

	if err == nil {
		t.Fatal("expected error for mixed flags")
	}
}

func TestComparisonCompletesSameWorkload(t *testing.T) {
	path := writeTestConfig(t, validYAML)

	var output bytes.Buffer

	if err := runComparison(path, &output); err != nil {
		t.Fatal(err)
	}

	lines := strings.Split(output.String(), "\n")

	for _, strategy := range []string{"fifo", "capacity-fit", "capacity-fit-guarded"} {
		found := false

		for _, line := range lines {
			if !strings.HasPrefix(line, strategy+" ") {
				continue
			}

			fields := strings.Fields(line)

			if len(fields) != 5 {
				continue
			}

			if fields[1] != "5" || fields[2] != "0" || fields[3] != "0" {
				t.Fatalf(
					"unexpected metrics for %s: %s",
					strategy, line,
				)
			}

			found = true
		}

		if !found {
			t.Fatalf(
				"missing results for %s",
				strategy,
			)
		}
	}
}
