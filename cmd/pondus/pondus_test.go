package main

import (
	"bytes"
	"fmt"
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

const multiClientYAML = `
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

func writeTestConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadConfig(t *testing.T) {
	file, err := loadConfig(writeTestConfig(t, validYAML))
	if err != nil {
		t.Fatal(err)
	}
	if file.Limits.MaxConcurrent != 2 || file.Limits.MaxCapacity != 2 || file.Operations.Count != 5 || file.Scheduler.Type != "fifo" {
		t.Fatalf("unexpected file: %+v", file)
	}
}

func TestLoadConfigInvalid(t *testing.T) {
	tests := []struct {
		name, content string
	}{
		{"unknown field", strings.Replace(validYAML, "max_capacity:", "max_capcity:", 1)},
		{"invalid duration", strings.Replace(validYAML, "duration: 1ms", "duration: invalid", 1)},
		{"unsupported scheduler", strings.Replace(validYAML, "type: fifo", "type: random", 1)},
		{"too many operations", strings.Replace(validYAML, "count: 5", "count: 10", 1)},
		{"multiple documents", validYAML + "\n---\n" + validYAML},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := loadConfig(writeTestConfig(t, test.content)); err == nil {
				t.Fatal("expected configuration error")
			}
		})
	}
}

func TestInvalidClients(t *testing.T) {
	tests := []struct {
		name    string
		clients []ClientConfig
	}{
		{"duplicate client", []ClientConfig{{ID: "a", Operations: 1, Cost: 1}, {ID: "a", Operations: 1, Cost: 1}}},
		{"empty client ID", []ClientConfig{{ID: "", Operations: 1, Cost: 1}}},
		{"invalid cost", []ClientConfig{{ID: "a", Operations: 1, Cost: 0}}},
		{"cost exceeds capacity", []ClientConfig{{ID: "a", Operations: 1, Cost: 11}}},
		{"invalid operation count", []ClientConfig{{ID: "a", Operations: 0, Cost: 1}}},
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

func TestBuildBatchMultipleClients(t *testing.T) {
	var file FileConfig
	file.Clients = []ClientConfig{
		{ID: "a", Operations: 2, Cost: 8},
		{ID: "b", Operations: 3, Cost: 2},
		{ID: "c", Operations: 1, Cost: 1},
	}
	batch := buildBatch(file)
	wantClients := []string{"a", "b", "c", "a", "b", "b"}
	wantCosts := []uint64{8, 2, 1, 8, 2, 2}

	if len(batch) != len(wantClients) {
		t.Fatalf("operations=%d, want %d", len(batch), len(wantClients))
	}

	for i, operation := range batch {
		if operation.ID != fmt.Sprintf("op-%d", i+1) || operation.ClientID != wantClients[i] || operation.Cost != wantCosts[i] {
			t.Fatalf("operation %d: %+v", i, operation)
		}
	}
}

func TestRun(t *testing.T) {
	var output bytes.Buffer
	if err := run([]string{"-concurrency=2", "-capacity=2", "-operations=4", "-duration=1ms"}, &output); err != nil {
		t.Fatal(err)
	}

	text := output.String()
	for i := 1; i <= 4; i++ {
		id := fmt.Sprintf("op-%d |", i)
		if !strings.Contains(text, id) {
			t.Errorf("missing operation %s", id)
		}
	}

	for _, expected := range []string{"state=completed", "Completed: 4", "Average reserved utilization"} {
		if !strings.Contains(text, expected) {
			t.Errorf("missing %q in output:\n%s", expected, text)
		}
	}

	if got := strings.Count(text, "state=completed"); got != 4 {
		t.Fatalf("completed=%d, want 4\n%s", got, text)
	}
}

func TestRunWithConfig(t *testing.T) {
	path := writeTestConfig(t, validYAML)
	var output bytes.Buffer

	if err := run([]string{"-client", path}, &output); err != nil {
		t.Fatal(err)
	}

	if got := strings.Count(output.String(), "state=completed"); got != 5 {
		t.Fatalf("completed=%d, want 5\n%s", got, output.String())
	}
}

func TestRunWithSchedulerStrategies(t *testing.T) {
	tests := []struct{ name, scheduler string }{
		{"fifo", "type: fifo"},
		{"capacity fit", "type: capacity-fit"},
		{"guarded", "type: capacity-fit-guarded\n  max_bypasses: 2"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			yaml := strings.Replace(validYAML, "type: fifo", test.scheduler, 1)
			var output bytes.Buffer

			if err := runConfigured(writeTestConfig(t, yaml), &output); err != nil {
				t.Fatal(err)
			}

			if got := strings.Count(output.String(), "state=completed"); got != 5 {
				t.Fatalf("completed=%d, want 5\n%s", got, output.String())
			}
		})
	}
}

func TestRunMultipleClients(t *testing.T) {
	var output bytes.Buffer
	if err := runConfigured(writeTestConfig(t, multiClientYAML), &output); err != nil {
		t.Fatal(err)
	}

	if got := strings.Count(output.String(), "state=completed"); got != 6 {
		t.Fatalf("completed=%d, want 6\n%s", got, output.String())
	}

	for _, client := range []string{"a", "b", "c"} {
		if got := strings.Count(output.String(), "client="+client+" "); got != 2 {
			t.Errorf("client %s: operations=%d, want 2", client, got)
		}
	}
}

func TestRunInvalidArguments(t *testing.T) {
	path := writeTestConfig(t, validYAML)
	tests := []struct {
		name string
		args []string
	}{
		{"invalid concurrency", []string{"-concurrency=0"}},
		{"invalid capacity", []string{"-capacity=0"}},
		{"negative operations", []string{"-operations=-1"}},
		{"invalid duration", []string{"-duration=0s"}},
		{"unknown flag", []string{"-unknown=1"}},
		{"unexpected argument", []string{"extra"}},
		{"mixed config flags", []string{"-client", path, "-operations", "10"}},
		{"compare without config", []string{"-compare"}},
		{"mixed comparison flags", []string{"-client", path, "-compare", "-operations", "5"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			if err := run(test.args, &output); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestRunComparison(t *testing.T) {
	var output bytes.Buffer
	if err := run([]string{"-client", writeTestConfig(t, validYAML), "-compare"}, &output); err != nil {
		t.Fatal(err)
	}

	text := output.String()
	if strings.Count(text, "Scheduler comparison") != 1 {
		t.Fatalf("missing comparison header:\n%s", text)
	}

	for _, strategy := range []string{"fifo", "capacity-fit", "capacity-fit-guarded"} {
		rows := 0
		foundResults := false
		for line := range strings.SplitSeq(text, "\n") {
			if !strings.HasPrefix(line, strategy+" ") {
				continue
			}

			rows++
			fields := strings.Fields(line)

			if len(fields) == 5 {
				foundResults = true
				if fields[1] != "5" || fields[2] != "0" || fields[3] != "0" {
					t.Errorf("unexpected metrics for %s: %s", strategy, line)
				}
			}
		}

		if rows != 2 || !foundResults {
			t.Errorf("strategy %s: rows=%d, results=%v", strategy, rows, foundResults)
		}
	}
}
