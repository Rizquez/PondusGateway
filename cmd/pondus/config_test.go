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

func writeTestConfig(
	t *testing.T,
	content string,
) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "config.yaml")

	if err := os.WriteFile(
		path,
		[]byte(content),
		0600,
	); err != nil {
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

	if config.Limits.MaxConcurrent != 2 ||
		config.Operations.Count != 5 ||
		config.Scheduler.Type != "fifo" {
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

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeTestConfig(t, tt.content)

			if _, err := loadConfig(path); err == nil {
				t.Fatal("expected configuration error")
			}
		})
	}
}

func TestRunWithConfig(t *testing.T) {
	path := writeTestConfig(t, validYAML)

	var output bytes.Buffer

	if err := run(
		[]string{"-config", path},
		&output,
	); err != nil {
		t.Fatal(err)
	}

	if got := strings.Count(
		output.String(),
		"state=completed",
	); got != 5 {
		t.Fatalf(
			"completed=%d, want 5\n%s",
			got,
			output.String(),
		)
	}
}

func TestRunRejectsMixedFlags(t *testing.T) {
	path := writeTestConfig(t, validYAML)

	var output bytes.Buffer

	err := run([]string{
		"-config", path,
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

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			content := strings.Replace(
				validYAML,
				"type: fifo",
				tt.scheduler,
				1,
			)

			path := writeTestConfig(t, content)

			var output bytes.Buffer

			if err := runConfigured(path, &output); err != nil {
				t.Fatal(err)
			}

			if got := strings.Count(
				output.String(),
				"state=completed",
			); got != 5 {
				t.Fatalf(
					"completed=%d, want 5\n%s",
					got,
					output.String(),
				)
			}
		})
	}
}
