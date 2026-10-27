package main

import (
	"bytes"
	"strings"
	"testing"
)

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

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var output bytes.Buffer

			if err := run(tt.args, &output); err == nil {
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

	if !strings.Contains(
		text,
		"Average reserved utilization",
	) {
		t.Fatalf("missing utilization metric:\n%s", text)
	}
}
