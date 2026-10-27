package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunComparison(t *testing.T) {
	path := writeTestConfig(t, validYAML)

	var output bytes.Buffer

	err := run(
		[]string{
			"-config", path,
			"-compare",
		},
		&output,
	)
	if err != nil {
		t.Fatal(err)
	}

	text := output.String()

	for _, strategy := range []string{
		"fifo",
		"capacity-fit",
		"capacity-fit-guarded",
	} {
		if !strings.Contains(text, strategy) {
			t.Errorf(
				"missing strategy %s:\n%s",
				strategy,
				text,
			)
		}
	}

	if got := strings.Count(
		text,
		"Scheduler comparison",
	); got != 1 {
		t.Fatalf(
			"comparison header count=%d, want 1",
			got,
		)
	}

	for _, strategy := range []string{
		"fifo",
		"capacity-fit",
		"capacity-fit-guarded",
	} {
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
		t.Fatal("expected error without -config")
	}
}

func TestCompareRejectsMixedFlags(t *testing.T) {
	path := writeTestConfig(t, validYAML)

	var output bytes.Buffer

	err := run([]string{
		"-config", path,
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

	for _, strategy := range []string{
		"fifo",
		"capacity-fit",
		"capacity-fit-guarded",
	} {
		found := false

		for _, line := range lines {
			if !strings.HasPrefix(line, strategy+" ") {
				continue
			}

			fields := strings.Fields(line)

			if len(fields) != 5 {
				continue
			}

			if fields[1] != "5" ||
				fields[2] != "0" ||
				fields[3] != "0" {
				t.Fatalf(
					"unexpected metrics for %s: %s",
					strategy,
					line,
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
