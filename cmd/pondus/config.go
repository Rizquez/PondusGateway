package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"pondus-gateway/internal/admission"
	"pondus-gateway/internal/core"
	"pondus-gateway/internal/execution"
	"pondus-gateway/internal/scheduler"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

type ClientConfig struct {
	ID         string `yaml:"id"`
	Operations int    `yaml:"operations"`
	Cost       uint64 `yaml:"cost"`
}

// FileConfig defines the configuration of a simulation using YAML.
//
// The workload must be specified using operations.count or a list
// of clients, but never both.
type FileConfig struct {
	Limits struct {
		MaxConcurrent int    `yaml:"max_concurrent"`
		MaxCapacity   uint64 `yaml:"max_capacity"`
		MaxQueueSize  int    `yaml:"max_queue_size"`
	} `yaml:"limits"`

	Operations struct {
		Count int `yaml:"count"`
	} `yaml:"operations"`

	Executor struct {
		Type     string `yaml:"type"`
		Duration string `yaml:"duration"`
	} `yaml:"executor"`

	Scheduler struct {
		Type        string `yaml:"type"`
		MaxBypasses uint64 `yaml:"max_bypasses"`
	} `yaml:"scheduler"`

	Clients []ClientConfig `yaml:"clients"`
}

// loadConfig loads and validates a single YAML document.
//
// It rejects unknown fields and additional documents.
func loadConfig(path string) (FileConfig, error) {
	var file FileConfig

	data, err := os.ReadFile(path)
	if err != nil {
		return file, fmt.Errorf("read config %q: %w", path, err)
	}

	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)

	if err := decoder.Decode(&file); err != nil {
		return file, fmt.Errorf("decode config: %w", err)
	}

	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return file, errors.New("config must contain exactly one YAML document")
		}

		return file, fmt.Errorf("decode trailing YAML: %w", err)
	}

	if err := file.validate(); err != nil {
		return file, err
	}

	return file, nil
}

// validate checks the configuration constraints of a simulation.
//
// It supports only the sleep executor and requires that max_bypasses
// be positive exclusively with the capacity-fit-guarded strategy.
func (file FileConfig) validate() error {
	limits := file.limits()

	if err := limits.Validate(); err != nil {
		return fmt.Errorf("invalid limits: %w", err)
	}

	if err := file.validateWorkload(); err != nil {
		return err
	}

	maxStarted := file.Limits.MaxConcurrent

	if file.Limits.MaxCapacity < uint64(maxStarted) {
		maxStarted = int(file.Limits.MaxCapacity)
	}

	if file.Operations.Count > maxStarted && file.Operations.Count-maxStarted > file.Limits.MaxQueueSize {
		return errors.New("operations.count exceeds admission capacity")
	}

	if file.Executor.Type != "sleep" {
		return fmt.Errorf("unsupported executor type %q", file.Executor.Type)
	}

	duration, err := time.ParseDuration(file.Executor.Duration)
	if err != nil || duration <= 0 {
		return errors.New("executor.duration must be a positive Go duration")
	}

	switch file.Scheduler.Type {
	case "fifo", "capacity-fit":
		if file.Scheduler.MaxBypasses != 0 {
			return errors.New("max_bypasses requires capacity-fit-guarded")
		}

	case "capacity-fit-guarded":
		if file.Scheduler.MaxBypasses == 0 {
			return errors.New("scheduler.max_bypasses must be greater than zero")
		}

	default:
		return fmt.Errorf("unsupported scheduler type %q", file.Scheduler.Type)
	}

	return nil
}

// validateWorkload requires a single workload type.
//
// Client IDs are checked without normalization, so
// leading spaces are taken into account when detecting
// duplicates.
func (file FileConfig) validateWorkload() error {
	hasClients := len(file.Clients) > 0
	hasOperations := file.Operations.Count > 0

	if hasClients == hasOperations {
		return errors.New("configure either operations.count or clients")
	}

	if file.Operations.Count < 0 {
		return errors.New("operations.count cannot be negative")
	}

	if hasClients {
		known := make(map[string]struct{}, len(file.Clients))

		for _, client := range file.Clients {
			if strings.TrimSpace(client.ID) == "" {
				return errors.New("client ID cannot be empty")
			}

			if _, exists := known[client.ID]; exists {
				return fmt.Errorf("duplicate client ID: %s", client.ID)
			}

			known[client.ID] = struct{}{}

			if client.Operations <= 0 {
				return fmt.Errorf("client %s must have at least one operation", client.ID)
			}

			if client.Cost == 0 || client.Cost > file.Limits.MaxCapacity {
				return fmt.Errorf("invalid cost for client %s", client.ID)
			}
		}
	}

	return nil
}

func buildSelector(file FileConfig) (scheduler.Selector, error) {
	switch file.Scheduler.Type {
	case "fifo":
		return scheduler.FIFO{}, nil

	case "capacity-fit":
		return scheduler.CapacityFit{}, nil

	case "capacity-fit-guarded":
		return scheduler.NewStarvationGuard(
			scheduler.CapacityFit{},
			file.Scheduler.MaxBypasses,
		)

	default:
		return nil, fmt.Errorf("unsupported scheduler type %q", file.Scheduler.Type)
	}
}

func runConfigured(path string, output io.Writer) error {
	file, err := loadConfig(path)
	if err != nil {
		return err
	}

	batch := buildBatch(file)
	limits := file.limits()
	controller, runner, err := newConfiguredRunner(file)

	if err != nil {
		return err
	}

	if err := validateBatchAdmission(limits, batch); err != nil {
		return err
	}

	fmt.Fprintf(
		output,
		"Pondus Gateway | scheduler=%s operations=%d\n",
		file.Scheduler.Type, len(batch),
	)

	results, report, err := measureRun(
		context.Background(),
		runner,
		controller,
		batch,
		limits.MaxCapacity,
	)

	printReport(output, report)

	if err != nil {
		return err
	}

	for index, result := range results {
		operation := batch[index]
		fmt.Fprintf(output, "%s | client=%s | cost=%d | admission=%s | state=%s | error=%v\n",
			result.OperationID, operation.ClientID, operation.Cost, result.Decision, result.State, result.Err,
		)
	}
	return nil
}

func (file FileConfig) limits() core.Limits {
	return core.Limits{
		MaxConcurrent: file.Limits.MaxConcurrent,
		MaxCapacity:   file.Limits.MaxCapacity,
		MaxQueueSize:  file.Limits.MaxQueueSize,
	}
}

// newConfiguredRunner assumes a previously validated configuration.
//
// Each invocation creates a separate controller and runner.
func newConfiguredRunner(file FileConfig) (*admission.Controller, *execution.Runner, error) {
	selector, err := buildSelector(file)
	if err != nil {
		return nil, nil, err
	}

	controller, err := admission.NewWithSelector(file.limits(), selector)
	if err != nil {
		return nil, nil, err
	}

	duration, err := time.ParseDuration(file.Executor.Duration)
	if err != nil {
		return nil, nil, err
	}

	runner, err := execution.New(
		controller,
		execution.SleepExecutor{
			Duration: duration,
		},
	)

	if err != nil {
		return nil, nil, err
	}

	return controller, runner, nil
}

// buildBatch generates operations based on the configured mode.
//
// If no clients are defined, it uses a default client and unit cost.
//
// Assumes a valid configuration.
func buildBatch(file FileConfig) []core.Operation {
	if len(file.Clients) == 0 {
		batch := make(
			[]core.Operation,
			0,
			file.Operations.Count,
		)

		for i := 0; i < file.Operations.Count; i++ {
			batch = append(batch, core.Operation{
				ID:       fmt.Sprintf("op-%d", i+1),
				ClientID: "cli-client",
				Type:     "sleep",
				Cost:     1,
			})
		}

		return batch
	}

	return buildClientBatch(file.Clients)
}

// buildClientBatch interleaves client operations in rounds,
// generating one request per client on each iteration.
//
// The distribution determines the order of admission, not
// the order of execution.
func buildClientBatch(clients []ClientConfig) []core.Operation {
	batch := make([]core.Operation, 0)
	remaining := make([]int, len(clients))

	for index, client := range clients {
		remaining[index] = client.Operations
	}

	for {
		added := false
		for index, client := range clients {
			if remaining[index] == 0 {
				continue
			}

			batch = append(batch, core.Operation{
				ID:       fmt.Sprintf("op-%d", len(batch)+1),
				ClientID: client.ID,
				Type:     "sleep",
				Cost:     client.Cost,
			})

			remaining[index]--
			added = true
		}

		if !added {
			break
		}
	}

	return batch
}

// validateBatchAdmission checks that all operations can be admitted
// initially within the specified limits, using an isolated controller.
//
// It does not execute operations or simulate subsequent capacity releases.
func validateBatchAdmission(limits core.Limits, batch []core.Operation) error {
	controller, err := admission.New(limits)
	if err != nil {
		return err
	}

	for _, operation := range batch {
		decision, err := controller.Submit(operation)

		if err != nil {
			return fmt.Errorf(
				"admit operation %s: %w",
				operation.ID, err,
			)
		}

		if decision == admission.Rejected {
			return fmt.Errorf("operation %s rejected by admission limits", operation.ID)
		}
	}

	return nil
}
