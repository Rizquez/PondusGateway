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

// ClientConfig defines a client's workload configuration.
type ClientConfig struct {
	ID         string `yaml:"id"`
	Operations int    `yaml:"operations"`
	Cost       uint64 `yaml:"cost"`
}

// FileConfig represents the configuration of a simulation
// loaded from a YAML file.
//
// It groups admission limits, the workload definition,
// the executor, the scheduling strategy, and the clients.
//
// The workload can be defined by a global number
// of operations or by a list of clients, but not both.
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

// loadConfig loads and validates the configuration from a YAML file.
//
// It requires that the file contain exactly one YAML document
// and validates the configuration constraints before returning it.
//
// It returns the loaded configuration or an error if reading, decoding
// or any of the validations fail.
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

// validate checks that the configuration meets the constraints required
// to run a simulation.
//
// Validates admission limits and the workload definition, including an
// admission capacity check for the mode based on a global number of operations.
//
// Supports the FIFO, capacity-fit, and capacity-fit-guarded strategies.
//
// The MaxBypasses parameter must be positive only for capacity-fit-guarded and
// zero for all other strategies.
//
// Returns an error describing the first violated constraint or nil if the
// configuration is valid.
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
		return fmt.Errorf(
			"unsupported executor type %q",
			file.Executor.Type,
		)
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
		return fmt.Errorf(
			"unsupported scheduler type %q",
			file.Scheduler.Type,
		)
	}

	return nil
}

// validateWorkload checks that the workload uses a single configuration mode.
//
// When configuring clients, it checks that their identifiers are neither empty
// nor duplicated, that each client has at least one operation, and that its cost
// is positive and does not exceed the configured maximum capacity.
//
// Returns an error upon the first violated constraint or nil if the workload is
// valid.
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
				return fmt.Errorf(
					"duplicate client ID: %s",
					client.ID,
				)
			}

			known[client.ID] = struct{}{}

			if client.Operations <= 0 {
				return fmt.Errorf(
					"client %s must have at least one operation",
					client.ID,
				)
			}

			if client.Cost == 0 || client.Cost > file.Limits.MaxCapacity {
				return fmt.Errorf(
					"invalid cost for client %s",
					client.ID,
				)
			}
		}
	}

	return nil
}

// buildSelector constructs the scheduling strategy specified in the configuration.
//
// Supports the following strategies:
//   - fifo: selects operations in first-in, first-out order.
//   - capacity-fit: uses the CapacityFit selector.
//   - capacity-fit-guarded: uses CapacityFit with protection against unlimited preemptions.
//
// Returns the corresponding selector or an error if the strategy is not supported or if
// StarvationGuard creation fails.
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
		return nil, fmt.Errorf(
			"unsupported scheduler type %q",
			file.Scheduler.Type,
		)
	}
}

// runConfigured runs a simulation defined by a YAML file.
//
// Loads and validates the configuration, generates the batch of operations,
// and creates a separate controller and executor.
//
// First checks that all operations in the batch can be accommodated within
// the configured limits.
//
// Runs the simulation, displays the metrics report and if the simulation
// completes without errors, prints the individual results along with the
// client and cost of each operation.
//
// Returns an error if any stage of configuration, validation, admission or
// execution fails, or nil if it completes successfully.
func runConfigured(path string, output io.Writer) error {
	config, err := loadConfig(path)
	if err != nil {
		return err
	}

	batch := buildBatch(config)
	limits := config.limits()
	controller, runner, err := newConfiguredRunner(config)

	if err != nil {
		return err
	}

	if err := validateBatchAdmission(limits, batch); err != nil {
		return err
	}

	fmt.Fprintf(
		output,
		"Pondus Gateway | scheduler=%s operations=%d\n",
		config.Scheduler.Type, len(batch),
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

	for i, result := range results {
		operation := batch[i]
		fmt.Fprintf(output, "%s | client=%s | cost=%d | admission=%s | state=%s | error=%v\n",
			result.OperationID, operation.ClientID, operation.Cost, result.Decision, result.State, result.Err,
		)
	}
	return nil
}

// limits converts the limits defined in FileConfig to the core.Limits
// structure used by the admission controller.
//
// Returns the concurrency, capacity, and maximum queue size values without
// modifying them or performing additional validations.
func (file FileConfig) limits() core.Limits {
	return core.Limits{
		MaxConcurrent: file.Limits.MaxConcurrent,
		MaxCapacity:   file.Limits.MaxCapacity,
		MaxQueueSize:  file.Limits.MaxQueueSize,
	}
}

// newConfiguredRunner constructs an admission controller and a Runner based
// on the provided configuration.
//
// It initializes the selected scheduler, creates a controller with its own
// registrations and capacity reservations, and configures a SleepExecutor with
// the specified duration.
//
// Returns the initialized controller and Runner or an error if the construction
// of any of its components fails.
//
// It is assumed that the configuration has been validated beforehand.
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

// buildBatch generates a batch of operations based on the configured
// workload mode.
//
// Returns the generated operations in the order in which they should
// be presented to the admission controller.
//
// It is assumed that the configuration contains valid values.
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

// buildClientBatch generates a batch of operations distributed
// among the configured clients using a round-robin approach.
//
// In each round, it includes one operation for each client that
// still has operations pending generation.
//
// Each operation receives a sequential identifier, its client’s
// identifier, the “sleep” type and the cost configured for that
// client.
//
// The process continues until all requested operations have been generated.
//
// It returns the interleaved batch, preserving the order of the clients
// within each round and without modifying the received configuration.
func buildClientBatch(clients []ClientConfig) []core.Operation {
	batch := make([]core.Operation, 0)
	remaining := make([]int, len(clients))

	for i, client := range clients {
		remaining[i] = client.Operations
	}

	for {
		added := false
		for i, client := range clients {
			if remaining[i] == 0 {
				continue
			}

			batch = append(batch, core.Operation{
				ID:       fmt.Sprintf("op-%d", len(batch)+1),
				ClientID: client.ID,
				Type:     "sleep",
				Cost:     client.Cost,
			})

			remaining[i]--
			added = true
		}

		if !added {
			break
		}
	}

	return batch
}

// validateBatchAdmission checks that all operations in a batch can be
// admitted within the provided limits.
//
// Creates a separate admission controller and submits each operation
// sequentially using Submit.
//
// Returns an error if the controller fails to be created any operation
// results in an admission error or any request receives a Rejected decision.
//
// Returns nil if all operations are admitted, regardless of whether they
// start immediately or remain in the waiting queue.
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
			return fmt.Errorf(
				"operation %s rejected by admission limits",
				operation.ID,
			)
		}
	}

	return nil
}
