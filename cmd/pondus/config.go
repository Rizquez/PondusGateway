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

func loadConfig(path string) (FileConfig, error) {
	var config FileConfig

	data, err := os.ReadFile(path)
	if err != nil {
		return config, fmt.Errorf(
			"read config %q: %w", path, err,
		)
	}

	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)

	if err := decoder.Decode(&config); err != nil {
		return config, fmt.Errorf(
			"decode config: %w", err,
		)
	}

	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return config, errors.New(
				"config must contain exactly one YAML document",
			)
		}
		return config, fmt.Errorf(
			"decode trailing YAML: %w", err,
		)
	}

	if err := config.validate(); err != nil {
		return config, err
	}

	return config, nil
}

func (config FileConfig) validate() error {
	limits := core.Limits{
		MaxConcurrent: config.Limits.MaxConcurrent,
		MaxCapacity:   config.Limits.MaxCapacity,
		MaxQueueSize:  config.Limits.MaxQueueSize,
	}

	if err := limits.Validate(); err != nil {
		return fmt.Errorf("invalid limits: %w", err)
	}

	hasClients := len(config.Clients) > 0
	hasOperations := config.Operations.Count > 0

	if hasClients == hasOperations {
		return errors.New(
			"configure either operations.count or clients",
		)
	}

	if config.Operations.Count < 0 {
		return errors.New(
			"operations.count cannot be negative",
		)
	}

	if hasClients {
		known := make(map[string]struct{}, len(config.Clients))

		for _, client := range config.Clients {
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

			if client.Cost == 0 ||
				client.Cost > config.Limits.MaxCapacity {
				return fmt.Errorf(
					"invalid cost for client %s",
					client.ID,
				)
			}
		}
	}

	maxStarted := config.Limits.MaxConcurrent

	if config.Limits.MaxCapacity < uint64(maxStarted) {
		maxStarted = int(config.Limits.MaxCapacity)
	}

	if config.Operations.Count > maxStarted &&
		config.Operations.Count-maxStarted >
			config.Limits.MaxQueueSize {

		return errors.New(
			"operations.count exceeds admission capacity",
		)
	}

	if config.Executor.Type != "sleep" {
		return fmt.Errorf(
			"unsupported executor type %q",
			config.Executor.Type,
		)
	}

	duration, err := time.ParseDuration(
		config.Executor.Duration,
	)
	if err != nil || duration <= 0 {
		return errors.New(
			"executor.duration must be a positive Go duration",
		)
	}

	switch config.Scheduler.Type {
	case "fifo", "capacity-fit":
		if config.Scheduler.MaxBypasses != 0 {
			return errors.New(
				"max_bypasses requires capacity-fit-guarded",
			)
		}

	case "capacity-fit-guarded":
		if config.Scheduler.MaxBypasses == 0 {
			return errors.New(
				"scheduler.max_bypasses must be greater than zero",
			)
		}

	default:
		return fmt.Errorf(
			"unsupported scheduler type %q",
			config.Scheduler.Type,
		)
	}

	return nil
}

func buildSelector(
	config FileConfig,
) (scheduler.Selector, error) {
	switch config.Scheduler.Type {
	case "fifo":
		return scheduler.FIFO{}, nil

	case "capacity-fit":
		return scheduler.CapacityFit{}, nil

	case "capacity-fit-guarded":
		return scheduler.NewStarvationGuard(
			scheduler.CapacityFit{},
			config.Scheduler.MaxBypasses,
		)

	default:
		return nil, fmt.Errorf(
			"unsupported scheduler type %q",
			config.Scheduler.Type,
		)
	}
}

func runConfigured(
	path string,
	output io.Writer,
) error {
	config, err := loadConfig(path)
	if err != nil {
		return err
	}

	selector, err := buildSelector(config)
	if err != nil {
		return err
	}

	controller, err := admission.NewWithSelector(
		core.Limits{
			MaxConcurrent: config.Limits.MaxConcurrent,
			MaxCapacity:   config.Limits.MaxCapacity,
			MaxQueueSize:  config.Limits.MaxQueueSize,
		},
		selector,
	)
	if err != nil {
		return err
	}

	duration, err := time.ParseDuration(
		config.Executor.Duration,
	)
	if err != nil {
		return err
	}

	runner, err := execution.New(
		controller,
		execution.SleepExecutor{
			Duration: duration,
		},
	)
	if err != nil {
		return err
	}

	batch := buildBatch(config)

	if err := validateBatchAdmission(
		core.Limits{
			MaxConcurrent: config.Limits.MaxConcurrent,
			MaxCapacity:   config.Limits.MaxCapacity,
			MaxQueueSize:  config.Limits.MaxQueueSize,
		},
		batch,
	); err != nil {
		return err
	}

	fmt.Fprintf(
		output,
		"Pondus Gateway | scheduler=%s operations=%d\n",
		config.Scheduler.Type,
		len(batch),
	)

	results, report, err := measureRun(
		context.Background(),
		runner,
		controller,
		batch,
		config.Limits.MaxCapacity,
	)

	printReport(output, report)

	if err != nil {
		return err
	}

	for i, result := range results {
		operation := batch[i]

		fmt.Fprintf(
			output,
			"%s | client=%s | cost=%d | admission=%s | state=%s | error=%v\n",
			result.OperationID,
			operation.ClientID,
			operation.Cost,
			result.Decision,
			result.State,
			result.Err,
		)
	}

	return nil
}

func buildBatch(config FileConfig) []core.Operation {
	if len(config.Clients) == 0 {
		batch := make(
			[]core.Operation,
			0,
			config.Operations.Count,
		)

		for i := 0; i < config.Operations.Count; i++ {
			batch = append(batch, core.Operation{
				ID:       fmt.Sprintf("op-%d", i+1),
				ClientID: "cli-client",
				Type:     "sleep",
				Cost:     1,
			})
		}

		return batch
	}

	batch := make([]core.Operation, 0)
	remaining := make([]int, len(config.Clients))

	for i, client := range config.Clients {
		remaining[i] = client.Operations
	}

	for {
		added := false

		for i, client := range config.Clients {
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

func validateBatchAdmission(
	limits core.Limits,
	batch []core.Operation,
) error {
	controller, err := admission.New(limits)
	if err != nil {
		return err
	}

	for _, operation := range batch {
		decision, err := controller.Submit(operation)

		if err != nil {
			return fmt.Errorf(
				"admit operation %s: %w",
				operation.ID,
				err,
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
