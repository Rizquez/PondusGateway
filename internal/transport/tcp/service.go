package tcp

import (
	"context"
	"errors"
	"fmt"
	"pondus-gateway/internal/control/admission"
	"pondus-gateway/internal/control/scheduler"
	"pondus-gateway/internal/domain"
	"pondus-gateway/internal/execution"
	"pondus-gateway/internal/execution/simulate"
	"pondus-gateway/internal/transport/protocol"
	"time"
)

var (
	ErrDurationPositive    = errors.New("sleep duration must be positive")
	ErrMaxByPassesGuarded  = errors.New("max_bypasses is only allowed with capacity-fit-guarded")
	ErrMaxByPassesPositive = errors.New("max_bypasses must be greater than zero")
	ErrBusy                = errors.New("the service is busy")
	ErrBatchRejected       = errors.New("batch initially rejected")
)

// Config defines the limits and scheduling strategy for the TCP service,
// along with the duration of the simulated operations.
type Config struct {
	Limits        domain.Limits
	Scheduler     string
	MaxBypasses   uint64
	SleepDuration time.Duration
}

// The service processes batch execution requests.
//
// It allows only one batch to be processed at a time.
type Service struct {
	config Config
	busy   chan struct{}
}

// Validate checks the service limits and the compatibility
// of the parameters with the configured planning strategy.
func (config Config) Validate() error {
	if err := config.Limits.Validate(); err != nil {
		return fmt.Errorf("limits: %w", err)
	}

	if config.SleepDuration <= 0 {
		return ErrDurationPositive
	}

	switch config.Scheduler {
	case "fifo", "capacity-fit":
		if config.MaxBypasses != 0 {
			return ErrMaxByPassesGuarded
		}
	case "capacity-fit-guarded":
		if config.MaxBypasses == 0 {
			return ErrMaxByPassesPositive
		}
	default:
		return fmt.Errorf("scheduler not accepted: %q", config.Scheduler)
	}

	return nil
}

// New creates a TCP service with validated settings
// and initialized request exclusivity control.
func New(config Config) (*Service, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	return &Service{config: config, busy: make(chan struct{}, 1)}, nil
}

// Handles, validates, and executes a batch of operations, returning
// a protocol response with the results.
//
// Rejects the entire batch if any operation is incompatible or exceeds
// the initial admission limits.
//
// Does not wait if the service is processing another batch.
func (service *Service) Handle(ctx context.Context, request protocol.Request) protocol.Response {
	if err := request.Validate(); err != nil {
		return failure(request.RequestID, protocol.CodeInvalidRequest, "invalid request")
	}

	select {
	case service.busy <- struct{}{}:
		defer func() { <-service.busy }()
	default:
		return failure(request.RequestID, protocol.CodeServerBusy, "the server is processing another batch")
	}

	if ctx.Err() != nil {
		return failure(request.RequestID, protocol.CodeExecutionError, "request canceled")
	}

	operations := make([]domain.Operation, len(request.Operations))
	for index, operation := range request.Operations {
		if operation.Type != "sleep" {
			return failure(request.RequestID, protocol.CodeInvalidOperation, "unsupported operation type")
		}

		if operation.Cost > service.config.Limits.MaxCapacity {
			return failure(request.RequestID, protocol.CodeInvalidOperation, "cost exceeding the server's capacity")
		}

		operations[index] = operation.Domain()
	}

	if err := service.validateAdmission(operations); err != nil {
		if errors.Is(err, ErrBatchRejected) {
			return failure(request.RequestID, protocol.CodeAdmissionRejected, "the lot exceeds the initial acceptance limits")
		}
		return failure(request.RequestID, protocol.CodeExecutionError, "internal error during initial admission")
	}

	selector, err := service.selector()
	if err != nil {
		return failure(request.RequestID, protocol.CodeExecutionError, "the scheduler could not be configured")
	}

	controller, err := admission.NewWithSelector(service.config.Limits, selector)
	if err != nil {
		return failure(request.RequestID, protocol.CodeExecutionError, "the driver could not be created")
	}

	runner, err := execution.New(controller, simulate.SleepExecutor{Duration: service.config.SleepDuration})
	if err != nil {
		return failure(request.RequestID, protocol.CodeExecutionError, "the executor could not be created")
	}

	results, runErr := runner.Run(ctx, operations)
	response := protocol.Response{
		RequestID: request.RequestID,
		Status:    protocol.StatusCompleted,
		Results:   make([]protocol.Result, len(results)),
	}

	for index, result := range results {
		var message *string
		if result.Err != nil {
			value := result.Err.Error()
			message = &value
		}
		response.Results[index] = protocol.Result{
			OperationID: result.OperationID,
			Decision:    string(result.Decision),
			State:       string(result.State),
			Error:       message,
		}
	}

	if runErr != nil {
		response.Status = protocol.StatusError
		response.Error = &protocol.ResponseError{
			Code:    protocol.CodeExecutionError,
			Message: "internal error during batch execution",
		}
	}

	return response
}

// failure constructs an error response for the protocol
// associated with the request identifier.
func failure(id string, code string, message string) protocol.Response {
	return protocol.Response{
		RequestID: id,
		Status:    protocol.StatusError,
		Error: &protocol.ResponseError{
			Code:    code,
			Message: message,
		},
	}
}

// validateAdmission checks whether the entire batch can be initially admitted
// using a separate controller, without executing operations or simulating subsequent
// capacity releases.
func (service *Service) validateAdmission(operations []domain.Operation) error {
	controller, err := admission.New(service.config.Limits)
	if err != nil {
		return err
	}

	for _, operation := range operations {
		decision, err := controller.Submit(operation)

		if err != nil {
			return err
		}

		if decision == admission.Rejected {
			return ErrBatchRejected
		}
	}

	return nil
}

// The selector builds the planning strategy configured
// for the service, including starvation protection.
func (service *Service) selector() (scheduler.Selector, error) {
	switch service.config.Scheduler {
	case "fifo":
		return scheduler.FIFO{}, nil
	case "capacity-fit":
		return scheduler.CapacityFit{}, nil
	case "capacity-fit-guarded":
		return scheduler.NewStarvationGuard(scheduler.CapacityFit{}, service.config.MaxBypasses)
	default:
		return nil, fmt.Errorf("unsupported scheduler")
	}
}
