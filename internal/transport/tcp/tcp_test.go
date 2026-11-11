package tcp

import (
	"context"
	"errors"
	"pondus-gateway/internal/domain"
	"pondus-gateway/internal/transport/protocol"
	"testing"
	"time"
)

func configForTest() Config {
	return Config{
		Limits: domain.Limits{
			MaxConcurrent: 1,
			MaxCapacity:   2,
			MaxQueueSize:  2,
		},
		Scheduler:     "fifo",
		SleepDuration: time.Millisecond,
	}
}

func requestForTest() protocol.Request {
	return protocol.Request{
		RequestID: "r1",
		Operations: []protocol.Operation{
			{ID: "op1", ClientID: "a", Type: "sleep", Cost: 2},
			{ID: "op2", ClientID: "b", Type: "sleep", Cost: 1},
		},
	}
}

func TestServiceExecutesBatch(t *testing.T) {
	service, err := New(configForTest())
	if err != nil {
		t.Fatal(err)
	}

	response := service.Handle(context.Background(), requestForTest())
	if response.Status != protocol.StatusCompleted || response.RequestID != "r1" || len(response.Results) != 2 {
		t.Fatalf("%+v", response)
	}

	for _, result := range response.Results {
		if result.State != "completed" || result.Error != nil {
			t.Fatalf("%+v", result)
		}
	}

	again := service.Handle(context.Background(), requestForTest())
	if again.Status != protocol.StatusCompleted {
		t.Fatalf("second execution: %+v", again)
	}
}

func TestServiceRejectsInvalidWorkloads(t *testing.T) {
	service, err := New(configForTest())
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		request protocol.Request
		code    string
	}{
		{"no operations", protocol.Request{RequestID: "empty"}, protocol.CodeInvalidRequest},
		{"unsupported type", protocol.Request{RequestID: "type", Operations: []protocol.Operation{{ID: "a", ClientID: "c", Type: "cpu", Cost: 1}}}, protocol.CodeInvalidOperation},
		{"excessive cost", protocol.Request{RequestID: "cost", Operations: []protocol.Operation{{ID: "a", ClientID: "c", Type: "sleep", Cost: 3}}}, protocol.CodeInvalidOperation},
		{"insufficient tail", protocol.Request{RequestID: "queue", Operations: []protocol.Operation{{ID: "a", ClientID: "c", Type: "sleep", Cost: 2}, {ID: "b", ClientID: "c", Type: "sleep", Cost: 1}, {ID: "c", ClientID: "c", Type: "sleep", Cost: 1}, {ID: "d", ClientID: "c", Type: "sleep", Cost: 1}}}, protocol.CodeAdmissionRejected},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := service.Handle(context.Background(), test.request)
			if response.Error == nil || response.Error.Code != test.code {
				t.Fatalf("%+v", response)
			}
		})
	}
}

func TestServiceAdmissionInternalFailureClassification(t *testing.T) {
	service, err := New(configForTest())
	if err != nil {
		t.Fatal(err)
	}

	service.config.Limits.MaxConcurrent = 0

	response := service.Handle(context.Background(), requestForTest())
	if response.Status != protocol.StatusError || response.Error == nil {
		t.Fatalf("expected a service error; got %v", response)
	}

	if response.Error.Code != protocol.CodeExecutionError {
		t.Fatalf(
			"error code = %q; want %q",
			response.Error.Code, protocol.CodeExecutionError,
		)
	}
}

func TestServiceBusyUntilRunReturns(t *testing.T) {
	cfg := configForTest()
	cfg.SleepDuration = 250 * time.Millisecond
	service, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	finished := make(chan protocol.Response, 1)
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()
	go func() {
		finished <- service.Handle(ctx, requestForTest())
	}()

	deadline := time.After(2 * time.Second)
	for len(service.busy) == 0 {
		select {
		case <-deadline:
			t.Fatal("the execution did not begin")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	response := service.Handle(context.Background(), requestForTest())
	if response.Error == nil || response.Error.Code != protocol.CodeServerBusy {
		t.Fatalf("%+v", response)
	}

	cancel()
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("the auction did not conclude after it was canceled")
	}

	response = service.Handle(context.Background(), requestForTest())
	if response.Status != protocol.StatusCompleted {
		t.Fatalf("new execution: %+v", response)
	}
}

func TestConfigValidate(t *testing.T) {
	cfg := configForTest()
	cfg.Scheduler = "capacity-fit-guarded"
	cfg.MaxBypasses = 2
	if _, err := New(cfg); err != nil {
		t.Fatal(err)
	}

	cfg.MaxBypasses = 0
	if _, err := New(cfg); err == nil {
		t.Fatal("an error was expected")
	}
}

func TestConfigGuardedInvalidBypassesError(t *testing.T) {
	cfg := configForTest()
	cfg.Scheduler = "capacity-fit-guarded"
	cfg.MaxBypasses = 0

	err := cfg.Validate()
	if !errors.Is(err, ErrMaxByPassesPositive) {
		t.Fatalf(
			"Validate() = %v; want %v",
			err, ErrMaxByPassesPositive,
		)
	}
}
