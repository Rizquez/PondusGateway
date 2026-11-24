package tcpstream

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"pondus-gateway/internal/control/admission"
	"pondus-gateway/internal/domain"
	"pondus-gateway/internal/execution/continuous"
	"pondus-gateway/internal/transport/stream"
	"sync"
	"testing"
	"time"
)

type controlledExecutor struct {
	started chan string
	mutex   sync.Mutex
	release map[string]chan struct{}
}

func newControlledExecutor() *controlledExecutor {
	return &controlledExecutor{
		started: make(chan string, 16),
		release: make(map[string]chan struct{}),
	}
}

func (executor *controlledExecutor) Execute(ctx context.Context, operation domain.Operation) error {
	executor.mutex.Lock()
	release := make(chan struct{})
	executor.release[operation.ID] = release
	executor.mutex.Unlock()

	executor.started <- operation.ID
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-release:
		return nil
	}
}

func (executor *controlledExecutor) finish(operationID string) {
	executor.mutex.Lock()
	defer executor.mutex.Unlock()
	close(executor.release[operationID])
}

func startTestServer(
	t *testing.T,
	limits domain.Limits,
	executor *controlledExecutor,
) (*Server, *admission.Controller, *continuous.Coordinator, string, func()) {
	t.Helper()

	controller, err := admission.New(limits)
	if err != nil {
		t.Fatalf("create admission controller: %v", err)
	}

	coordinator, err := continuous.New(controller, executor)
	if err != nil {
		t.Fatalf("create continuous coordinator: %v", err)
	}
	if err := coordinator.Start(); err != nil {
		t.Fatalf("start coordinator: %v", err)
	}

	server, err := New(Config{
		MaxConnections:  4,
		MaxMessageBytes: 4096,
		OutgoingQueue:   16,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    time.Second,
	}, coordinator)
	if err != nil {
		t.Fatalf("create TCP stream server: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen on TCP: %v", err)
	}

	serverContext, cancelServer := context.WithCancel(context.Background())
	serverFinished := make(chan error, 1)
	go func() {
		serverFinished <- server.Serve(serverContext, listener)
	}()

	cleanup := func() {
		cancelServer()
		_ = listener.Close()
		select {
		case serveError := <-serverFinished:
			if serveError != nil {
				t.Errorf("TCP server returned unexpected error: %v", serveError)
			}
		case <-time.After(3 * time.Second):
			t.Error("TCP server did not stop within deadline")
		}
		closeContext, cancelClose := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancelClose()
		if closeError := coordinator.Close(closeContext); closeError != nil {
			t.Errorf("coordinator did not close cleanly: %v", closeError)
		}
	}

	return server, controller, coordinator, listener.Addr().String(), cleanup
}

func connectTestClient(t *testing.T, address string) (net.Conn, *bufio.Reader) {
	t.Helper()
	connection, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		t.Fatalf("connect TCP client: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return connection, bufio.NewReader(connection)
}

func sendOperation(t *testing.T, connection net.Conn, requestID, operationID string) {
	t.Helper()
	message := stream.Message{
		Type:      stream.TypeSubmit,
		RequestID: requestID,
		Operation: &stream.Operation{
			ID:       operationID,
			ClientID: "test-client",
			Type:     "sleep",
			Cost:     1,
		},
	}
	if err := stream.Encode(connection, message); err != nil {
		t.Fatalf("send request %q: %v", requestID, err)
	}
}

func readMessage(t *testing.T, connection net.Conn, reader *bufio.Reader) stream.Message {
	t.Helper()
	if err := connection.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set TCP read deadline: %v", err)
	}

	line, err := reader.ReadBytes('\n')
	if err != nil {
		t.Fatalf("read response: %v", err)
	}

	message, err := stream.Decode(line)
	if err != nil {
		t.Fatalf("decode response %q: %v", line, err)
	}

	return message
}

func assertAdmission(t *testing.T, message stream.Message, requestID, operationID, decision string) {
	t.Helper()
	if message.Type != stream.TypeAdmission ||
		message.RequestID != requestID ||
		message.OperationID != operationID ||
		message.Decision != decision {
		t.Fatalf(
			"admission = %+v; want type=admission request=%q operation=%q decision=%q",
			message, requestID, operationID, decision,
		)
	}
}

func awaitExecutorStart(t *testing.T, executor *controlledExecutor, operationID string) {
	t.Helper()
	select {
	case startedOperationID := <-executor.started:
		if startedOperationID != operationID {
			t.Fatalf("executor started %q; want %q", startedOperationID, operationID)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("executor did not start operation %q", operationID)
	}
}

func TestServerPersistentConnectionPreservesAdmissionResultOrder(t *testing.T) {
	executor := newControlledExecutor()
	_, _, _, address, cleanup := startTestServer(t, domain.Limits{
		MaxConcurrent: 1,
		MaxCapacity:   1,
		MaxQueueSize:  2,
	}, executor)
	defer cleanup()

	connection, reader := connectTestClient(t, address)
	sendOperation(t, connection, "request-first", "operation-first")
	assertAdmission(t, readMessage(t, connection, reader), "request-first", "operation-first", "started")
	awaitExecutorStart(t, executor, "operation-first")

	sendOperation(t, connection, "request-second", "operation-second")
	assertAdmission(t, readMessage(t, connection, reader), "request-second", "operation-second", "queued")

	executor.finish("operation-first")
	firstResult := readMessage(t, connection, reader)
	if firstResult.Type != stream.TypeResult || firstResult.RequestID != "request-first" || firstResult.State != "completed" {
		t.Fatalf("first result = %+v; want completed request-first", firstResult)
	}

	awaitExecutorStart(t, executor, "operation-second")
	executor.finish("operation-second")
	secondResult := readMessage(t, connection, reader)
	if secondResult.Type != stream.TypeResult ||
		secondResult.RequestID != "request-second" ||
		secondResult.State != "completed" ||
		secondResult.Decision != "queued" {
		t.Fatalf("second result = %+v; want completed request-second with queued decision", secondResult)
	}
}

func TestServerSharesBudgetAcrossConnections(t *testing.T) {
	executor := newControlledExecutor()
	_, controller, _, address, cleanup := startTestServer(t, domain.Limits{
		MaxConcurrent: 1,
		MaxCapacity:   1,
		MaxQueueSize:  2,
	}, executor)
	defer cleanup()

	firstConnection, firstReader := connectTestClient(t, address)
	secondConnection, secondReader := connectTestClient(t, address)

	sendOperation(t, firstConnection, "first-request", "first-operation")
	assertAdmission(t, readMessage(t, firstConnection, firstReader), "first-request", "first-operation", "started")
	awaitExecutorStart(t, executor, "first-operation")

	sendOperation(t, secondConnection, "second-request", "second-operation")
	assertAdmission(t, readMessage(t, secondConnection, secondReader), "second-request", "second-operation", "queued")

	active, queued, used := controller.Snapshot()
	if active != 1 || queued != 1 || used != 1 {
		t.Fatalf("shared snapshot = (%d, %d, %d); want (1, 1, 1)", active, queued, used)
	}

	executor.finish("first-operation")
	firstResult := readMessage(t, firstConnection, firstReader)
	if firstResult.State != "completed" {
		t.Fatalf("first result state = %q; want completed", firstResult.State)
	}

	awaitExecutorStart(t, executor, "second-operation")
	executor.finish("second-operation")
	secondResult := readMessage(t, secondConnection, secondReader)
	if secondResult.State != "completed" {
		t.Fatalf("second result state = %q; want completed", secondResult.State)
	}
}

func TestServerDisconnectCancelsOnlyItsOwner(t *testing.T) {
	executor := newControlledExecutor()
	_, _, _, address, cleanup := startTestServer(t, domain.Limits{
		MaxConcurrent: 2,
		MaxCapacity:   2,
		MaxQueueSize:  2,
	}, executor)
	defer cleanup()

	firstConnection, firstReader := connectTestClient(t, address)
	secondConnection, secondReader := connectTestClient(t, address)

	sendOperation(t, firstConnection, "first-request", "first-operation")
	assertAdmission(t, readMessage(t, firstConnection, firstReader), "first-request", "first-operation", "started")
	awaitExecutorStart(t, executor, "first-operation")

	sendOperation(t, secondConnection, "second-request", "second-operation")
	assertAdmission(t, readMessage(t, secondConnection, secondReader), "second-request", "second-operation", "started")
	awaitExecutorStart(t, executor, "second-operation")

	if err := firstConnection.Close(); err != nil {
		t.Fatalf("disconnect first owner: %v", err)
	}
	executor.finish("second-operation")
	result := readMessage(t, secondConnection, secondReader)
	if result.RequestID != "second-request" || result.State != "completed" {
		t.Fatalf("remaining owner result = %+v; want completed second-request", result)
	}
}

func TestServerDuplicateRequestIDAndReuseAfterResult(t *testing.T) {
	executor := newControlledExecutor()
	_, _, _, address, cleanup := startTestServer(t, domain.Limits{
		MaxConcurrent: 1,
		MaxCapacity:   1,
		MaxQueueSize:  2,
	}, executor)
	defer cleanup()

	connection, reader := connectTestClient(t, address)
	sendOperation(t, connection, "reusable-request", "first-operation")
	assertAdmission(t, readMessage(t, connection, reader), "reusable-request", "first-operation", "started")
	awaitExecutorStart(t, executor, "first-operation")

	sendOperation(t, connection, "reusable-request", "duplicate-operation")
	duplicateResponse := readMessage(t, connection, reader)
	if duplicateResponse.Type != stream.TypeError || duplicateResponse.Error == nil || duplicateResponse.Error.Code != "duplicate_request" {
		t.Fatalf("duplicate request response = %+v; want duplicate_request error", duplicateResponse)
	}
	if duplicateResponse.RequestID != "" {
		t.Fatalf("duplicate response request_id = %q; want empty to avoid confusing the original request", duplicateResponse.RequestID)
	}

	executor.finish("first-operation")
	firstResult := readMessage(t, connection, reader)
	if firstResult.Type != stream.TypeResult || firstResult.State != "completed" {
		t.Fatalf("first result = %+v; want completed", firstResult)
	}

	sendOperation(t, connection, "reusable-request", "second-operation")
	assertAdmission(t, readMessage(t, connection, reader), "reusable-request", "second-operation", "started")
	awaitExecutorStart(t, executor, "second-operation")
	executor.finish("second-operation")
	secondResult := readMessage(t, connection, reader)
	if secondResult.Type != stream.TypeResult || secondResult.State != "completed" {
		t.Fatalf("second result = %+v; want completed", secondResult)
	}
}

func TestServerRejectsInvalidMessagesWithoutClosingConnection(t *testing.T) {
	executor := newControlledExecutor()
	_, _, _, address, cleanup := startTestServer(t, domain.Limits{
		MaxConcurrent: 1,
		MaxCapacity:   1,
		MaxQueueSize:  1,
	}, executor)
	defer cleanup()

	connection, reader := connectTestClient(t, address)
	if _, err := fmt.Fprint(connection, "{\"type\":\"unrecognized\"}\n"); err != nil {
		t.Fatalf("send invalid message: %v", err)
	}
	response := readMessage(t, connection, reader)
	if response.Type != stream.TypeError || response.Error == nil || response.Error.Code != "invalid_request" {
		t.Fatalf("invalid message response = %+v; want invalid_request error", response)
	}

	sendOperation(t, connection, "valid-request", "valid-operation")
	assertAdmission(t, readMessage(t, connection, reader), "valid-request", "valid-operation", "started")
	awaitExecutorStart(t, executor, "valid-operation")
	executor.finish("valid-operation")
	result := readMessage(t, connection, reader)
	if result.State != "completed" {
		t.Fatalf("result state = %q; want completed", result.State)
	}
}

func TestServerConfigValidation(t *testing.T) {
	invalidConfigs := []struct {
		name   string
		config Config
	}{
		{name: "zero connections", config: Config{MaxMessageBytes: 32, OutgoingQueue: 1, ReadTimeout: time.Second, WriteTimeout: time.Second}},
		{name: "zero maximum message size", config: Config{MaxConnections: 1, OutgoingQueue: 1, ReadTimeout: time.Second, WriteTimeout: time.Second}},
		{name: "zero outgoing queue", config: Config{MaxConnections: 1, MaxMessageBytes: 32, ReadTimeout: time.Second, WriteTimeout: time.Second}},
		{name: "negative read timeout", config: Config{MaxConnections: 1, MaxMessageBytes: 32, OutgoingQueue: 1, ReadTimeout: -time.Second, WriteTimeout: time.Second}},
		{name: "zero write timeout", config: Config{MaxConnections: 1, MaxMessageBytes: 32, OutgoingQueue: 1, ReadTimeout: time.Second}},
	}
	for _, scenario := range invalidConfigs {
		t.Run(scenario.name, func(t *testing.T) {
			if err := scenario.config.Validate(); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("Validate() error = %v; want ErrInvalidConfig", err)
			}
		})
	}
}

func TestServerAllowsDisabledReadTimeout(t *testing.T) {
	config := Config{
		MaxConnections:  1,
		MaxMessageBytes: 4096,
		OutgoingQueue:   4,
		ReadTimeout:     0,
		WriteTimeout:    time.Second,
	}

	if err := config.Validate(); err != nil {
		t.Fatalf("Validate() = %v; want nil for disabled read timeout", err)
	}

	executor := newControlledExecutor()
	controller, err := admission.New(domain.Limits{
		MaxConcurrent: 1,
		MaxCapacity:   1,
		MaxQueueSize:  1,
	})
	if err != nil {
		t.Fatalf("create Controller: %v", err)
	}

	coordinator, err := continuous.New(controller, executor)
	if err != nil {
		t.Fatalf("create Coordinator: %v", err)
	}
	if err := coordinator.Start(); err != nil {
		t.Fatalf("start Coordinator: %v", err)
	}

	server, err := New(config, coordinator)
	if err != nil {
		t.Fatalf("create TCP server: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen on TCP: %v", err)
	}

	serverContext, cancelServer := context.WithCancel(context.Background())
	serverFinished := make(chan error, 1)
	go func() { serverFinished <- server.Serve(serverContext, listener) }()
	defer func() {
		cancelServer()
		_ = listener.Close()
		select {
		case serverError := <-serverFinished:
			if serverError != nil {
				t.Errorf("Serve() = %v; want nil", serverError)
			}
		case <-time.After(3 * time.Second):
			t.Error("Serve() did not terminate")
		}
		closeContext, cancelClose := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancelClose()

		if closeError := coordinator.Close(closeContext); closeError != nil {
			t.Errorf("Close() = %v; want nil", closeError)
		}
	}()

	connection, reader := connectTestClient(t, listener.Addr().String())
	sendOperation(t, connection, "long-wait-request", "long-wait-operation")
	assertAdmission(t, readMessage(t, connection, reader), "long-wait-request", "long-wait-operation", "started")
	awaitExecutorStart(t, executor, "long-wait-operation")

	time.Sleep(100 * time.Millisecond)
	executor.finish("long-wait-operation")
	result := readMessage(t, connection, reader)
	if result.State != "completed" {
		t.Fatalf("result State = %q; want completed", result.State)
	}
}

type cancellationResistantExecutor struct {
	started   chan string
	cancelled chan string
	mutex     sync.Mutex
	releases  map[string]chan struct{}
}

func newCancellationResistantExecutor() *cancellationResistantExecutor {
	return &cancellationResistantExecutor{
		started:   make(chan string, 8),
		cancelled: make(chan string, 8),
		releases:  make(map[string]chan struct{}),
	}
}

func (executor *cancellationResistantExecutor) Execute(ctx context.Context, operation domain.Operation) error {
	release := make(chan struct{})
	executor.mutex.Lock()
	executor.releases[operation.ID] = release
	executor.mutex.Unlock()

	executor.started <- operation.ID

	select {
	case <-ctx.Done():
		executor.cancelled <- operation.ID
		<-release
		return ctx.Err()
	case <-release:
		return nil
	}
}

func (executor *cancellationResistantExecutor) release(operationID string) {
	executor.mutex.Lock()
	defer executor.mutex.Unlock()

	close(executor.releases[operationID])
}

func assertExecutorEvent(t *testing.T, events <-chan string, expectedOperationID string) {
	t.Helper()

	select {
	case operationID := <-events:
		if operationID != expectedOperationID {
			t.Fatalf("executor event operation = %q; want %q", operationID, expectedOperationID)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("executor event for %q was not received", expectedOperationID)
	}
}

func assertControllerSnapshotEventually(
	t *testing.T,
	controller *admission.Controller,
	expectedActive int,
	expectedQueued int,
	expectedUsed uint64,
) {
	t.Helper()

	deadline := time.After(2 * time.Second)
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()

	for {
		active, queued, used := controller.Snapshot()
		if active == expectedActive && queued == expectedQueued && used == expectedUsed {
			return
		}

		select {
		case <-deadline:
			t.Fatalf(
				"Controller.Snapshot() = (%d, %d, %d); want (%d, %d, %d)",
				active, queued, used, expectedActive, expectedQueued, expectedUsed,
			)
		case <-poll.C:
		}
	}
}

func TestServerDisconnectKeepsReservationUntilExecutorReturns(t *testing.T) {
	executor := newCancellationResistantExecutor()
	controller, err := admission.New(domain.Limits{
		MaxConcurrent: 1,
		MaxCapacity:   1,
		MaxQueueSize:  3,
	})
	if err != nil {
		t.Fatalf("create Controller: %v", err)
	}

	coordinator, err := continuous.New(controller, executor)
	if err != nil {
		t.Fatalf("create Coordinator: %v", err)
	}
	if err := coordinator.Start(); err != nil {
		t.Fatalf("start Coordinator: %v", err)
	}

	server, err := New(Config{
		MaxConnections:  3,
		MaxMessageBytes: 4096,
		OutgoingQueue:   16,
		WriteTimeout:    time.Second,
	}, coordinator)
	if err != nil {
		t.Fatalf("create TCP stream server: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	serverContext, cancelServer := context.WithCancel(context.Background())
	serverFinished := make(chan error, 1)
	go func() {
		serverFinished <- server.Serve(serverContext, listener)
	}()

	defer func() {
		cancelServer()
		_ = listener.Close()
		select {
		case serveError := <-serverFinished:
			if serveError != nil {
				t.Errorf("Serve() = %v; want nil", serveError)
			}
		case <-time.After(3 * time.Second):
			t.Error("Serve() did not terminate")
		}

		closeContext, cancelClose := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancelClose()
		if closeError := coordinator.Close(closeContext); closeError != nil {
			t.Errorf("Coordinator.Close() = %v; want nil", closeError)
		}
	}()

	disconnectingConnection, disconnectingReader := connectTestClient(t, listener.Addr().String())
	remainingConnection, remainingReader := connectTestClient(t, listener.Addr().String())

	sendOperation(t, disconnectingConnection, "running-request", "running-operation")
	assertAdmission(t, readMessage(t, disconnectingConnection, disconnectingReader), "running-request", "running-operation", "started")
	assertExecutorEvent(t, executor.started, "running-operation")

	sendOperation(t, disconnectingConnection, "cancelled-request", "cancelled-operation")
	assertAdmission(t, readMessage(t, disconnectingConnection, disconnectingReader), "cancelled-request", "cancelled-operation", "queued")

	sendOperation(t, remainingConnection, "remaining-request", "remaining-operation")
	assertAdmission(t, readMessage(t, remainingConnection, remainingReader), "remaining-request", "remaining-operation", "queued")

	assertControllerSnapshotEventually(t, controller, 1, 2, 1)

	if err := disconnectingConnection.Close(); err != nil {
		t.Fatalf("disconnect first owner: %v", err)
	}
	assertExecutorEvent(t, executor.cancelled, "running-operation")

	assertControllerSnapshotEventually(t, controller, 1, 1, 1)

	executor.release("running-operation")
	assertExecutorEvent(t, executor.started, "remaining-operation")
	assertControllerSnapshotEventually(t, controller, 1, 0, 1)

	executor.release("remaining-operation")
	result := readMessage(t, remainingConnection, remainingReader)
	if result.Type != stream.TypeResult || result.RequestID != "remaining-request" || result.State != "completed" {
		t.Fatalf("remaining owner result = %+v; want completed remaining-request", result)
	}
	assertControllerSnapshotEventually(t, controller, 0, 0, 0)
}

func TestServerHalfCloseCancelsOwner(t *testing.T) {
	executor := newControlledExecutor()
	_, controller, _, address, cleanup := startTestServer(t, domain.Limits{
		MaxConcurrent: 1,
		MaxCapacity:   1,
		MaxQueueSize:  1,
	}, executor)
	defer cleanup()

	connection, reader := connectTestClient(t, address)
	sendOperation(t, connection, "half-close-request", "half-close-operation")
	assertAdmission(t, readMessage(t, connection, reader), "half-close-request", "half-close-operation", "started")
	awaitExecutorStart(t, executor, "half-close-operation")

	tcpConnection, ok := connection.(*net.TCPConn)
	if !ok {
		t.Fatalf("connection type = %T; want *net.TCPConn", connection)
	}
	if err := tcpConnection.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite() = %v; want nil", err)
	}

	assertControllerSnapshotEventually(t, controller, 0, 0, 0)
}

type rejectionEngine struct{}

func (rejectionEngine) Submit(_ string, _ domain.Operation) (admission.Decision, <-chan continuous.Result, error) {
	return admission.Rejected, nil, nil
}

func (rejectionEngine) CancelOwner(_ string) error {
	return nil
}

func TestServerConnectionLimitRejectsExcessAndRecovers(t *testing.T) {
	server, err := New(Config{
		MaxConnections:  1,
		MaxMessageBytes: 4096,
		OutgoingQueue:   2,
		ReadTimeout:     0,
		WriteTimeout:    time.Second,
	}, rejectionEngine{})
	if err != nil {
		t.Fatalf("create server: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	serverContext, cancelServer := context.WithCancel(context.Background())
	serverFinished := make(chan error, 1)
	go func() {
		serverFinished <- server.Serve(serverContext, listener)
	}()
	defer func() {
		cancelServer()
		_ = listener.Close()
		select {
		case serverError := <-serverFinished:
			if serverError != nil {
				t.Errorf("server shutdown: %v", serverError)
			}
		case <-time.After(2 * time.Second):
			t.Error("server did not stop")
		}
	}()

	firstConnection, firstReader := connectTestClient(t, listener.Addr().String())
	sendOperation(t, firstConnection, "first-request", "first-operation")
	assertAdmission(t, readMessage(t, firstConnection, firstReader), "first-request", "first-operation", "rejected")

	excessConnection, excessReader := connectTestClient(t, listener.Addr().String())
	if err := excessConnection.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := excessReader.ReadByte(); !errors.Is(err, io.EOF) {
		t.Fatalf("excess connection read error = %v; want EOF", err)
	}

	if err := firstConnection.Close(); err != nil {
		t.Fatalf("close admitted connection: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		candidate, dialError := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
		if dialError != nil {
			t.Fatalf("connect after releasing capacity: %v", dialError)
		}

		message := stream.Message{
			Type:      stream.TypeSubmit,
			RequestID: "recovery-request",
			Operation: &stream.Operation{
				ID:       "recovery-operation",
				ClientID: "test-client",
				Type:     "sleep",
				Cost:     1,
			},
		}
		writeError := stream.Encode(candidate, message)
		if writeError == nil {
			_ = candidate.SetReadDeadline(time.Now().Add(40 * time.Millisecond))
			line, readError := bufio.NewReader(candidate).ReadBytes('\n')
			if readError == nil {
				response, decodeError := stream.Decode(line)
				_ = candidate.Close()
				if decodeError != nil {
					t.Fatalf("decode recovery response: %v", decodeError)
				}
				assertAdmission(t, response, "recovery-request", "recovery-operation", "rejected")
				return
			}
		}
		_ = candidate.Close()
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("server did not admit new connections after capacity was released")
}

func TestServerSlowReaderDoesNotBlockConnectionHandler(t *testing.T) {
	server, err := New(Config{
		MaxConnections:  1,
		MaxMessageBytes: 4096,
		OutgoingQueue:   1,
		ReadTimeout:     0,
		WriteTimeout:    50 * time.Millisecond,
	}, rejectionEngine{})
	if err != nil {
		t.Fatalf("create server: %v", err)
	}

	serverConnection, clientConnection := net.Pipe()
	defer clientConnection.Close()

	handlerFinished := make(chan struct{})
	go func() {
		defer close(handlerFinished)
		server.handle(context.Background(), serverConnection)
	}()

	request := stream.Message{
		Type:      stream.TypeSubmit,
		RequestID: "slow-reader-request",
		Operation: &stream.Operation{
			ID: "slow-reader-operation", ClientID: "slow-client", Type: "sleep", Cost: 1,
		},
	}
	if err := stream.Encode(clientConnection, request); err != nil {
		t.Fatalf("submit operation: %v", err)
	}

	select {
	case <-handlerFinished:
	case <-time.After(2 * time.Second):
		t.Fatal("slow client blocked its connection handler past the write timeout")
	}
}

func TestServerShutdownClosesConnectionsButKeepsActiveReservations(t *testing.T) {
	executor := newCancellationResistantExecutor()
	controller, err := admission.New(domain.Limits{
		MaxConcurrent: 1,
		MaxCapacity:   1,
		MaxQueueSize:  1,
	})
	if err != nil {
		t.Fatal(err)
	}

	coordinator, err := continuous.New(controller, executor)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Start(); err != nil {
		t.Fatal(err)
	}

	server, err := New(Config{
		MaxConnections:  2,
		MaxMessageBytes: 4096,
		OutgoingQueue:   4,
		WriteTimeout:    time.Second,
	}, coordinator)
	if err != nil {
		t.Fatal(err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	serverContext, cancelServer := context.WithCancel(context.Background())
	serverFinished := make(chan error, 1)
	go func() {
		serverFinished <- server.Serve(serverContext, listener)
	}()

	connection, reader := connectTestClient(t, listener.Addr().String())
	sendOperation(t, connection, "shutdown-request", "shutdown-operation")
	assertAdmission(t, readMessage(t, connection, reader), "shutdown-request", "shutdown-operation", "started")
	assertExecutorEvent(t, executor.started, "shutdown-operation")

	cancelServer()

	select {
	case serveError := <-serverFinished:
		if serveError != nil {
			t.Fatalf("Server.Serve() = %v, want nil after shutdown", serveError)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("TCP server did not stop after context cancellation")
	}

	assertExecutorEvent(t, executor.cancelled, "shutdown-operation")
	assertControllerSnapshotEventually(t, controller, 1, 0, 1)

	closeContext, cancelClose := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelClose()
	if closeError := coordinator.Close(closeContext); !errors.Is(closeError, context.DeadlineExceeded) {
		t.Fatalf("Coordinator.Close() = %v, want context.DeadlineExceeded", closeError)
	}

	assertControllerSnapshotEventually(t, controller, 1, 0, 1)
	executor.release("shutdown-operation")

	finishContext, cancelFinish := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelFinish()
	if closeError := coordinator.Close(finishContext); closeError != nil {
		t.Fatalf("Coordinator.Close() after executor return = %v, want nil", closeError)
	}
	assertControllerSnapshotEventually(t, controller, 0, 0, 0)
}
