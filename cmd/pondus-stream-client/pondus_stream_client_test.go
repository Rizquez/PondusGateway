package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"pondus-gateway/internal/control/admission"
	"pondus-gateway/internal/domain"
	"pondus-gateway/internal/execution/continuous"
	"pondus-gateway/internal/execution/simulate"
	"pondus-gateway/internal/transport/stream"
	tcpstream "pondus-gateway/internal/transport/tcp-stream"
	"strings"
	"testing"
	"time"
)

func testStreamRequest(requestID, operationID string) stream.Message {
	return stream.Message{
		Type:      stream.TypeSubmit,
		RequestID: requestID,
		Operation: &stream.Operation{
			ID:       operationID,
			ClientID: "client-test",
			Type:     "sleep",
			Cost:     1,
		},
	}
}

func createRequestFile(t *testing.T, requests []stream.Message) string {
	t.Helper()

	filePath := filepath.Join(t.TempDir(), "requests.jsonl")
	var content bytes.Buffer
	for _, request := range requests {
		if err := stream.Encode(&content, request); err != nil {
			t.Fatalf("encode test request %q: %v", request.RequestID, err)
		}
	}

	if err := os.WriteFile(filePath, content.Bytes(), 0600); err != nil {
		t.Fatalf("write request file: %v", err)
	}

	return filePath
}

func assertContains(t *testing.T, output, expected string) {
	t.Helper()
	if !strings.Contains(output, expected) {
		t.Fatalf("output = %q, want substring %q", output, expected)
	}
}

func TestClientConfigValidation(t *testing.T) {
	tests := []struct {
		name  string
		args  []string
		valid bool
	}{
		{name: "default configuration", valid: true},
		{name: "custom window", args: []string{"-window", "2"}, valid: true},
		{name: "invalid window", args: []string{"-window", "0"}},
		{name: "invalid timeout", args: []string{"-timeout", "0s"}},
		{name: "invalid message size", args: []string{"-max-message-bytes", "0"}},
		{name: "unexpected argument", args: []string{"extra"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseConfig(test.args)
			if (err == nil) != test.valid {
				t.Fatalf("parseConfig(%v) error = %v, want valid=%t", test.args, err, test.valid)
			}
		})
	}
}

func TestLoadRequestsValidation(t *testing.T) {
	tests := []struct {
		name      string
		content   string
		wantCount int
	}{
		{
			name: "valid NDJSON",
			content: `{"type":"submit","request_id":"first","operation":{"id":"operation-first","client_id":"client-a","type":"sleep","cost":1}}` + "\n" +
				`{"type":"submit","request_id":"second","operation":{"id":"operation-second","client_id":"client-a","type":"sleep","cost":1}}` + "\n",
			wantCount: 2,
		},
		{name: "empty file"},
		{name: "malformed JSON", content: "{\n"},
		{name: "duplicate request ID", content: `{"type":"submit","request_id":"same","operation":{"id":"first","client_id":"client-a","type":"sleep","cost":1}}` + "\n" + `{"type":"submit","request_id":"same","operation":{"id":"second","client_id":"client-a","type":"sleep","cost":1}}` + "\n"},
		{name: "duplicate operation ID", content: `{"type":"submit","request_id":"first","operation":{"id":"same","client_id":"client-a","type":"sleep","cost":1}}` + "\n" + `{"type":"submit","request_id":"second","operation":{"id":"same","client_id":"client-a","type":"sleep","cost":1}}` + "\n"},
		{name: "response is not request", content: `{"type":"error","request_id":"bad","error":{"code":"invalid","message":"wrong"}}` + "\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			filePath := filepath.Join(t.TempDir(), "requests.jsonl")
			if err := os.WriteFile(filePath, []byte(test.content), 0600); err != nil {
				t.Fatal(err)
			}
			requests, err := loadRequests(filePath, 65536)
			if test.wantCount == 0 {
				if err == nil {
					t.Fatalf("loadRequests(%s) = %d requests, want an error", test.name, len(requests))
				}
				return
			}
			if err != nil || len(requests) != test.wantCount {
				t.Fatalf(
					"loadRequests(%s) = %d requests, error=%v; want %d requests",
					test.name, len(requests), err, test.wantCount,
				)
			}
		})
	}
}

func serveScriptedConnection(t *testing.T, serverConnection net.Conn) {
	t.Helper()
	defer serverConnection.Close()

	const maxMessageBytes = 8192
	_ = serverConnection.SetReadDeadline(time.Now().Add(2 * time.Second))

	var receivedRequests []stream.Message
	reader := bufio.NewReader(serverConnection)
	for len(receivedRequests) < 2 {
		line, err := readLine(reader, maxMessageBytes)
		if err != nil {
			t.Errorf("server read request: %v", err)
			return
		}

		request, err := stream.Decode(line)
		if err != nil {
			t.Errorf("server decode request: %v", err)
			return
		}

		receivedRequests = append(receivedRequests, request)
	}

	responses := []stream.Message{
		{Type: stream.TypeAdmission, RequestID: "first", OperationID: "operation-first", Decision: stream.DecisionStarted},
		{Type: stream.TypeAdmission, RequestID: "second", OperationID: "operation-second", Decision: stream.DecisionQueued},
		{Type: stream.TypeResult, RequestID: "second", OperationID: "operation-second", Decision: stream.DecisionQueued, State: string(domain.StateCompleted)},
		{Type: stream.TypeResult, RequestID: "first", OperationID: "operation-first", Decision: stream.DecisionStarted, State: string(domain.StateCompleted)},
	}
	for _, response := range responses {
		if err := stream.Encode(serverConnection, response); err != nil {
			t.Errorf("server write response: %v", err)
			return
		}
	}
}

func startScriptedServer(t *testing.T, handler func(net.Conn)) (string, func()) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	finished := make(chan struct{})
	go func() {
		defer close(finished)
		connection, acceptError := listener.Accept()
		if acceptError != nil {
			return
		}
		handler(connection)
	}()

	return listener.Addr().String(), func() {
		_ = listener.Close()
		<-finished
	}
}

func TestClientReceivesInterleavedResults(t *testing.T) {
	address, stopServer := startScriptedServer(t, func(connection net.Conn) {
		serveScriptedConnection(t, connection)
	})
	defer stopServer()

	filePath := createRequestFile(t, []stream.Message{
		testStreamRequest("first", "operation-first"),
		testStreamRequest("second", "operation-second"),
	})

	var output bytes.Buffer
	err := run(context.Background(), []string{"-addr", address, "-file", filePath, "-window", "2", "-timeout", "2s"}, &output)
	if err != nil {
		t.Fatalf("run client: %v", err)
	}

	assertContains(t, output.String(), "admission request_id=first operation_id=operation-first decision=started")
	assertContains(t, output.String(), "admission request_id=second operation_id=operation-second decision=queued")
	assertContains(t, output.String(), "result request_id=second operation_id=operation-second state=completed")
	assertContains(t, output.String(), "result request_id=first operation_id=operation-first state=completed")
}

func TestClientRejectsWrongOperationID(t *testing.T) {
	address, stopServer := startScriptedServer(t, func(connection net.Conn) {
		defer connection.Close()
		reader := bufio.NewReader(connection)
		_, _ = readLine(reader, 65536)
		response := stream.Message{
			Type:        stream.TypeAdmission,
			RequestID:   "request-a",
			OperationID: "other-operation",
			Decision:    stream.DecisionStarted,
		}
		_ = stream.Encode(connection, response)
	})
	defer stopServer()

	filePath := createRequestFile(t, []stream.Message{testStreamRequest("request-a", "expected-operation")})
	var output bytes.Buffer

	err := run(context.Background(), []string{"-addr", address, "-file", filePath, "-timeout", "2s"}, &output)
	if err == nil || !strings.Contains(err.Error(), "invalid admission") {
		t.Fatalf("run error = %v, want invalid admission error", err)
	}
}

func TestClientTimeoutWithoutResponses(t *testing.T) {
	address, stopServer := startScriptedServer(t, func(connection net.Conn) {
		defer connection.Close()
		reader := bufio.NewReader(connection)
		_, _ = readLine(reader, 65536)
		<-time.After(100 * time.Millisecond)
	})
	defer stopServer()

	filePath := createRequestFile(t, []stream.Message{testStreamRequest("request-timeout", "operation-timeout")})
	var output bytes.Buffer

	err := run(context.Background(), []string{"-addr", address, "-file", filePath, "-timeout", "20ms"}, &output)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("run error = %v, want context deadline exceeded", err)
	}
}

func TestClientWithRealContinuousServer(t *testing.T) {
	controller, err := admission.New(domain.Limits{MaxConcurrent: 1, MaxCapacity: 2, MaxQueueSize: 4})
	if err != nil {
		t.Fatal(err)
	}

	coordinator, err := continuous.New(controller, simulate.SleepExecutor{Duration: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}

	if err := coordinator.Start(); err != nil {
		t.Fatal(err)
	}

	server, err := tcpstream.New(tcpstream.Config{
		MaxConnections:  2,
		MaxMessageBytes: 65536,
		OutgoingQueue:   16,
		WriteTimeout:    time.Second,
	}, coordinator)
	if err != nil {
		t.Fatal(err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serverContext, cancelServer := context.WithCancel(context.Background())
	serverFinished := make(chan error, 1)
	go func() { serverFinished <- server.Serve(serverContext, listener) }()

	defer func() {
		cancelServer()
		_ = listener.Close()
		if serveError := <-serverFinished; serveError != nil {
			t.Errorf("server exit: %v", serveError)
		}
		closeContext, cancelClose := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancelClose()
		if closeError := coordinator.Close(closeContext); closeError != nil {
			t.Errorf("coordinator close: %v", closeError)
		}
	}()

	filePath := createRequestFile(t, []stream.Message{
		testStreamRequest("first", "operation-first"),
		testStreamRequest("second", "operation-second"),
		testStreamRequest("third", "operation-third"),
	})
	var output bytes.Buffer
	clientArgs := []string{"-addr", listener.Addr().String(), "-file", filePath, "-window", "2", "-timeout", "2s"}

	if err := run(context.Background(), clientArgs, &output); err != nil {
		t.Fatalf("run client: %v", err)
	}

	for requestIndex := 1; requestIndex <= 3; requestIndex++ {
		requestID := []string{"first", "second", "third"}[requestIndex-1]
		assertContains(t, output.String(), fmt.Sprintf("result request_id=%s ", requestID))
	}
}

func TestClientAcceptsTerminalErrorAfterAdmission(t *testing.T) {
	request := &requestProgress{
		operationID: "operation-failed",
		decision:    stream.DecisionStarted,
		admitted:    true,
	}
	progress := map[string]*requestProgress{"request-failed": request}
	response := stream.Message{
		Type:      stream.TypeError,
		RequestID: "request-failed",
		Error: &stream.Failure{
			Code:    "execution_error",
			Message: "missing operation result",
		},
	}
	var output bytes.Buffer

	finished, err := processResponse(response, progress, &output)
	if err != nil || !finished || !request.finished {
		t.Fatalf(
			"processResponse() = finished=%t, error=%v, request.finished=%t; want terminal success",
			finished, err, request.finished,
		)
	}
	assertContains(t, output.String(), "error request_id=request-failed code=execution_error message=missing operation result")
}
