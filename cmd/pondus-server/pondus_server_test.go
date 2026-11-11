package main

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net"
	"pondus-gateway/internal/domain"
	"pondus-gateway/internal/transport/protocol"
	"pondus-gateway/internal/transport/tcp"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type acceptListener struct {
	net.Listener
	acceptedThird chan struct{}
	busyWrite     chan struct{}
	unblockWrite  chan struct{}
	count         int
}

type blockedWriteConn struct {
	net.Conn
	started chan struct{}
	release chan struct{}
}

func (listener *acceptListener) Accept() (net.Conn, error) {
	conn, err := listener.Listener.Accept()
	if err != nil {
		return nil, err
	}

	listener.count++

	switch listener.count {
	case 2:
		return &blockedWriteConn{
			Conn:    conn,
			started: listener.busyWrite,
			release: listener.unblockWrite,
		}, nil
	case 3:
		close(listener.acceptedThird)
	}
	return conn, nil
}

func (conn *blockedWriteConn) Write(p []byte) (int, error) {
	close(conn.started)
	<-conn.release
	return conn.Conn.Write(p)
}

func testConfig() Config {
	return Config{
		MaxMessageBytes: 2048,
		MaxConnections:  2,
		ReadTimeout:     time.Second,
		WriteTimeout:    time.Second,
	}
}

func requestJSON() string {
	return `{"request_id":"req-1","operations":[{"id":"op-1","client_id":"a","type":"sleep","cost":1}]}` + "\n"
}

func startTestServer(t *testing.T, config Config, handler Handler) (string, context.CancelFunc) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	server, err := NewServer(config, handler)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- server.Serve(ctx, listener)
	}()

	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Serve: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("Serve didn't finish")
		}
	})

	return listener.Addr().String(), cancel
}

func send(t *testing.T, addr, message string) protocol.Response {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

	if _, err := conn.Write([]byte(message)); err != nil {
		t.Fatal(err)
	}

	line, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}

	var response protocol.Response
	if err := json.Unmarshal(line, &response); err != nil {
		t.Fatal(err)
	}

	return response
}

func TestServerValidRequestNotYetExecutable(t *testing.T) {
	addr, _ := startTestServer(t, testConfig(), nil)
	response := send(t, addr, requestJSON())
	if response.RequestID != "req-1" ||
		response.Status != protocol.StatusError ||
		response.Error == nil ||
		response.Error.Code != protocol.CodeExecutionError {
		t.Fatalf("response: %+v", response)
	}
}

func TestServerMalformedAndInvalidRequests(t *testing.T) {
	addr, _ := startTestServer(t, testConfig(), nil)
	tests := []struct{ name, message, code string }{
		{"invalid JSON", "{\n", protocol.CodeInvalidRequest},
		{"unknown field", `{"request_id":"x","operations":[],"extra":true}` + "\n", protocol.CodeInvalidRequest},
		{"invalid operation", `{"request_id":"x","operations":[{"id":"a","client_id":"b","type":"sleep","cost":0}]}` + "\n", protocol.CodeInvalidOperation},
		{"two documents", requestJSON() + requestJSON(), ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := send(t, addr, test.message)

			if test.code == "" {
				if response.Error == nil || response.Error.Code != protocol.CodeExecutionError {
					t.Fatalf("%+v", response)
				}
				return
			}

			if response.Error == nil || response.Error.Code != test.code {
				t.Fatalf("response: %+v", response)
			}
		})
	}
}

func TestServerRejectsOversizeAndIncomplete(t *testing.T) {
	config := testConfig()
	config.MaxMessageBytes = 70
	addr, _ := startTestServer(t, config, nil)
	response := send(t, addr, strings.Repeat("x", 72)+"\n")
	if response.Error == nil || response.Error.Code != protocol.CodeInvalidRequest {
		t.Fatalf("%+v", response)
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}

	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn.Write([]byte("{\"request_id\":\"partial\""))

	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.CloseWrite()
	}

	line, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}

	var result protocol.Response
	if err = json.Unmarshal(line, &result); err != nil {
		t.Fatal(err)
	}

	if result.Error == nil || result.Error.Code != protocol.CodeInvalidRequest {
		t.Fatalf("%+v", result)
	}
}

func TestServerRejectsConcurrentConnections(t *testing.T) {
	config := testConfig()
	config.MaxConnections = 1
	entered := make(chan struct{})
	release := make(chan struct{})

	handler := func(ctx context.Context, req protocol.Request) protocol.Response {
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return protocol.Response{Status: protocol.StatusCompleted}
	}

	addr, _ := startTestServer(t, config, handler)
	first, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}

	defer first.Close()
	_ = first.SetDeadline(time.Now().Add(2 * time.Second))

	if _, err := first.Write([]byte(requestJSON())); err != nil {
		t.Fatal(err)
	}

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}

	second := send(t, addr, requestJSON())
	if second.Error == nil || second.Error.Code != protocol.CodeServerBusy {
		t.Fatalf("%+v", second)
	}

	close(release)
	line, err := bufio.NewReader(first).ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}

	var result protocol.Response
	if err = json.Unmarshal(line, &result); err != nil {
		t.Fatal(err)
	}

	if result.Status != protocol.StatusCompleted || result.RequestID != "req-1" {
		t.Fatalf("%+v", result)
	}
}

func TestServerReadTimeoutAndConfiguration(t *testing.T) {
	bad := testConfig()
	bad.MaxConnections = 0
	if _, err := NewServer(bad, nil); err == nil {
		t.Fatal("an error was expected")
	}

	config := testConfig()
	config.ReadTimeout = 20 * time.Millisecond
	addr, _ := startTestServer(t, config, nil)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}

	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))

	line, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}

	result, err := protocol.DecodeResponse(line)
	if err != nil {
		t.Fatal(err)
	}

	if result.Error == nil || result.Error.Code != protocol.CodeInvalidRequest {
		t.Fatalf("%+v", result)
	}
}

func TestServerShutdownInterruptsPendingRead(t *testing.T) {
	config := testConfig()
	config.ReadTimeout = 5 * time.Second
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	server, err := NewServer(config, nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- server.Serve(ctx, listener)
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		cancel()
		t.Fatal(err)
	}

	defer conn.Close()
	_, _ = conn.Write([]byte("{"))
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("the server did not terminate when the context was canceled")
	}
}

func TestServerRunsTCPWorkload(t *testing.T) {
	engine, err := tcp.New(tcp.Config{
		Limits: domain.Limits{
			MaxConcurrent: 1,
			MaxCapacity:   2,
			MaxQueueSize:  2,
		},
		Scheduler:     "fifo",
		SleepDuration: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	addr, _ := startTestServer(t, testConfig(), engine.Handle)
	response := send(t, addr, requestJSON())
	if response.RequestID != "req-1" ||
		response.Status != protocol.StatusCompleted ||
		len(response.Results) != 1 ||
		response.Results[0].State != "completed" {
		t.Fatalf("response: %+v", response)
	}
}

func TestServerCancelsOnClientDisconnect(t *testing.T) {
	started := make(chan struct{})
	cancelled := make(chan struct{})
	handler := func(ctx context.Context, req protocol.Request) protocol.Response {
		close(started)
		<-ctx.Done()
		close(cancelled)
		return errorResponse(req.RequestID, protocol.CodeExecutionError, "canceled")
	}

	addr, _ := startTestServer(t, testConfig(), handler)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}

	if _, err = conn.Write([]byte(requestJSON())); err != nil {
		t.Fatal(err)
	}

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("the application was not received")
	}

	_ = conn.Close()

	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("the disconnection did not clear the handler's context")
	}
}

func TestServerBusyConnectionDoesNotStallAccept(t *testing.T) {
	cfg := testConfig()
	cfg.MaxConnections = 1

	base, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	listener := &acceptListener{
		Listener:      base,
		acceptedThird: make(chan struct{}),
		busyWrite:     make(chan struct{}),
		unblockWrite:  make(chan struct{}),
	}

	entered := make(chan struct{})
	releaseHandler := make(chan struct{})
	handler := func(ctx context.Context, _ protocol.Request) protocol.Response {
		close(entered)
		select {
		case <-releaseHandler:
		case <-ctx.Done():
		}
		return protocol.Response{Status: protocol.StatusCompleted}
	}

	server, err := NewServer(cfg, handler)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- server.Serve(ctx, listener)
	}()

	defer func() {
		close(listener.unblockWrite)
		close(releaseHandler)
		cancel()

		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Serve: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("Serve did not stop")
		}
	}()

	first, err := net.DialTimeout("tcp", base.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}

	defer first.Close()
	if _, err := first.Write([]byte(requestJSON())); err != nil {
		t.Fatal(err)
	}

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first handler did not start")
	}

	second, err := net.DialTimeout("tcp", base.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}

	defer second.Close()
	select {
	case <-listener.busyWrite:
	case <-time.After(time.Second):
		t.Fatal("server did not start writing server_busy")
	}

	third, err := net.DialTimeout("tcp", base.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer third.Close()

	select {
	case <-listener.acceptedThird:
	case <-time.After(150 * time.Millisecond):
		t.Error("a blocked server_busy response stalled the accept loop")
	}
}

func TestServerOnlyProcessesFirstRequestPerConnection(t *testing.T) {
	var calls atomic.Int32
	entered := make(chan struct{})
	release := make(chan struct{})
	handler := func(ctx context.Context, req protocol.Request) protocol.Response {
		if calls.Add(1) == 1 {
			close(entered)
		}

		select {
		case <-release:
		case <-ctx.Done():
		}
		return protocol.Response{Status: protocol.StatusCompleted}
	}

	addr, _ := startTestServer(t, testConfig(), handler)
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

	if _, err := io.WriteString(conn, requestJSON()+strings.Replace(requestJSON(), "req-1", "req-2", 1)); err != nil {
		t.Fatal(err)
	}

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("the server did not start the first batch")
	}

	close(release)

	reader := bufio.NewReader(conn)
	line, err := reader.ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}

	respose, err := protocol.DecodeResponse(line)
	if err != nil {
		t.Fatal(err)
	}

	if respose.RequestID != "req-1" || respose.Status != protocol.StatusCompleted {
		t.Fatalf("unexpected response: %+v", respose)
	}

	_, err = reader.ReadBytes('\n')
	if err != io.EOF {
		t.Fatalf("EOF was expected after the first result; err=%v", err)
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("%d batches were executed; only one was expected", got)
	}
}

func TestServerTrailingBytesDoNotCancelExecution(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	canceled := make(chan struct{}, 1)
	handler := func(ctx context.Context, _ protocol.Request) protocol.Response {
		close(entered)
		select {
		case <-ctx.Done():
			canceled <- struct{}{}
		case <-release:
		}
		return protocol.Response{Status: protocol.StatusCompleted}
	}
	addr, _ := startTestServer(t, testConfig(), handler)
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.WriteString(conn, requestJSON()); err != nil {
		t.Fatal(err)
	}

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("the server did not start running")
	}

	if _, err := io.WriteString(conn, "additional-information\n"); err != nil {
		t.Fatal(err)
	}

	select {
	case <-canceled:
		t.Fatal("the additional data caused the execution to be canceled.")
	case <-time.After(50 * time.Millisecond):
		// No cancellations were observed during execution
	}

	close(release)

	line, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}

	response, err := protocol.DecodeResponse(line)
	if err != nil {
		t.Fatal(err)
	}

	if response.Status != protocol.StatusCompleted {
		t.Fatalf("unexpected response: %+v", response)
	}
}

func TestServerDisconnectedHandlerRetainsConnectionSlot(t *testing.T) {
	config := testConfig()
	config.MaxConnections = 1
	entered := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	handler := func(ctx context.Context, _ protocol.Request) protocol.Response {
		close(entered)
		<-ctx.Done()
		close(canceled)
		<-release // Simulates an executor that has not yet finished
		return protocol.Response{Status: protocol.StatusCompleted}
	}

	addr, _ := startTestServer(t, config, handler)
	first, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if _, err := io.WriteString(first, requestJSON()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("the driver didn't start")
	}
	_ = first.Close()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("the context was not cleared after the system was completely shut down")
	}

	second := send(t, addr, requestJSON())
	close(release)
	if second.Error == nil || second.Error.Code != protocol.CodeServerBusy {
		t.Fatalf("the spot was opened up before the manager finished: %+v", second)
	}
}

func TestServerHalfCloseWriteCancelsRequest(t *testing.T) {
	entered := make(chan struct{})
	canceled := make(chan struct{})
	handler := func(ctx context.Context, _ protocol.Request) protocol.Response {
		close(entered)
		<-ctx.Done()
		close(canceled)
		return errorResponse("", protocol.CodeExecutionError, "request canceled")
	}

	addr, _ := startTestServer(t, testConfig(), handler)
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(conn, requestJSON()); err != nil {
		t.Fatal(err)
	}

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("the server did not initiate the request")
	}

	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		t.Fatal("a TCP connection was expected")
	}
	if err := tcpConn.CloseWrite(); err != nil {
		t.Fatal(err)
	}

	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("CloseWrite no solicitó la cancelación")
	}

	line, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}

	response, err := protocol.DecodeResponse(line)
	if err != nil {
		t.Fatal(err)
	}

	if response.RequestID != "req-1" ||
		response.Status != protocol.StatusError ||
		response.Error == nil ||
		response.Error.Code != protocol.CodeExecutionError {
		t.Fatalf("unexpected response after CloseWrite: %+v", response)
	}
}
