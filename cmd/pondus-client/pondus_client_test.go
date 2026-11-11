package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"pondus-gateway/internal/transport/protocol"
	"strings"
	"testing"
	"time"
)

func testRequest() protocol.Request {
	return protocol.Request{
		RequestID: "req-1",
		Operations: []protocol.Operation{
			{
				ID:       "op-1",
				ClientID: "client-a",
				Type:     "sleep",
				Cost:     1,
			},
		},
	}
}

func requestFile(t *testing.T, content []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "request.json")
	if err := os.WriteFile(path, content, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func testPeer(t *testing.T, handler func(net.Conn, protocol.Request)) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))

		message, err := bufio.NewReader(conn).ReadBytes('\n')
		if err != nil {
			return
		}

		request, err := protocol.DecodeRequest(message)
		if err != nil {
			return
		}

		handler(conn, request)
	}()

	t.Cleanup(func() {
		_ = listener.Close()
		select {
		case <-done:
		case <-time.After(4 * time.Second):
			t.Error("the test server did not finish")
		}
	})

	return listener.Addr().String()
}

func TestClientRunCompleted(t *testing.T) {
	addr := testPeer(t, func(conn net.Conn, req protocol.Request) {
		_ = protocol.EncodeResponse(conn, protocol.Response{
			RequestID: req.RequestID,
			Status:    protocol.StatusCompleted,
			Results: []protocol.Result{{
				OperationID: "op-1",
				Decision:    "started",
				State:       "completed",
			}},
		})
	})

	var body bytes.Buffer
	if err := protocol.EncodeRequest(&body, testRequest()); err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	if err := run([]string{"-addr", addr, "-file", requestFile(t, body.Bytes()), "-timeout=2s"}, &output); err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{"req-1", "op-1", "admission=started", "state=completed"} {
		if !strings.Contains(output.String(), want) {
			t.Errorf("falta %q en %q", want, output.String())
		}
	}
}

func TestClientRunServerErrorPreservesResults(t *testing.T) {
	addr := testPeer(t, func(conn net.Conn, req protocol.Request) {
		_ = protocol.EncodeResponse(conn, protocol.Response{
			RequestID: req.RequestID,
			Status:    protocol.StatusError,
			Results: []protocol.Result{{
				OperationID: "op-1",
				Decision:    "started",
				State:       "failed",
			}},
			Error: &protocol.ResponseError{
				Code:    protocol.CodeExecutionError,
				Message: "the execution failed",
			},
		})
	})

	var body bytes.Buffer
	_ = protocol.EncodeRequest(&body, testRequest())

	var output bytes.Buffer
	err := run([]string{"-addr=" + addr, "-file=" + requestFile(t, body.Bytes())}, &output)
	if err == nil || !strings.Contains(err.Error(), protocol.CodeExecutionError) {
		t.Fatalf("error = %v", err)
	}

	if !strings.Contains(output.String(), "op-1") || !strings.Contains(output.String(), "state=failed") {
		t.Fatalf("partial results were lost: %s", output.String())
	}
}

func TestClientRejectsInvalidFileAndFlags(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"missing file", nil},
		{"negative timeout", []string{"-file=a", "-timeout=0s"}},
		{"unknown flag", []string{"-what=1"}},
		{"extra positional", []string{"-file=a", "extra"}},
		{"missing path", []string{"-file=missing.json"}},
		{"invalid JSON", []string{"-file=" + requestFile(t, []byte("{invalid"))}},
		{"empty operations", []string{"-file=" + requestFile(t, []byte(`{"request_id":"r","operations":[]}`))}},
		{"oversized input", []string{"-file=" + requestFile(t, bytes.Repeat([]byte("x"), maxRequestBytes+1))}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := run(tt.args, &bytes.Buffer{}); err == nil {
				t.Fatal("an error was expected")
			}
		})
	}
}

func TestExchangeRejectsInvalidResponses(t *testing.T) {
	tests := []struct {
		name     string
		response string
	}{
		{"wrong request id", `{"request_id":"other","status":"completed"}` + "\n"},
		{"malformed JSON", "not json\n"},
		{"missing code", `{"request_id":"req-1","status":"error","error":{"message":"oops"}}` + "\n"},
		{"unknown status", `{"request_id":"req-1","status":"processing"}` + "\n"},
		{"multiple documents", `{"request_id":"req-1","status":"completed"} {"status":"error"}` + "\n"},
		{"completed with error", `{"request_id":"req-1","status":"completed","error":{"code":"x","message":"err"}}` + "\n"},
		{"oversized response", strings.Repeat("x", maxResponseBytes+1) + "\n"},
		{"incomplete response", `{"request_id":"req-1","status":"completed"}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			addr := testPeer(t, func(conn net.Conn, _ protocol.Request) {
				_, _ = conn.Write([]byte(test.response))
			})

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			if _, err := exchange(ctx, addr, testRequest()); err == nil {
				t.Fatal("a protocol error was expected")
			}
		})
	}
}

func TestExchangeTimeout(t *testing.T) {
	addr := testPeer(t, func(conn net.Conn, _ protocol.Request) {
		// No response is written; the connection remains open until the client's context times out
		var buf [1]byte
		_, _ = conn.Read(buf[:])
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := exchange(ctx, addr, testRequest())
	if err == nil {
		t.Fatal("a timeout was expected")
	}
}

func TestExchangeAcceptsBusyWithoutRequestID(t *testing.T) {
	addr := testPeer(t, func(conn net.Conn, _ protocol.Request) {
		_ = protocol.EncodeResponse(conn, protocol.Response{
			Status: protocol.StatusError,
			Error: &protocol.ResponseError{
				Code:    protocol.CodeServerBusy,
				Message: "saturated",
			},
		})
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := exchange(ctx, addr, testRequest())
	if err != nil || resp.Error == nil || resp.Error.Code != protocol.CodeServerBusy {
		t.Fatalf("response=%+v err=%v", resp, err)
	}
}

func TestExchangeCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := exchange(ctx, "127.0.0.1:1", testRequest())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}
