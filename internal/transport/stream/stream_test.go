package stream

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func validSubmitMessage() Message {
	return Message{
		Type:      TypeSubmit,
		RequestID: "request-one",
		Operation: &Operation{
			ID:       "operation-one",
			ClientID: "client-one",
			Type:     "sleep",
			Cost:     2,
		},
	}
}

func TestStreamMessageRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		message Message
	}{
		{
			name:    "submit operation",
			message: validSubmitMessage(),
		},
		{
			name: "admission queued",
			message: Message{
				Type: TypeAdmission, RequestID: "request-one",
				OperationID: "operation-one", Decision: DecisionQueued,
			},
		},
		{
			name: "completed result",
			message: Message{
				Type: TypeResult, RequestID: "request-one",
				OperationID: "operation-one", Decision: DecisionQueued, State: "completed",
			},
		},
		{
			name: "cancelled result",
			message: Message{
				Type: TypeResult, RequestID: "request-one",
				OperationID: "operation-one", Decision: DecisionStarted, State: "cancelled",
				Error: &Failure{Code: "execution_error", Message: "context canceled"},
			},
		},
		{
			name: "connection error without request id",
			message: Message{
				Type: TypeError, Error: &Failure{Code: "server_busy", Message: "connection limit reached"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var encoded bytes.Buffer
			if err := Encode(&encoded, test.message); err != nil {
				t.Fatal(err)
			}

			if !bytes.HasSuffix(encoded.Bytes(), []byte("\n")) {
				t.Fatal("encoded message is missing the line delimiter")
			}

			decoded, err := Decode(encoded.Bytes())
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(decoded, test.message) {
				t.Fatalf("decoded = %+v, want %+v", decoded, test.message)
			}
		})
	}
}

func TestStreamMessageRejectsInvalidFields(t *testing.T) {
	tests := []struct {
		name string
		data string
	}{
		{name: "malformed JSON", data: `{`},
		{name: "unknown type", data: `{"type":"unknown","request_id":"req"}`},
		{name: "unknown field", data: `{"type":"error","error":{"code":"busy","message":"busy"},"unexpected":true}`},
		{name: "extra document", data: `{"type":"error","error":{"code":"busy","message":"busy"}} {}`},
		{name: "missing request id", data: `{"type":"submit","operation":{"id":"op","client_id":"client","type":"sleep","cost":1}}`},
		{name: "missing operation", data: `{"type":"submit","request_id":"req"}`},
		{name: "zero cost", data: `{"type":"submit","request_id":"req","operation":{"id":"op","client_id":"client","type":"sleep","cost":0}}`},
		{name: "unexpected submit state", data: `{"type":"submit","request_id":"req","state":"running","operation":{"id":"op","client_id":"client","type":"sleep","cost":1}}`},
		{name: "unknown admission", data: `{"type":"admission","request_id":"req","operation_id":"op","decision":"approved"}`},
		{name: "result for rejected operation", data: `{"type":"result","request_id":"req","operation_id":"op","decision":"rejected","state":"failed"}`},
		{name: "nonterminal result", data: `{"type":"result","request_id":"req","operation_id":"op","decision":"queued","state":"running"}`},
		{name: "completed result with error", data: `{"type":"result","request_id":"req","operation_id":"op","decision":"started","state":"completed","error":{"code":"execution_error","message":"failed"}}`},
		{name: "error without code", data: `{"type":"error","error":{"code":"","message":"failure"}}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Decode([]byte(test.data))
			if !errors.Is(err, ErrInvalidMessage) {
				t.Fatalf("Decode() error = %v, want ErrInvalidMessage", err)
			}
		})
	}
}

func TestStreamOperationMapping(t *testing.T) {
	operation := validSubmitMessage().Operation.Domain()

	if operation.ID != "operation-one" ||
		operation.ClientID != "client-one" ||
		operation.Type != "sleep" ||
		operation.Cost != 2 {
		t.Fatalf("mapped operation = %+v, want operation-one/client-one/sleep/2", operation)
	}
}

func TestStreamEncodeDoesNotWriteInvalidMessage(t *testing.T) {
	message := validSubmitMessage()
	message.Operation.Cost = 0

	var output bytes.Buffer
	err := Encode(&output, message)

	if !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Encode() error = %v, want ErrInvalidMessage", err)
	}
	if strings.TrimSpace(output.String()) != "" {
		t.Fatalf("Encode() wrote invalid message: %q", output.String())
	}
}
