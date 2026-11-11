package protocol

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func validRequest() Request {
	return Request{
		RequestID: "req-1",
		Operations: []Operation{
			{ID: "op-1", ClientID: "a", Type: "sleep", Cost: 2},
			{ID: "op-2", ClientID: "b", Type: "sleep", Cost: 1},
		},
	}
}

func TestRequestValidate(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Request)
		want error
	}{
		{"empty request ID", func(request *Request) { request.RequestID = "  " }, ErrInvalidRequest},
		{"empty batch", func(request *Request) { request.Operations = nil }, ErrInvalidRequest},
		{"empty operation ID", func(request *Request) { request.Operations[0].ID = " " }, ErrInvalidOperation},
		{"empty client ID", func(request *Request) { request.Operations[0].ClientID = "" }, ErrInvalidOperation},
		{"empty operation type", func(request *Request) { request.Operations[0].Type = "" }, ErrInvalidOperation},
		{"zero cost", func(request *Request) { request.Operations[0].Cost = 0 }, ErrInvalidOperation},
		{"duplicate ID", func(request *Request) { request.Operations[1].ID = "op-1" }, ErrInvalidOperation},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := validRequest()
			test.edit(&request)
			if err := request.Validate(); !errors.Is(err, test.want) {
				t.Fatalf(
					"Validate() = %v; want %v",
					err, test.want,
				)
			}
		})
	}

	if err := validRequest().Validate(); err != nil {
		t.Fatalf("valid request: %v", err)
	}
}

func TestRequestJSONRoundTrip(t *testing.T) {
	request := validRequest()
	var output bytes.Buffer
	if err := EncodeRequest(&output, request); err != nil {
		t.Fatal(err)
	}
	if !bytes.HasSuffix(output.Bytes(), []byte("\n")) {
		t.Fatal("missing newline delimiter")
	}

	decoded, err := DecodeRequest(output.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, request) {
		t.Fatalf("decoded = %#v; want %#v", decoded, request)
	}
}

func TestDecodeRequestInvalid(t *testing.T) {
	tests := []struct {
		name, raw string
		want      error
	}{
		{"malformed", `{`, ErrInvalidRequest},
		{"unknown field", `{"request_id":"r","operations":[{"id":"a","client_id":"b","type":"sleep","cost":1,"typo":1}]}`, ErrInvalidRequest},
		{"extra document", `{"request_id":"r","operations":[{"id":"a","client_id":"b","type":"sleep","cost":1}]} {}`, ErrInvalidRequest},
		{"invalid cost", `{"request_id":"r","operations":[{"id":"a","client_id":"b","type":"sleep","cost":0}]}`, ErrInvalidOperation},
		{"negative cost", `{"request_id":"r","operations":[{"id":"a","client_id":"b","type":"sleep","cost":-1}]}`, ErrInvalidRequest},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := DecodeRequest([]byte(test.raw))
			if !errors.Is(err, test.want) {
				t.Fatalf(
					"error = %v; want %v",
					err, test.want,
				)
			}
		})
	}
}

func TestResponseJSONRoundTrip(t *testing.T) {
	failure := "context deadline exceeded"
	response := Response{
		RequestID: "req-1", Status: StatusCompleted,
		Results: []Result{
			{OperationID: "op-1", Decision: "started", State: "completed"},
			{OperationID: "op-2", Decision: "queued", State: "failed", Error: &failure},
		},
	}
	var output bytes.Buffer

	if err := EncodeResponse(&output, response); err != nil {
		t.Fatal(err)
	}

	if !strings.HasSuffix(output.String(), "\n") {
		t.Fatal("missing newline delimiter")
	}

	decoded, err := DecodeResponse(output.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, response) {
		t.Fatalf("decoded = %#v; want %#v", decoded, response)
	}

	serverError := Response{
		RequestID: "req-2",
		Status:    StatusError,
		Error: &ResponseError{
			Code:    CodeServerBusy,
			Message: "server busy",
		},
	}

	output.Reset()
	if err := EncodeResponse(&output, serverError); err != nil {
		t.Fatal(err)
	}

	decoded, err = DecodeResponse(output.Bytes())
	if err != nil || !reflect.DeepEqual(decoded, serverError) {
		t.Fatalf(
			"decoded = %#v; err = %v",
			decoded, err,
		)
	}
}

func TestCoreOperationMapping(t *testing.T) {
	got := validRequest().Operations[0].Domain()
	if got.ID != "op-1" || got.ClientID != "a" || got.Type != "sleep" || got.Cost != 2 {
		t.Fatalf("unexpected core operation: %+v", got)
	}
}

func TestDecodeResponseRejectsInconsistentContract(t *testing.T) {
	tests := []struct {
		name    string
		message string
	}{
		{
			name:    "unknown status",
			message: `{"request_id":"req-1","status":"processing"}`,
		},
		{
			name:    "completed with general error",
			message: `{"request_id":"req-1","status":"completed","error":{"code":"execution_error","message":"failure"}}`,
		},
		{
			name:    "error status without error object",
			message: `{"request_id":"req-1","status":"error"}`,
		},
		{
			name:    "error status without code",
			message: `{"request_id":"req-1","status":"error","error":{"message":"failure"}}`,
		},
		{
			name:    "error status with empty message",
			message: `{"request_id":"req-1","status":"error","error":{"code":"execution_error","message":""}}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if response, err := DecodeResponse([]byte(test.message)); err == nil {
				t.Fatalf("DecodeResponse() accepted inconsistent response: %+v", response)
			}
		})
	}
}

func TestDecodeResponseAllowsErrorWithPartialResults(t *testing.T) {
	message := []byte(`{"request_id":"req-1","status":"error","results":[{"operation_id":"op-1","decision":"started","state":"failed","error":"canceled"}],"error":{"code":"execution_error","message":"execution interrupted"}}`)
	response, err := DecodeResponse(message)
	if err != nil {
		t.Fatalf("DecodeResponse() = %v", err)
	}
	if response.Status != StatusError || len(response.Results) != 1 {
		t.Fatalf("unexpected response: %+v", response)
	}
}

func TestDecodeResponseAllowsBusyWithoutRequestID(t *testing.T) {
	message := []byte(`{"status":"error","error":{"code":"server_busy","message":"connection limit reached"}}`)
	response, err := DecodeResponse(message)
	if err != nil {
		t.Fatalf("DecodeResponse() = %v", err)
	}
	if response.RequestID != "" || response.Error == nil || response.Error.Code != CodeServerBusy {
		t.Fatalf("unexpected response: %+v", response)
	}
}
