package stream

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"pondus-gateway/internal/domain"
	"strings"
)

const (
	TypeSubmit       = "submit"
	TypeAdmission    = "admission"
	TypeResult       = "result"
	TypeError        = "error"
	DecisionStarted  = "started"
	DecisionQueued   = "queued"
	DecisionRejected = "rejected"
)

var ErrInvalidMessage = errors.New("invalid stream message")

// Operation represents an operation in the JSON
// format of the stream protocol.
type Operation struct {
	ID       string `json:"id"`
	ClientID string `json:"client_id"`
	Type     string `json:"type"`
	Cost     uint64 `json:"cost"`
}

// Failure describes a protocol error using
// an identifiable code and a message.
type Failure struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Message represents a message from the stream protocol.
//
// Its allowed and required fields depend on the message type.
type Message struct {
	Type        string     `json:"type"`
	RequestID   string     `json:"request_id,omitempty"`
	Operation   *Operation `json:"operation,omitempty"`
	OperationID string     `json:"operation_id,omitempty"`
	Decision    string     `json:"decision,omitempty"`
	State       string     `json:"state,omitempty"`
	Error       *Failure   `json:"error,omitempty"`
}

// Domain converts the protocol operation to its domain
// representation without performing any validations.
func (operation Operation) Domain() domain.Operation {
	return domain.Operation{
		ID: operation.ID, ClientID: operation.ClientID,
		Type: operation.Type, Cost: operation.Cost,
	}
}

// Validate checks the structure and consistency of the fields
// based on the message type.
//
// Requires valid admission decisions and terminal states for
// the execution results.
func (message Message) Validate() error {
	invalid := func(reason string) error {
		return fmt.Errorf("%w: %s", ErrInvalidMessage, reason)
	}

	if message.Type != TypeError && strings.TrimSpace(message.RequestID) == "" {
		return invalid("request_id required")
	}

	switch message.Type {
	case TypeSubmit:
		if message.Operation == nil ||
			message.OperationID != "" ||
			message.Decision != "" ||
			message.State != "" ||
			message.Error != nil {
			return invalid("invalid submit fields")
		}

		if err := message.Operation.Domain().Validate(); err != nil {
			return fmt.Errorf("%w: operation: %v", ErrInvalidMessage, err)
		}

	case TypeAdmission:
		if message.Operation != nil ||
			strings.TrimSpace(message.OperationID) == "" ||
			message.State != "" ||
			message.Error != nil {
			return invalid("invalid admission fields")
		}

		if message.Decision != DecisionStarted && message.Decision != DecisionQueued && message.Decision != DecisionRejected {
			return invalid("invalid admission decision")
		}

	case TypeResult:
		if message.Operation != nil || strings.TrimSpace(message.OperationID) == "" {
			return invalid("invalid result fields")
		}

		if message.Decision != DecisionStarted && message.Decision != DecisionQueued {
			return invalid("result requires accepted decision")
		}

		if message.State != string(domain.StateCompleted) &&
			message.State != string(domain.StateFailed) &&
			message.State != string(domain.StateCancelled) {
			return invalid("invalid terminal state")
		}

		if message.Error != nil && !message.Error.valid() {
			return invalid("incomplete result error")
		}

		if message.State == string(domain.StateCompleted) && message.Error != nil {
			return invalid("completed result cannot have error")
		}

	case TypeError:
		if message.Operation != nil ||
			message.OperationID != "" ||
			message.Decision != "" ||
			message.State != "" ||
			message.Error == nil ||
			!message.Error.valid() {
			return invalid("invalid error fields")
		}

	default:
		return invalid("unknown message type")
	}

	return nil
}

// valid checks that the error contains a non-empty code and message.
func (failure Failure) valid() bool {
	return strings.TrimSpace(failure.Code) != "" && strings.TrimSpace(failure.Message) != ""
}

// Encode validates and writes a JSON message using
// the stream protocol.
//
// It does not write the message if validation fails.
func Encode(output io.Writer, message Message) error {
	if err := message.Validate(); err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(message)
}

// Decode decodes and validates a single JSON message
// from the stream protocol.
//
// Rejects unknown fields and additional JSON documents.
func Decode(data []byte) (Message, error) {
	var message Message
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&message); err != nil {
		return Message{}, fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}

	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return Message{}, fmt.Errorf("%w: multiple JSON documents", ErrInvalidMessage)
		}

		return Message{}, fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}

	if err := message.Validate(); err != nil {
		return Message{}, err
	}

	return message, nil
}
