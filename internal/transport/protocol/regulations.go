package protocol

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
	StatusCompleted = "completed"
	StatusError     = "error"

	CodeInvalidRequest    = "invalid_request"
	CodeInvalidOperation  = "invalid_operation"
	CodeAdmissionRejected = "admission_rejected"
	CodeServerBusy        = "server_busy"
	CodeExecutionError    = "execution_error"
)

var (
	ErrInvalidRequest   = errors.New("invalid request")
	ErrInvalidOperation = errors.New("invalid operation")
	ErrInvalidResponse  = errors.New("invalid response")
	ErrMoreThanOneJSON  = errors.New("more than one JSON document was found")
)

type Operation struct {
	ID       string `json:"id"`
	ClientID string `json:"client_id"`
	Type     string `json:"type"`
	Cost     uint64 `json:"cost"`
}

type Request struct {
	RequestID  string      `json:"request_id"`
	Operations []Operation `json:"operations"`
}

type Result struct {
	OperationID string  `json:"operation_id"`
	Decision    string  `json:"decision"`
	State       string  `json:"state"`
	Error       *string `json:"error"`
}

type ResponseError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type Response struct {
	RequestID string         `json:"request_id"`
	Status    string         `json:"status"`
	Results   []Result       `json:"results,omitempty"`
	Error     *ResponseError `json:"error,omitempty"`
}

func (operation Operation) Domain() domain.Operation {
	return domain.Operation{
		ID:       operation.ID,
		ClientID: operation.ClientID,
		Type:     operation.Type,
		Cost:     operation.Cost,
	}
}

func (request Request) Validate() error {
	if strings.TrimSpace(request.RequestID) == "" {
		return fmt.Errorf("%w: request_id empty", ErrInvalidRequest)
	}

	if len(request.Operations) == 0 {
		return fmt.Errorf("%w: operations must contain at least one operation", ErrInvalidRequest)
	}

	seen := make(map[string]struct{}, len(request.Operations))
	for index, operation := range request.Operations {
		if err := operation.Domain().Validate(); err != nil {
			return fmt.Errorf(
				"%w: operations[%d]: %v",
				ErrInvalidOperation, index, err,
			)
		}

		if _, exists := seen[operation.ID]; exists {
			return fmt.Errorf(
				"%w: ID duplicated %q",
				ErrInvalidOperation, operation.ID,
			)
		}

		seen[operation.ID] = struct{}{}
	}

	return nil
}

func DecodeRequest(message []byte) (Request, error) {
	var request Request

	if err := decodeOne(message, &request); err != nil {
		return Request{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}

	if err := request.Validate(); err != nil {
		return Request{}, err
	}

	return request, nil
}

func DecodeResponse(message []byte) (Response, error) {
	var response Response

	if err := decodeOne(message, &response); err != nil {
		return Response{}, err
	}

	if err := response.Validate(); err != nil {
		return Response{}, err
	}

	return response, nil
}

func (response Response) Validate() error {
	switch response.Status {
	case StatusCompleted:
		if response.Error != nil {
			return fmt.Errorf("%w: a completed response cannot contain a general error", ErrInvalidResponse)
		}
	case StatusError:
		if response.Error == nil ||
			strings.TrimSpace(response.Error.Code) == "" ||
			strings.TrimSpace(response.Error.Message) == "" {
			return fmt.Errorf("%w: an error response requires a code and message", ErrInvalidResponse)
		}
	default:
		return fmt.Errorf(
			"%w: unknown status %q",
			ErrInvalidResponse, response.Status,
		)
	}
	return nil
}

func EncodeRequest(output io.Writer, request Request) error {
	if err := request.Validate(); err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(request)
}

func EncodeResponse(output io.Writer, response Response) error {
	return json.NewEncoder(output).Encode(response)
}

func decodeOne(message []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(message))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(target); err != nil {
		return err
	}

	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return ErrMoreThanOneJSON
		}
		return err
	}
	return nil
}
