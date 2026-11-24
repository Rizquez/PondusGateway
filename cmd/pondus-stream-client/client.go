package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"pondus-gateway/internal/transport/stream"
)

const maxRequestFileBytes = 4 << 20

// requestProgress tracks the status of a request, from the time
// it is accepted until a final response is received.
type requestProgress struct {
	operationID string
	decision    string
	admitted    bool
	finished    bool
}

// loadRequests loads and validates a batch of JSON Lines requests.
//
// It requires submit messages with unique identifiers and checks
// the file size and message size limits.
func loadRequests(filePath string, maxMessageBytes int) ([]stream.Message, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("open request file: %w", err)
	}
	defer file.Close()

	data, err := io.ReadAll(io.LimitReader(file, maxRequestFileBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read request file: %w", err)
	}

	if len(data) > maxRequestFileBytes {
		return nil, fmt.Errorf("request file exceeds %d bytes", maxRequestFileBytes)
	}

	var requests []stream.Message
	requestIDs := make(map[string]struct{})
	operationIDs := make(map[string]struct{})

	lines := bytes.Split(data, []byte{'\n'})
	for lineIndex, line := range lines {
		if lineIndex == len(lines)-1 && len(line) == 0 {
			continue
		}

		line = bytes.TrimSuffix(line, []byte{'\r'})
		if len(line) == 0 || len(line)+1 > maxMessageBytes {
			return nil, fmt.Errorf("line %d: empty or oversized message", lineIndex+1)
		}

		request, err := stream.Decode(line)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", lineIndex+1, err)
		}

		if request.Type != stream.TypeSubmit {
			return nil, fmt.Errorf("line %d: expected submit, got %q", lineIndex+1, request.Type)
		}

		if _, exists := requestIDs[request.RequestID]; exists {
			return nil, fmt.Errorf("line %d: duplicate request_id %q", lineIndex+1, request.RequestID)
		}

		if _, exists := operationIDs[request.Operation.ID]; exists {
			return nil, fmt.Errorf("line %d: duplicate operation id %q", lineIndex+1, request.Operation.ID)
		}

		requestIDs[request.RequestID] = struct{}{}
		operationIDs[request.Operation.ID] = struct{}{}
		requests = append(requests, request)
	}

	if len(requests) == 0 {
		return nil, errors.New("request file contains no submit messages")
	}

	return requests, nil
}

// exchange sends a batch of requests over a persistent TCP connection
// and processes their responses until all requests are completed.
//
// It limits the number of concurrent requests using a send window and
// respects context cancellation.
func exchange(ctx context.Context, config clientConfig, requests []stream.Message, output io.Writer) error {
	connection, err := (&net.Dialer{}).DialContext(ctx, "tcp", config.address)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer connection.Close()

	stopOnCancellation := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stopOnCancellation()

	progress := make(map[string]*requestProgress, len(requests))
	for _, request := range requests {
		progress[request.RequestID] = &requestProgress{operationID: request.Operation.ID}
	}

	slots := make(chan struct{}, config.window)
	writerDone := make(chan error, 1)
	go sendRequests(ctx, connection, requests, slots, writerDone)

	reader := bufio.NewReader(connection)
	for completed := 0; completed < len(requests); {
		line, readError := readLine(reader, config.maxMessageBytes)
		if readError != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			select {
			case sendError := <-writerDone:
				if sendError != nil {
					return fmt.Errorf("send requests: %w", sendError)
				}
			default:
			}

			return fmt.Errorf("read response: %w", readError)
		}

		response, decodeError := stream.Decode(line)
		if decodeError != nil {
			return fmt.Errorf("decode response: %w", decodeError)
		}

		finished, processError := processResponse(response, progress, output)
		if processError != nil {
			return processError
		}

		if finished {
			completed++
			<-slots
		}
	}

	if sendError := <-writerDone; sendError != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		return fmt.Errorf("send requests: %w", sendError)
	}
	return nil
}

// sendRequests transmits requests while adhering to the transmission window.
//
// It waits for available capacity before each transmission and reports its
// completion or the first error encountered.
func sendRequests(ctx context.Context, connection net.Conn, requests []stream.Message, slots chan<- struct{}, done chan<- error) {
	for _, request := range requests {
		select {
		case <-ctx.Done():
			done <- ctx.Err()
			return
		case slots <- struct{}{}:
		}

		if err := stream.Encode(connection, request); err != nil {
			_ = connection.Close()
			done <- err
			return
		}
	}
	done <- nil
}

// processResponse validates and logs the progress of a request.
//
// Checks that identifiers match and verifies the sequence
// of input and output, preventing duplicate terminal responses.
func processResponse(response stream.Message, progress map[string]*requestProgress, output io.Writer) (bool, error) {
	request, exists := progress[response.RequestID]
	if !exists {
		return false, fmt.Errorf("unexpected response for request_id %q", response.RequestID)
	}

	if request.finished {
		return false, fmt.Errorf("duplicate terminal response for request_id %q", response.RequestID)
	}

	switch response.Type {
	case stream.TypeAdmission:
		if request.admitted || response.OperationID != request.operationID {
			return false, fmt.Errorf(
				"invalid admission for request_id %q: operation_id=%q",
				response.RequestID, response.OperationID,
			)
		}

		request.admitted = true
		request.decision = response.Decision

		if _, err := fmt.Fprintf(
			output,
			"admission request_id=%s operation_id=%s decision=%s\n",
			response.RequestID, response.OperationID, response.Decision,
		); err != nil {
			return false, err
		}

		if response.Decision == stream.DecisionRejected {
			request.finished = true
			return true, nil
		}

		return false, nil

	case stream.TypeResult:
		if !request.admitted ||
			request.decision == stream.DecisionRejected ||
			response.OperationID != request.operationID ||
			response.Decision != request.decision {
			return false, fmt.Errorf("unexpected result for request_id %q", response.RequestID)
		}

		request.finished = true
		if response.Error != nil {
			_, err := fmt.Fprintf(
				output,
				"result request_id=%s operation_id=%s state=%s error=%s: %s\n",
				response.RequestID, response.OperationID, response.State, response.Error.Code, response.Error.Message,
			)
			return true, err
		}

		_, err := fmt.Fprintf(
			output,
			"result request_id=%s operation_id=%s state=%s\n",
			response.RequestID, response.OperationID, response.State,
		)
		return true, err

	case stream.TypeError:
		request.finished = true
		_, err := fmt.Fprintf(
			output,
			"error request_id=%s code=%s message=%s\n",
			response.RequestID, response.Error.Code, response.Error.Message,
		)
		return true, err

	default:
		return false, fmt.Errorf("unexpected message type %q", response.Type)
	}
}

// readLine reads a message delimited by a line break,
// allowing fragments larger than the internal buffer.
//
// It rejects responses that exceed the maximum size,
// including the delimiter.
func readLine(reader *bufio.Reader, maxBytes int) ([]byte, error) {
	var message []byte
	for {
		fragment, err := reader.ReadSlice('\n')
		if len(message)+len(fragment) > maxBytes {
			return nil, fmt.Errorf("response exceeds %d bytes", maxBytes)
		}

		message = append(message, fragment...)
		if err == nil {
			return message, nil
		}

		if !errors.Is(err, bufio.ErrBufferFull) {
			return nil, err
		}
	}
}
