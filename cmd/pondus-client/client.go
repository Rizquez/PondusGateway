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
	"pondus-gateway/internal/transport/protocol"
)

const (
	maxRequestBytes  = 64 * 1024
	maxResponseBytes = 4 * 1024 * 1024
)

var (
	ErrResponse = errors.New("the completed response contains a general error")
	ErrCode     = errors.New("error response without a code")
)

// loadRequest loads and validates a JSON request from a file.
//
// It rejects requests that exceed the maximum allowed size.
func loadRequest(path string) (protocol.Request, error) {
	file, err := os.Open(path)

	if err != nil {
		return protocol.Request{}, fmt.Errorf("open request: %w", err)
	}

	defer file.Close()

	message, err := io.ReadAll(io.LimitReader(file, maxRequestBytes+1))
	if err != nil {
		return protocol.Request{}, fmt.Errorf("read request: %w", err)
	}

	if len(message) > maxRequestBytes {
		return protocol.Request{}, fmt.Errorf("the request exceeds %d bytes", maxRequestBytes)
	}

	request, err := protocol.DecodeRequest(message)
	if err != nil {
		return protocol.Request{}, fmt.Errorf("invalid request: %w", err)
	}

	return request, nil
}

// exchange sends a request to the TCP server and receives its response,
// respecting cancellation and context timeouts.
//
// Limits the size of messages and verifies that identifiers match, except
// in server busy responses without an ID.
func exchange(ctx context.Context, address string, request protocol.Request) (protocol.Response, error) {
	var message bytes.Buffer
	if err := protocol.EncodeRequest(&message, request); err != nil {
		return protocol.Response{}, err
	}

	if message.Len() > maxRequestBytes {
		return protocol.Response{}, fmt.Errorf("the encoded request exceeds %d bytes", maxRequestBytes)
	}

	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return protocol.Response{}, fmt.Errorf("connect to the server: %w", err)
	}
	defer conn.Close()

	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			return protocol.Response{}, err
		}
	}

	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	if _, err := io.Copy(conn, &message); err != nil {
		return protocol.Response{}, fmt.Errorf("send request: %w", err)
	}

	line, err := bufio.NewReader(io.LimitReader(conn, maxResponseBytes+1)).ReadBytes('\n')
	if len(line) > maxResponseBytes {
		return protocol.Response{}, fmt.Errorf("the response exceeds %d bytes", maxResponseBytes)
	}
	if err != nil {
		return protocol.Response{}, fmt.Errorf("read the full answer: %w", err)
	}

	response, err := protocol.DecodeResponse(line)
	if err != nil {
		return protocol.Response{}, fmt.Errorf("respuesta JSON inválida: %w", err)
	}

	busyWithoutID := response.RequestID == "" &&
		response.Status == protocol.StatusError &&
		response.Error != nil &&
		response.Error.Code == protocol.CodeServerBusy

	if response.RequestID != request.RequestID && !busyWithoutID {
		return protocol.Response{}, fmt.Errorf(
			"request_id does not match: received %q, expected %q",
			response.RequestID, request.RequestID,
		)
	}

	switch response.Status {
	case protocol.StatusCompleted:
		if response.Error != nil {
			return protocol.Response{}, ErrResponse
		}
	case protocol.StatusError:
		if response.Error == nil || response.Error.Code == "" {
			return protocol.Response{}, ErrCode
		}
	default:
		return protocol.Response{}, fmt.Errorf("unknown response status: %q", response.Status)
	}

	return response, nil
}

// printResults displays the overall status of the request and the
// individual results of the received operations.
func printResults(output io.Writer, response protocol.Response) {
	fmt.Fprintf(
		output,
		"Request: %s | status=%s\n",
		response.RequestID, response.Status,
	)

	for _, result := range response.Results {
		value := "<nil>"
		if result.Error != nil {
			value = *result.Error
		}

		fmt.Fprintf(
			output,
			"%s | admission=%s | state=%s | error=%s\n",
			result.OperationID, result.Decision, result.State, value,
		)
	}
}
