package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"pondus-gateway/internal/transport/protocol"
	"sync"
	"time"
)

var (
	ErrLimitsPositive = errors.New("TCP limits must be positive")
	ErrListenerNil    = errors.New("listener nil")
)

// Config defines the connection limits, the maximum message size,
// and the timeouts for read and write operations.
type Config struct {
	MaxMessageBytes int64
	MaxConnections  int
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
}

// A handler processes a protocol request using an
// execution context and returns its response.
type Handler func(context.Context, protocol.Request) protocol.Response

// The server manages TCP connections and delegates
// the processing of requests to the configured handler.
type Server struct {
	config  Config
	handler Handler
}

// Validate checks that the limits for connections,
// messages and timeouts are positive.
func (config Config) Validate() error {
	if config.MaxMessageBytes <= 0 || config.MaxConnections <= 0 || config.ReadTimeout <= 0 || config.WriteTimeout <= 0 {
		return ErrLimitsPositive
	}
	return nil
}

// NewServer creates a server with the validated configuration.
//
// If no Handler is provided, it uses a fallback handler.
func NewServer(config Config, handler Handler) (*Server, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	if handler == nil {
		handler = unavailable
	}

	return &Server{config: config, handler: handler}, nil
}

// Serve accepts and handles TCP connections until the context
// is canceled, the listener is closed or an error occurs.
//
// It limits the number of active connections and rejects excess
// connections when possible.
//
// Before terminating, it waits for the managed connections to close.
func (server *Server) Serve(ctx context.Context, listener net.Listener) error {
	if listener == nil {
		return ErrListenerNil
	}

	var wg sync.WaitGroup
	slots := make(chan struct{}, server.config.MaxConnections)
	busySlots := make(chan struct{}, server.config.MaxConnections)
	stopped := make(chan struct{})

	go func() {
		select {
		case <-ctx.Done():
			_ = listener.Close()
		case <-stopped:
		}
	}()

	defer close(stopped)
	defer wg.Wait()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}

			if errors.Is(err, net.ErrClosed) {
				return nil
			}

			return fmt.Errorf("accept connection: %w", err)
		}
		select {
		case slots <- struct{}{}:
			wg.Go(func() {
				defer func() { <-slots }()
				server.handle(ctx, conn)
			})
		default:
			select {
			case busySlots <- struct{}{}:
				wg.Go(func() {
					defer func() { <-busySlots }()
					defer conn.Close()

					_ = conn.SetWriteDeadline(time.Now().Add(server.config.WriteTimeout))
					_ = protocol.EncodeResponse(conn, errorResponse("", protocol.CodeServerBusy, "connection limit reached"))
				})
			default:
				_ = conn.Close()
			}
		}
	}
}

// unavailable returns an error indicating that request
// processing is not yet available.
func unavailable(_ context.Context, request protocol.Request) protocol.Response {
	return errorResponse(request.RequestID, protocol.CodeExecutionError, "the feature will be available in v0.6.0, stage 6.4")
}

// errorResponse constructs an error response from the protocol
// associated with a request's identifier.
func errorResponse(id string, code string, message string) protocol.Response {
	return protocol.Response{
		RequestID: id,
		Status:    protocol.StatusError,
		Error: &protocol.ResponseError{
			Code:    code,
			Message: message,
		},
	}
}

// handle processes a single JSON request per TCP connection,
// applying the configured size limits and timeouts.
//
// It clears the request context upon detecting that the connection
// is closing and sends a response before closing the connection.
func (server *Server) handle(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Now()) })
	defer stop()

	_ = conn.SetReadDeadline(time.Now().Add(server.config.ReadTimeout))
	reader := bufio.NewReader(conn)
	line, err := bufio.NewReader(io.LimitReader(reader, server.config.MaxMessageBytes+1)).ReadBytes('\n')

	var response protocol.Response
	switch {
	case int64(len(line)) > server.config.MaxMessageBytes:
		response = errorResponse("", protocol.CodeInvalidRequest, "message too large")
	case err != nil:
		response = errorResponse("", protocol.CodeInvalidRequest, "incomplete application or expired review period")
	default:
		request, decodeErr := protocol.DecodeRequest(line)

		if decodeErr != nil {
			code := protocol.CodeInvalidRequest

			if errors.Is(decodeErr, protocol.ErrInvalidOperation) {
				code = protocol.CodeInvalidOperation
			}

			response = errorResponse("", code, "invalid JSON request")
		} else {
			requestCtx, cancel := context.WithCancel(ctx)
			_ = conn.SetReadDeadline(time.Time{})

			go func() {
				_, _ = io.Copy(io.Discard, reader)
				cancel()
			}()

			response = server.handler(requestCtx, request)
			response.RequestID = request.RequestID
		}
	}

	_ = conn.SetWriteDeadline(time.Now().Add(server.config.WriteTimeout))
	_ = protocol.EncodeResponse(conn, response)
}
