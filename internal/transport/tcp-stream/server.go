package tcpstream

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"pondus-gateway/internal/control/admission"
	"pondus-gateway/internal/domain"
	"pondus-gateway/internal/execution/continuous"
	"pondus-gateway/internal/transport/stream"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrInvalidConfig = errors.New("invalid TCP stream configuration")
	ErrNilEngine     = errors.New("stream engine is required")
	ErrServing       = errors.New("TCP stream server is already serving")
	ErrListener      = errors.New("listener is required")
)

// Engine defines the contract for accepting and canceling
// operations associated with a connection, the results of
// which are received asynchronously.
type Engine interface {
	Submit(ownerID string, operation domain.Operation) (admission.Decision, <-chan continuous.Result, error)
	CancelOwner(ownerID string) error
}

// Config defines the limits for connections, messages
// and pending responses, as well as the read and write
// timeouts.
type Config struct {
	MaxConnections  int
	MaxMessageBytes int
	OutgoingQueue   int
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
}

// The server manages persistent TCP connections and delegates
// the execution of operations to the configured engine.
//
// The server can only be started once.
type Server struct {
	config  Config
	engine  Engine
	serving atomic.Bool
	nextID  atomic.Uint64
}

// outbound represents a message waiting to be sent and, optionally,
// the request that should be marked as complete after it is sent.
type outbound struct {
	message           stream.Message
	finishedRequestID string
}

// peer maintains the context and state of a TCP connection,
// including active requests and their response queue.
type peer struct {
	server     *Server
	connection net.Conn
	ownerID    string
	ctx        context.Context
	cancel     context.CancelFunc
	messages   chan outbound
	mutex      sync.Mutex
	inflight   map[string]struct{}
}

// Validate checks the server limits and timeouts.
//
// Allows you to disable the read timeout by setting
// the value to zero.
func (config Config) Validate() error {
	if config.MaxConnections <= 0 || config.MaxMessageBytes <= 0 || config.OutgoingQueue <= 0 ||
		config.ReadTimeout < 0 || config.WriteTimeout <= 0 {
		return ErrInvalidConfig
	}
	return nil
}

// New creates a TCP server with the validated configuration.
//
// Requires a non-null Engine.
func New(config Config, engine Engine) (*Server, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	if engine == nil {
		return nil, ErrNilEngine
	}

	return &Server{config: config, engine: engine}, nil
}

// Serve accepts TCP connections until the context is canceled
// or the listener is closed, while adhering to the connection
// limit.
//
// It rejects any excess connections and waits for the handlers
// to finish before returning.
func (server *Server) Serve(ctx context.Context, listener net.Listener) error {
	if !server.serving.CompareAndSwap(false, true) {
		return ErrServing
	}

	if listener == nil {
		return ErrListener
	}

	connections := make(map[net.Conn]struct{})
	var connectionsMutex sync.Mutex
	var handlers sync.WaitGroup
	capacity := make(chan struct{}, server.config.MaxConnections)
	stopped := make(chan struct{})

	go func() {
		select {
		case <-ctx.Done():
			_ = listener.Close()
		case <-stopped:
		}
	}()
	defer close(stopped)

	var acceptError error
	for {
		connection, err := listener.Accept()

		if err != nil {
			if ctx.Err() == nil && !errors.Is(err, net.ErrClosed) {
				acceptError = err
			}
			break
		}

		if ctx.Err() != nil {
			_ = connection.Close()
			break
		}

		select {
		case capacity <- struct{}{}:
			connectionsMutex.Lock()
			connections[connection] = struct{}{}
			connectionsMutex.Unlock()

			handlers.Go(func() {
				defer func() {
					_ = connection.Close()
					connectionsMutex.Lock()
					delete(connections, connection)
					connectionsMutex.Unlock()
					<-capacity
				}()
				server.handle(ctx, connection)
			})
		default:
			_ = connection.Close()
		}
	}

	connectionsMutex.Lock()
	for connection := range connections {
		_ = connection.Close()
	}

	connectionsMutex.Unlock()
	handlers.Wait()
	return acceptError
}

// handle manages the lifecycle of a connection and its responses.
//
// Assigns exclusive ownership of the connection and requests that
// its operations be canceled upon disconnection, before waiting for
// the writer.
func (server *Server) handle(ctx context.Context, connection net.Conn) {
	connectionCtx, cancel := context.WithCancel(ctx)
	connectionOwner := "tcp-" + strconv.FormatUint(server.nextID.Add(1), 10)
	observer := &peer{
		server:     server,
		connection: connection,
		ownerID:    connectionOwner,
		ctx:        connectionCtx,
		cancel:     cancel,
		messages:   make(chan outbound, server.config.OutgoingQueue),
		inflight:   make(map[string]struct{}),
	}

	var writer sync.WaitGroup
	writer.Go(func() {
		observer.writeResponses()
	})

	observer.readRequests()
	cancel()
	_ = connection.Close()
	_ = server.engine.CancelOwner(connectionOwner)
	writer.Wait()
}

// enqueue attempts to add a response to the outbound
// queue without blocking.
//
// If the queue is full, it cancels the context and
// closes the connection to prevent a backlog of responses.
func (peer *peer) enqueue(response outbound) bool {
	select {
	case <-peer.ctx.Done():
		return false
	default:
	}

	select {
	case <-peer.ctx.Done():
		return false
	case peer.messages <- response:
		return true
	default:
		peer.cancel()
		_ = peer.connection.Close()
		return false
	}
}

// writeResponses sequentially sends the pending messages,
// applying the configured write timeout.
//
// It releases the request identifiers only after successfully
// sending its final response.
func (peer *peer) writeResponses() {
	for {
		select {
		case <-peer.ctx.Done():
			return
		case response := <-peer.messages:
			if err := peer.connection.SetWriteDeadline(time.Now().Add(peer.server.config.WriteTimeout)); err != nil {
				peer.cancel()
				_ = peer.connection.Close()
				return
			}

			if response.finishedRequestID != "" {
				peer.mutex.Lock()
			}

			if err := stream.Encode(peer.connection, response.message); err != nil {
				if response.finishedRequestID != "" {
					peer.mutex.Unlock()
				}

				peer.cancel()
				_ = peer.connection.Close()
				return
			}

			if response.finishedRequestID != "" {
				delete(peer.inflight, response.finishedRequestID)
				peer.mutex.Unlock()
			}
		}
	}
}

// readRequests processes successive requests from
// the same connection, validating the protocol and
// preventing duplicate concurrent identifiers.
//
// It immediately acknowledges receipt and delegates
// the sending of the final results to separate goroutines.
func (peer *peer) readRequests() {
	reader := bufio.NewReader(peer.connection)
	for {
		var readDeadline time.Time
		if peer.server.config.ReadTimeout > 0 {
			readDeadline = time.Now().Add(peer.server.config.ReadTimeout)
		}

		if err := peer.connection.SetReadDeadline(readDeadline); err != nil {
			return
		}

		line, err := readLine(reader, peer.server.config.MaxMessageBytes)
		if err != nil {
			return
		}

		request, err := stream.Decode(line)
		if err != nil || request.Type != stream.TypeSubmit {
			if !peer.enqueue(outbound{message: protocolError("", "invalid_request", "expected a valid submit message")}) {
				return
			}
			continue
		}

		if !peer.reserveRequestID(request.RequestID) {
			if !peer.enqueue(outbound{message: protocolError("", "duplicate_request", "request_id already in flight")}) {
				return
			}
			continue
		}

		decision, resultChannel, submitError := peer.server.engine.Submit(peer.ownerID, request.Operation.Domain())
		if submitError != nil {
			if !peer.enqueue(outbound{
				message:           protocolError(request.RequestID, "admission_error", submitError.Error()),
				finishedRequestID: request.RequestID,
			}) {
				return
			}
			continue
		}

		admissionMessage := stream.Message{
			Type:        stream.TypeAdmission,
			RequestID:   request.RequestID,
			OperationID: request.Operation.ID,
			Decision:    string(decision),
		}

		if decision == admission.Rejected {
			if !peer.enqueue(outbound{message: admissionMessage, finishedRequestID: request.RequestID}) {
				return
			}
			continue
		}

		if !peer.enqueue(outbound{message: admissionMessage}) {
			return
		}

		go peer.forwardResult(request.RequestID, request.Operation.ID, resultChannel)
	}
}

// reserveRequestID registers a request ID if it is not already
// active.
//
// Protects the shared register using the connection's mutex.
func (peer *peer) reserveRequestID(requestID string) bool {
	peer.mutex.Lock()
	defer peer.mutex.Unlock()

	if _, exists := peer.inflight[requestID]; exists {
		return false
	}

	peer.inflight[requestID] = struct{}{}
	return true
}

// forwardResult waits for the result of a supported operation
// and adds it to the response queue.
//
// It stops waiting if the connection is canceled and reports
// a protocol error if the channel is closed without a result.
func (peer *peer) forwardResult(requestID string, operationID string, results <-chan continuous.Result) {
	select {
	case <-peer.ctx.Done():
		return
	case result, open := <-results:
		if !open {
			peer.enqueue(outbound{
				message:           protocolError(requestID, "execution_error", "missing operation result"),
				finishedRequestID: requestID,
			})
			return
		}

		resultMessage := stream.Message{
			Type:        stream.TypeResult,
			RequestID:   requestID,
			OperationID: operationID,
			Decision:    string(result.Decision),
			State:       string(result.State),
		}

		if result.Err != nil {
			resultMessage.Error = &stream.Failure{
				Code:    "execution_error",
				Message: result.Err.Error(),
			}
		}

		peer.enqueue(outbound{
			message:           resultMessage,
			finishedRequestID: requestID,
		})
	}
}

// protocolError constructs an error message from the stream protocol
// associated with a request's identifier.
func protocolError(requestID string, code string, message string) stream.Message {
	return stream.Message{
		Type:      stream.TypeError,
		RequestID: requestID,
		Error: &stream.Failure{
			Code:    code,
			Message: message,
		},
	}
}

// readLine reads a message delimited by a line break,
// allowing fragments larger than the internal buffer.
//
// It rejects messages that exceed the maximum size,
// including the delimiter.
func readLine(reader *bufio.Reader, maxBytes int) ([]byte, error) {
	var message []byte
	for {
		fragment, err := reader.ReadSlice('\n')
		if len(message)+len(fragment) > maxBytes {
			return nil, fmt.Errorf("message too large")
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
