package admission

import (
	"errors"
	"pondus-gateway/internal/domain"
)

var ErrSessionClosed = errors.New("controller session is not active")

// Session represents a controller's exclusive property,
// allowing it to manage its operations as long as it
// remains active.
type Session struct {
	controller *Controller
}

// Submit accepts an operation through the controller's own session.
//
// It rejects the request with ErrSessionClosed if the session is no
// longer valid.
func (session *Session) Submit(operation domain.Operation) (Decision, error) {
	if session == nil || session.controller == nil {
		return Rejected, ErrSessionClosed
	}

	controller := session.controller
	controller.mutex.Lock()
	defer controller.mutex.Unlock()

	if !session.validLocked() {
		return Rejected, ErrSessionClosed
	}
	return controller.submitLocked(operation)
}

// Finish releases an active operation and promotes pending operations
// according to the configured planning strategy.
func (session *Session) Finish(id string) ([]domain.Operation, error) {
	return session.finish(id, true)
}

// FinishWithoutPromotion releases an active operation without promoting
// any new pending operations.
func (session *Session) FinishWithoutPromotion(id string) error {
	_, err := session.finish(id, false)
	return err
}

// CancelPending removes pending operations through the owner session.
//
// Returns ErrSessionClosed if the session is no longer valid.
func (session *Session) CancelPending() ([]domain.Operation, error) {
	if session == nil || session.controller == nil {
		return nil, ErrSessionClosed
	}

	controller := session.controller
	controller.mutex.Lock()
	defer controller.mutex.Unlock()

	if !session.validLocked() {
		return nil, ErrSessionClosed
	}
	return controller.cancelPendingLocked(), nil
}

// Snapshot returns the current state of the controller, even if the session
// is no longer owned. A null session returns zero values.
func (session *Session) Snapshot() (active, queued int, used uint64) {
	if session == nil || session.controller == nil {
		return 0, 0, 0
	}
	return session.controller.Snapshot()
}

// Release releases the exclusive ownership of the controller.
//
// Requires a valid session and that there are no active, pending
// operations or reserved capacity.
func (session *Session) Release() error {
	if session == nil || session.controller == nil {
		return ErrSessionClosed
	}

	controller := session.controller
	controller.mutex.Lock()
	defer controller.mutex.Unlock()

	if !session.validLocked() {
		return ErrSessionClosed
	}

	used, _ := controller.capacity.Snapshot()
	if len(controller.active) != 0 || len(controller.pending) != 0 || used != 0 {
		return ErrControllerNotEmpty
	}

	controller.owner = nil
	return nil
}

// finish completes an operation through the owning
// session, with optional promotion of pending items.
//
// Requires a valid session.
func (session *Session) finish(id string, promote bool) ([]domain.Operation, error) {
	if session == nil || session.controller == nil {
		return nil, ErrSessionClosed
	}

	controller := session.controller
	controller.mutex.Lock()
	defer controller.mutex.Unlock()

	if !session.validLocked() {
		return nil, ErrSessionClosed
	}

	return controller.finishLocked(id, promote)
}

// validLocked verifies that the session retains ownership of the controller.
//
// Requires the controller's mutex to be acquired.
func (session *Session) validLocked() bool {
	return session != nil && session.controller != nil && session.controller.owner == session
}
