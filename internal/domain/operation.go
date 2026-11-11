package domain

import (
	"errors"
	"strings"
)

var (
	ErrOperationID   = errors.New("operation ID cannot be empty")
	ErrClientID      = errors.New("client ID cannot be empty")
	ErrOperationType = errors.New("operation type cannot be empty")
	ErrOperationCost = errors.New("operation cost must be greater than zero")
)

// An Operation represents a unit of work
// with an associated capacity cost.
type Operation struct {
	ID       string
	ClientID string
	Type     string
	Cost     uint64
}

// Validate checks that the identifiers and type
// are not empty and that the cost is greater than zero.
func (operation Operation) Validate() error {
	switch {
	case strings.TrimSpace(operation.ID) == "":
		return ErrOperationID

	case strings.TrimSpace(operation.ClientID) == "":
		return ErrClientID

	case strings.TrimSpace(operation.Type) == "":
		return ErrOperationType

	case operation.Cost == 0:
		return ErrOperationCost

	default:
		return nil
	}
}
