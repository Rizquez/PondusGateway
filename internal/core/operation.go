package core

import (
	"errors"
	"strings"
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
		return errors.New("operation ID cannot be empty")

	case strings.TrimSpace(operation.ClientID) == "":
		return errors.New("client ID cannot be empty")

	case strings.TrimSpace(operation.Type) == "":
		return errors.New("operation type cannot be empty")

	case operation.Cost == 0:
		return errors.New("operation cost must be greater than zero")

	default:
		return nil
	}
}
