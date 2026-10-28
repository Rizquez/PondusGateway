package core

import (
	"errors"
	"strings"
)

// Operation represents an operation associated with a customer,
// identified by an ID, a type, and a cost.
type Operation struct {
	ID       string
	ClientID string
	Type     string
	Cost     uint64
}

// Validate checks that the operation complies with the established
// constraints.
//
// The ID, ClientID, and Type fields cannot be empty or contain only
// spaces; and the Cost field must be greater than zero.
//
// Returns an error describing the first constraint violation or nil
// if the operation is valid.
//
// It does not modify or normalize the operation’s data, nor does it
// check for duplicate identifiers.
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
