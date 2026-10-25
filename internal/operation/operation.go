package operation

import (
	"errors"
	"strings"
)

type Operation struct {
	ID       string
	ClientID string
	Type     string
	Cost     uint64
}

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
