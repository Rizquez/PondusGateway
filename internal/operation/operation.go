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

/*
For now, the rules will be:
  - The cost will be a value provided by the person creating the transaction and must be greater than zero
  - Identifiers and the type cannot be empty or consist solely of spaces
  - Duplicate identifiers will not be checked; that would require knowledge of other operations, not just one
  - Validate() checks the data, but does not modify or normalize the operation
*/
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

// TODO: Before implementing capacity control, I will need to define exactly what the Cost units represent;
// whether they are abstract values assigned manually or estimates derived from a consumption model
