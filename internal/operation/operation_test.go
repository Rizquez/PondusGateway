package operation

import (
	"math"
	"testing"
)

func TestOperationValidate(t *testing.T) {
	valid := Operation{
		ID:       "operation-1",
		ClientID: "client-1",
		Type:     "compute",
		Cost:     20,
	}

	tests := []struct {
		name      string
		operation Operation
		wantError bool
	}{
		{"valid", valid, false},

		{"empty ID", Operation{
			ClientID: "client-1",
			Type:     "compute",
			Cost:     20,
		}, true},

		{"blank client", Operation{
			ID:       "operation-1",
			ClientID: "   ",
			Type:     "compute",
			Cost:     20,
		}, true},

		{"empty type", Operation{
			ID:       "operation-1",
			ClientID: "client-1",
			Cost:     20,
		}, true},

		{"zero cost", Operation{
			ID:       "operation-1",
			ClientID: "client-1",
			Type:     "compute",
		}, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.operation.Validate()

			if (err != nil) != test.wantError {
				t.Fatalf(
					"Validate() err=%v, wantError=%v",
					err, test.wantError,
				)
			}
		})
	}
}

func TestOperationValidateEdgeCases(t *testing.T) {
	tests := []struct {
		name      string
		operation Operation
		wantError bool
	}{
		{
			name: "whitespace ID",
			operation: Operation{
				ID:       " \t\n",
				ClientID: "client-1",
				Type:     "compute",
				Cost:     1,
			},
			wantError: true,
		},
		{
			name: "whitespace type",
			operation: Operation{
				ID:       "operation-1",
				ClientID: "client-1",
				Type:     " \t",
				Cost:     1,
			},
			wantError: true,
		},
		{
			name: "minimum positive cost",
			operation: Operation{
				ID:       "operation-1",
				ClientID: "client-1",
				Type:     "compute",
				Cost:     1,
			},
			wantError: false,
		},
		{
			name: "maximum uint64 cost",
			operation: Operation{
				ID:       "operation-1",
				ClientID: "client-1",
				Type:     "compute",
				Cost:     math.MaxUint64,
			},
			wantError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.operation.Validate()

			if (err != nil) != test.wantError {
				t.Fatalf(
					"Validate() error = %v, wantError = %v",
					err, test.wantError,
				)
			}
		})
	}
}
