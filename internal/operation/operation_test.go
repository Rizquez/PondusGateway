package operation

import (
	"math"
	"testing"
)

func TestOperationValidate(test *testing.T) {
	valid := Operation{
		ID:       "op-1",
		ClientID: "client-1",
		Type:     "compute",
		Cost:     20,
	}

	tests := []struct {
		name      string
		op        Operation
		wantError bool
	}{
		{"valid", valid, false},

		{"empty ID", Operation{
			ClientID: "client-1",
			Type:     "compute",
			Cost:     20,
		}, true},

		{"blank client", Operation{
			ID:       "op-1",
			ClientID: "   ",
			Type:     "compute",
			Cost:     20,
		}, true},

		{"empty type", Operation{
			ID:       "op-1",
			ClientID: "client-1",
			Cost:     20,
		}, true},

		{"zero cost", Operation{
			ID:       "op-1",
			ClientID: "client-1",
			Type:     "compute",
		}, true},
	}

	for _, tt := range tests {
		test.Run(tt.name, func(test *testing.T) {
			err := tt.op.Validate()

			if (err != nil) != tt.wantError {
				test.Fatalf(
					"Validate() err=%v, wantError=%v",
					err,
					tt.wantError,
				)
			}
		})
	}
}

func TestOperationValidateEdgeCases(test *testing.T) {
	tests := []struct {
		name      string
		op        Operation
		wantError bool
	}{
		{
			name: "whitespace ID",
			op: Operation{
				ID:       " \t\n",
				ClientID: "client-1",
				Type:     "compute",
				Cost:     1,
			},
			wantError: true,
		},
		{
			name: "whitespace type",
			op: Operation{
				ID:       "op-1",
				ClientID: "client-1",
				Type:     " \t",
				Cost:     1,
			},
			wantError: true,
		},
		{
			name: "minimum positive cost",
			op: Operation{
				ID:       "op-1",
				ClientID: "client-1",
				Type:     "compute",
				Cost:     1,
			},
			wantError: false,
		},
		{
			name: "maximum uint64 cost",
			op: Operation{
				ID:       "op-1",
				ClientID: "client-1",
				Type:     "compute",
				Cost:     math.MaxUint64,
			},
			wantError: false,
		},
	}

	for _, tt := range tests {
		test.Run(tt.name, func(t *testing.T) {
			err := tt.op.Validate()

			if (err != nil) != tt.wantError {
				t.Fatalf(
					"Validate() error = %v, wantError = %v",
					err, tt.wantError,
				)
			}
		})
	}
}
