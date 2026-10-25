package settings

import "testing"

func TestLimitsValidate(t *testing.T) {
	tests := []struct {
		name    string
		limits  Limits
		wantErr bool
	}{
		{
			name: "valid",
			limits: Limits{
				MaxConcurrent: 8,
				MaxBudget:     30,
				MaxQueueSize:  100,
			},
			wantErr: false,
		},
		{
			name: "queue disabled",
			limits: Limits{
				MaxConcurrent: 1,
				MaxBudget:     1,
				MaxQueueSize:  0,
			},
			wantErr: false,
		},
		{
			name: "zero concurrency",
			limits: Limits{
				MaxConcurrent: 0,
				MaxBudget:     30,
				MaxQueueSize:  100,
			},
			wantErr: true,
		},
		{
			name: "negative concurrency",
			limits: Limits{
				MaxConcurrent: -1,
				MaxBudget:     30,
				MaxQueueSize:  100,
			},
			wantErr: true,
		},
		{
			name: "zero budget",
			limits: Limits{
				MaxConcurrent: 8,
				MaxBudget:     0,
				MaxQueueSize:  100,
			},
			wantErr: true,
		},
		{
			name: "negative queue",
			limits: Limits{
				MaxConcurrent: 8,
				MaxBudget:     30,
				MaxQueueSize:  -1,
			},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.limits.Validate()

			if (err != nil) != test.wantErr {
				t.Fatalf(
					"Validate() error = %v, wantErr = %v",
					err, test.wantErr,
				)
			}
		})
	}
}
