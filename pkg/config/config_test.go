package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOptions_Validate(t *testing.T) {
	tests := []struct {
		name          string
		options       *Options
		expectedError string
	}{
		{
			name:    "defaults are always valid",
			options: NewDefaultOptions(),
		},
		{
			name: "negative DeleteAfter value",
			options: &Options{
				ControllerID:   DefaultControllerID,
				ResyncInterval: DefaultResyncInterval,
				DeleteAfter:    -1 * time.Second,
			},
			expectedError: "--delete-after has to be greater than or equal to 0s",
		},
		{
			name: "invalid ResyncInterval value",
			options: &Options{
				ControllerID:   DefaultControllerID,
				ResyncInterval: 999 * time.Millisecond,
			},
			expectedError: "--resync-interval has to be greater than or equal to 1s",
		},
		{
			name: "empty controller id",
			options: &Options{
				ResyncInterval: DefaultResyncInterval,
			},
			expectedError: "--controller-id must not be empty",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.options.Validate()
			if test.expectedError != "" {
				require.Error(t, err)
				assert.Equal(t, test.expectedError, err.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}
