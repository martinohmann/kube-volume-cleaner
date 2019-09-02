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
			name:    "defaults",
			options: NewDefaultOptions(),
		},
		{
			name: "negative DeleteAfter value",
			options: &Options{
				ResyncInterval: DefaultResyncInterval,
				DeleteAfter:    -1 * time.Second,
			},
			expectedError: "--delete-after has to be greater than or equal to 0s",
		},
		{
			name: "invalid ResyncInterval value",
			options: &Options{
				ResyncInterval: 999 * time.Millisecond,
			},
			expectedError: "--resync-interval has to be greater than or equal to 1s",
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
