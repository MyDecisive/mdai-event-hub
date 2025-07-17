package common

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetEnvVariableWithDefault(t *testing.T) {
	tests := []struct {
		name         string
		key          string
		defaultValue string
		envValue     string
		setEnv       bool
		expected     string
	}{
		{
			name:         "env variable exists",
			key:          "TEST_VAR",
			defaultValue: "default",
			envValue:     "actual_value",
			setEnv:       true,
			expected:     "actual_value",
		},
		{
			name:         "env variable does not exist",
			key:          "NONEXISTENT_VAR",
			defaultValue: "default_value",
			setEnv:       false,
			expected:     "default_value",
		},
		{
			name:         "env variable exists but is empty",
			key:          "EMPTY_VAR",
			defaultValue: "default",
			envValue:     "",
			setEnv:       true,
			expected:     "",
		},
		{
			name:         "empty default value",
			key:          "MISSING_VAR",
			defaultValue: "",
			setEnv:       false,
			expected:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clean up environment
			os.Unsetenv(tt.key)

			// Set environment variable if needed
			if tt.setEnv {
				os.Setenv(tt.key, tt.envValue)
				defer os.Unsetenv(tt.key)
			}

			result := GetEnvVariableWithDefault(tt.key, tt.defaultValue)
			assert.Equal(t, tt.expected, result)
		})
	}
}
