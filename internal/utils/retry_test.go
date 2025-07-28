//nolint:revive
package utils

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestRetryInitializer(t *testing.T) {
	tests := []struct {
		name            string
		initializer     func() (string, error)
		maxElapsedTime  time.Duration
		initialInterval time.Duration
		expectedResult  string
		expectError     bool
	}{
		{
			name: "successful initialization on first try",
			initializer: func() (string, error) {
				return "success", nil
			},
			maxElapsedTime:  time.Second,
			initialInterval: time.Millisecond * 100,
			expectedResult:  "success",
			expectError:     false,
		},
		{
			name: "successful initialization after retries",
			initializer: func() func() (string, error) {
				attempts := 0
				return func() (string, error) {
					attempts++
					if attempts < 3 {
						return "", errors.New("temporary failure")
					}
					return "success_after_retries", nil
				}
			}(),
			maxElapsedTime:  time.Second * 5,
			initialInterval: time.Millisecond * 10,
			expectedResult:  "success_after_retries",
			expectError:     false,
		},
		{
			name: "initialization fails after max elapsed time",
			initializer: func() (string, error) {
				return "", errors.New("persistent failure")
			},
			maxElapsedTime:  time.Millisecond * 100,
			initialInterval: time.Millisecond * 10,
			expectedResult:  "",
			expectError:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			logger := zap.NewNop()

			result, err := RetryInitializer(
				ctx,
				logger,
				"test_resource",
				tt.initializer,
				tt.maxElapsedTime,
				tt.initialInterval,
			)

			if tt.expectError {
				require.Error(t, err)
				assert.Empty(t, result)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedResult, result)
			}
		})
	}
}

func TestRetryInitializerWithContext(t *testing.T) {
	t.Run("context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		logger := zap.NewNop()

		// Cancel context after a short delay
		go func() {
			time.Sleep(time.Millisecond * 50)
			cancel()
		}()

		initializer := func() (string, error) {
			time.Sleep(time.Millisecond * 100) // Simulate slow operation
			return "", errors.New("slow failure")
		}

		result, err := RetryInitializer(
			ctx,
			logger,
			"test_resource",
			initializer,
			time.Second,
			time.Millisecond*10,
		)

		require.Error(t, err)
		assert.Empty(t, result)
	})
}

func TestRetryInitializerWithDifferentTypes(t *testing.T) {
	t.Run("integer type", func(t *testing.T) {
		ctx := t.Context()
		logger := zap.NewNop()

		initializer := func() (int, error) {
			return 42, nil
		}

		result, err := RetryInitializer(
			ctx,
			logger,
			"int_resource",
			initializer,
			time.Second,
			time.Millisecond*10,
		)

		require.NoError(t, err)
		assert.Equal(t, 42, result)
	})

	t.Run("struct type", func(t *testing.T) {
		type TestStruct struct {
			Name string
			ID   int
		}

		ctx := t.Context()
		logger := zap.NewNop()

		expected := TestStruct{Name: "test", ID: 123}
		initializer := func() (TestStruct, error) {
			return expected, nil
		}

		result, err := RetryInitializer(
			ctx,
			logger,
			"struct_resource",
			initializer,
			time.Second,
			time.Millisecond*10,
		)

		require.NoError(t, err)
		assert.Equal(t, expected, result)
	})
}
