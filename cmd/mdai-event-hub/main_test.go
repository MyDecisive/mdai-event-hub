package main

import (
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/decisiveai/mdai-event-hub/internal/handlers"
	"github.com/decisiveai/mdai-event-hub/pkg/eventing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessEventPayload_Success(t *testing.T) {
	validJSON := `{"key1":"value1","key2":42,"nested":{"sub":"val"}}`
	event := eventing.MdaiEvent{
		ID:      "e1",
		Name:    "test",
		HubName: "hub1",
		Payload: validJSON,
	}

	result, err := handlers.ProcessEventPayload(event)
	require.NoError(t, err, "expected no error for valid JSON payload")

	assert.Contains(t, result, "key1")
	assert.Contains(t, result, "key2")
	assert.Contains(t, result, "nested")

	assert.Equal(t, "value1", result["key1"])
	assert.InDelta(t, float64(42), result["key2"], 0.001)
	nested, ok := result["nested"].(map[string]any)
	assert.True(t, ok, "expected nested to be a map[string]any")
	assert.Equal(t, "val", nested["sub"])
}

func TestProcessEventPayload_InvalidJSON(t *testing.T) {
	invalidJSON := `{"key1":"value1", "key2":}`
	event := eventing.MdaiEvent{
		ID:      "e2",
		Name:    "test-invalid",
		HubName: "hub2",
		Payload: invalidJSON,
	}

	result, err := handlers.ProcessEventPayload(event)
	assert.Nil(t, result, "expected result to be nil on invalid JSON")
	require.Error(t, err, "expected an error for invalid JSON payload")
	assert.Contains(t, err.Error(), "failed to unmarshal payload")
}

func TestMain_CustomValkeyExpiryEnvVar(t *testing.T) {
	key := valkeyAuditStreamExpiryMSEnvVarKey
	t.Setenv(key, "86400000") // 1 day in ms

	expiryMsStr := os.Getenv(key)
	ms, err := strconv.Atoi(expiryMsStr)
	require.NoError(t, err)

	expiry := time.Duration(ms) * time.Millisecond
	assert.Equal(t, 24*time.Hour, expiry)
}

func TestCreateLogger(t *testing.T) {
	logger := createLogger()
	assert.NotNil(t, logger)
	logger.Info("test message") // No crash = pass
}
