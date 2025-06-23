package eventing

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"testing"
)

func TestNewRmqBackend(t *testing.T) {
	logger := zaptest.NewLogger(t)
	connectionString := "amqp://guest:guest@localhost:5672/"
	queueName := "test-queue"

	backend := NewRmqBackend(connectionString, queueName, logger)

	assert.NotNil(t, backend)

	rmqBackend, ok := backend.(*RmqBackend)
	require.True(t, ok)
	assert.Equal(t, connectionString, rmqBackend.connectionString)
	assert.Equal(t, queueName, rmqBackend.queueName)
	assert.Equal(t, logger, rmqBackend.logger)
}

func TestRmqBackend_Connect_InvalidConnectionString(t *testing.T) {
	logger := zaptest.NewLogger(t)
	backend := &RmqBackend{
		connectionString: "invalid-connection-string",
		queueName:        "test-queue",
		logger:           logger,
	}

	err := backend.Connect()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to dial")
}

func TestRmqBackend_PublishMessage_NoConnection(t *testing.T) {
	logger := zaptest.NewLogger(t)
	backend := &RmqBackend{
		connectionString: "amqp://localhost:5672/",
		queueName:        "test-queue",
		logger:           logger,
	}

	event := MdaiEvent{
		Name:    "test-event",
		Payload: "test-payload",
		Source:  "test-source",
		HubName: "test-hub",
	}

	err := backend.PublishMessage(event)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "connection is closed")
}

func TestRmqBackend_StartListening_NoConnection(t *testing.T) {
	logger := zaptest.NewLogger(t)
	backend := &RmqBackend{
		connectionString: "amqp://localhost:5672/",
		queueName:        "test-queue",
		logger:           logger,
	}

	invoker := func(event MdaiEvent) error {
		return nil
	}

	err := backend.StartListening(invoker)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "connection is closed")
}

func TestRmqBackend_Close_SafeToCallMultipleTimes(t *testing.T) {
	logger := zaptest.NewLogger(t)
	backend := &RmqBackend{
		connectionString: "amqp://localhost:5672/",
		queueName:        "test-queue",
		logger:           logger,
	}

	backend.Close()
	backend.Close()
}

func TestRmqBackend_MarshalLogObject(t *testing.T) {
	logger := zaptest.NewLogger(t)
	backend := &RmqBackend{
		connectionString: "amqp://localhost:5672/",
		queueName:        "test-queue",
		logger:           logger,
		isListening:      true,
	}

	encoder := zaptest.NewLogger(t).Core().With([]zap.Field{zap.Object("backend", backend)})
	assert.NotNil(t, encoder)
}

func TestRmqBackend_StartListening_AlreadyListening(t *testing.T) {
	logger := zaptest.NewLogger(t)
	backend := &RmqBackend{
		connectionString: "amqp://localhost:5672/",
		queueName:        "test-queue",
		logger:           logger,
		isListening:      true,
	}

	invoker := func(event MdaiEvent) error {
		return nil
	}

	err := backend.StartListening(invoker)
	assert.NoError(t, err)
}
