package eventing

import (
	"go.uber.org/zap"
)

const (
	EventQueueName             = "mdai-events"
	ManualVariablesEventSource = "manual_variables_api"
)

func NewEventHub(connectionString string, queueName string, logger *zap.Logger) EventHub {
	return NewRmqBackend(connectionString, queueName, logger)
}
