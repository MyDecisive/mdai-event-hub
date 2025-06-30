package eventhub

import (
	"context"
	"github.com/decisiveai/mdai-event-hub/internal/common"
	"github.com/decisiveai/mdai-event-hub/pkg/eventing"
	"go.uber.org/zap"
	"time"
)

const (
	rabbitmqEndpointEnvVarKey = "RABBITMQ_ENDPOINT"
	rabbitmqPasswordEnvVarKey = "RABBITMQ_PASSWORD"
)

func Init(ctx context.Context, logger *zap.Logger) (eventing.EventHub, error) {
	rmqEndpoint := common.GetEnvVariableWithDefault(rabbitmqEndpointEnvVarKey, "")
	rmqPassword := common.GetEnvVariableWithDefault(rabbitmqPasswordEnvVarKey, "")

	logger.Info("Connecting to RabbitMQ",
		zap.String("endpoint", rmqEndpoint),
		zap.String("queue", eventing.EventQueueName))

	initializer := func() (eventing.EventHub, error) {
		hub := eventing.NewEventHub("amqp://mdai:"+rmqPassword+"@"+rmqEndpoint+"/", eventing.EventQueueName, logger)
		if err := hub.Connect(); err != nil {
			return nil, err
		}
		return hub, nil
	}

	return common.RetryInitializer(
		ctx,
		logger,
		"event hub",
		initializer,
		3*time.Minute,
		5*time.Second,
	)
}
