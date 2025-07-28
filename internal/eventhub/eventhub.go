package eventhub

import (
	"github.com/decisiveai/mdai-event-hub/pkg/eventing"
	"github.com/decisiveai/mdai-event-hub/pkg/eventing/nats"
	"go.uber.org/zap"
)

//nolint:ireturn
func Init(logger *zap.Logger) (eventing.Subscriber, error) {
	return nats.NewSubscriber(logger, "subscriber-event-hub")
}
