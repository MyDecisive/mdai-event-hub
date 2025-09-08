package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/decisiveai/mdai-data-core/eventing"
	"github.com/decisiveai/mdai-data-core/eventing/subscriber"
	"github.com/decisiveai/mdai-data-core/service"
	"github.com/decisiveai/mdai-event-hub/internal/eventhub"
	"go.uber.org/zap"
)

const serviceName = "github.com/decisiveai/mdai-event-hub"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	_, logger, teardown := service.InitLogger(ctx, serviceName)
	defer teardown()

	eventHub, cleanup := initDependencies(ctx, logger)
	defer cleanup()

	eventSubscriber, err := subscriber.NewSubscriber(ctx, logger, "subscriber-event-hub")
	if err != nil {
		logger.Fatal("Failed to create subscriber", zap.Error(err))
	}
	defer eventSubscriber.Close() //nolint:all

	// prometheus alerts
	if err := eventSubscriber.Subscribe(ctx, eventing.AlertConsumerGroupName, "alert", eventhub.WithRecover(logger, eventHub.ProcessAlertingEvent(ctx))); err != nil {
		logger.Fatal("Failed to start Alerts event listener", zap.Error(err))
	}

	// manual variables updates
	if err := eventSubscriber.Subscribe(ctx, eventing.VarsConsumerGroupName, "var", eventhub.WithRecover(logger, eventHub.ProcessVariableEvent(ctx))); err != nil {
		logger.Fatal("Failed to start Vars event listener", zap.Error(err))
	}

	<-ctx.Done()
	logger.Info("Service shutting down")
}
