package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/decisiveai/mdai-data-core/eventing"
	"github.com/decisiveai/mdai-data-core/eventing/subscriber"
	"github.com/decisiveai/mdai-event-hub/internal/eventhub"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func initLogger() *zap.Logger {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "timestamp"
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.CallerKey = "caller"

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		zapcore.Lock(os.Stdout),
		zap.DebugLevel,
	)
	return zap.New(core, zap.AddCaller())
}

func main() {
	logger := initLogger()
	//nolint:all
	defer logger.Sync()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	mdai, cleanup := initDependencies(ctx, logger)
	defer cleanup()

	eventSubscriber, err := subscriber.NewSubscriber(ctx, logger, "subscriber-event-hub")
	if err != nil {
		logger.Fatal("Failed to create subscriber", zap.Error(err))
	}
	//nolint:all
	defer eventSubscriber.Close()

	// prometheus alerts
	if err := eventSubscriber.Subscribe(ctx, eventing.AlertConsumerGroupName, "alert", eventhub.ProcessAlertingEvent(ctx, mdai)); err != nil {
		logger.Fatal("Failed to start Alerts event listener", zap.Error(err))
	}

	// manual variables updates
	if err := eventSubscriber.Subscribe(ctx, eventing.VarsConsumerGroupName, "var", eventhub.ProcessVariableEvent(ctx, mdai)); err != nil {
		logger.Fatal("Failed to start Alerts event listener", zap.Error(err))
	}

	<-ctx.Done()
	logger.Info("Service shutting down")
}
