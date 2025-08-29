package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/decisiveai/mdai-event-hub/internal/eventhub"
	"github.com/decisiveai/mdai-event-hub/pkg/eventing"
	"github.com/decisiveai/mdai-event-hub/pkg/eventing/nats"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	valkeyAuditStreamExpiryMSEnvVarKey = "VALKEY_AUDIT_STREAM_EXPIRY_MS"
	mdaiHubEventHistoryStreamName      = "mdai_hub_event_history"
	defaultValkeyAuditStreamExpiry     = 30 * 24 * time.Hour
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

	subscriber, err := nats.NewSubscriber(ctx, logger, "subscriber-event-hub")
	if err != nil {
		logger.Fatal("Failed to create subscriber", zap.Error(err))
	}
	//nolint:all
	defer subscriber.Close()

	// prometheus alerts
	if err := subscriber.Subscribe(ctx, eventing.AlertConsumerGroupName, "alert", eventhub.ProcessAlertingEvent(ctx, mdai)); err != nil {
		logger.Fatal("Failed to start Alerts event listener", zap.Error(err))
	}

	// manual variables updates
	if err := subscriber.Subscribe(ctx, eventing.VarsConsumerGroupName, "var", eventhub.ProcessVariableEvent(ctx, mdai)); err != nil {
		logger.Fatal("Failed to start Alerts event listener", zap.Error(err))
	}

	<-ctx.Done()
	logger.Info("Service shutting down")
}
