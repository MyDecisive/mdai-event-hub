package main

import (
	"context"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/decisiveai/mdai-data-core/audit"
	corehandlers "github.com/decisiveai/mdai-data-core/handlers"
	dcorekube "github.com/decisiveai/mdai-data-core/kube"
	"github.com/decisiveai/mdai-event-hub/internal/eventhub"
	"github.com/decisiveai/mdai-event-hub/internal/handlers"
	internalvalkey "github.com/decisiveai/mdai-event-hub/internal/valkey"
	"github.com/decisiveai/mdai-event-hub/pkg/eventing"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	valkeyAuditStreamExpiryMSEnvVarKey = "VALKEY_AUDIT_STREAM_EXPIRY_MS"
	mdaiHubEventHistoryStreamName      = "mdai_hub_event_history"
	defaultValkeyAuditStreamExpiry     = 30 * 24 * time.Hour
)

func createLogger() *zap.Logger {
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
	logger := createLogger()
	//nolint:all
	defer logger.Sync()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Initialize ValKeyClient with retry logic
	valkeyClient, err := internalvalkey.Init(ctx, logger)
	if err != nil {
		logger.Fatal("failed to get valkey client", zap.Error(err))
	}
	defer valkeyClient.Close()

	valkeyAuditStreamExpiry := defaultValkeyAuditStreamExpiry
	valkeyStreamExpiryMsStr := os.Getenv(valkeyAuditStreamExpiryMSEnvVarKey)
	if valkeyStreamExpiryMsStr != "" {
		envExpiryMs, parseErr := strconv.Atoi(valkeyStreamExpiryMsStr)
		if parseErr != nil {
			logger.Fatal("Failed to parse valkeyStreamExpiryMs env var", zap.Error(parseErr))
			return
		}
		valkeyAuditStreamExpiry = time.Duration(envExpiryMs) * time.Millisecond
		logger.Info("Using custom "+mdaiHubEventHistoryStreamName+" expiration threshold MS", zap.Int64("valkeyAuditStreamExpiryMs", valkeyAuditStreamExpiry.Milliseconds()))
	}

	auditAdapter := audit.NewAuditAdapter(logger, valkeyClient, valkeyAuditStreamExpiry)

	subscriber, err := eventhub.Init(logger)
	if err != nil {
		logger.Fatal("Failed to create subscriber", zap.Error(err))
	}
	defer func(subscriber eventing.Subscriber) {
		if subscribeCloseErr := subscriber.Close(); subscribeCloseErr != nil {
			logger.Warn("failed to close NATS subscriber", zap.Error(subscribeCloseErr))
		}
	}(subscriber)

	clientset, err := dcorekube.NewK8sClient(logger)
	if err != nil {
		logger.Fatal("Failed to create k8s client", zap.Error(err))
		return
	}

	configMgr, err := dcorekube.NewConfigMapController(dcorekube.AutomationConfigMapType, corev1.NamespaceAll, clientset, logger)
	if err != nil {
		logger.Fatal("Failed to create ConfigMap manager", zap.Error(err))
	}
	if confMgrRunErr := configMgr.Run(); confMgrRunErr != nil {
		logger.Fatal("Failed to run  ConfigMap manager", zap.Error(confMgrRunErr))
	}
	defer configMgr.Stop()

	mdaiInterface := handlers.MdaiInterface{
		Data:      corehandlers.NewHandlerAdapter(valkeyClient, logger),
		Logger:    logger,
		Namespace: metav1.NamespaceDefault,
		Kube:      clientset,
	}

	subscribe(ctx, subscriber, mdaiInterface, configMgr, auditAdapter, logger)

	<-ctx.Done()
	logger.Info("Service shutting down")
}

func subscribe(ctx context.Context, subscriber eventing.Subscriber, mdaiInterface handlers.MdaiInterface, configMgr *dcorekube.ConfigMapController, auditAdapter *audit.AuditAdapter, logger *zap.Logger) {
	// prometheus alerts
	if err := subscriber.Subscribe(ctx, eventing.AlertConsumerGroupName, "alert", eventhub.ProcessAlertingEvent(ctx, mdaiInterface, configMgr, auditAdapter)); err != nil {
		logger.Fatal("Failed to start Alerts event listener", zap.Error(err))
	}

	// manual variables updates
	if err := subscriber.Subscribe(ctx, eventing.VarsConsumerGroupName, "var", eventhub.ProcessVariableEvent(ctx, mdaiInterface)); err != nil {
		logger.Fatal("Failed to start Alerts event listener", zap.Error(err))
	}
}
