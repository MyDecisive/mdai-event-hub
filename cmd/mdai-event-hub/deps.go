package main

import (
	"context"
	"os"
	"strconv"
	"time"

	"github.com/decisiveai/mdai-data-core/audit"
	corehandlers "github.com/decisiveai/mdai-data-core/handlers"
	dcorekube "github.com/decisiveai/mdai-data-core/kube"
	"github.com/decisiveai/mdai-event-hub/internal/handlers"
	"github.com/decisiveai/mdai-event-hub/internal/valkey"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func initDependencies(ctx context.Context, logger *zap.Logger) (mdai handlers.MdaiInterface, cleanup func()) { //nolint:nonamedreturns
	valkeyClient, err := valkey.Init(ctx, logger)
	if err != nil {
		logger.Fatal("failed to get valkey client", zap.Error(err))
	}

	valkeyAuditStreamExpiry := defaultValkeyAuditStreamExpiry
	valkeyStreamExpiryMsStr := os.Getenv(valkeyAuditStreamExpiryMSEnvVarKey)
	if valkeyStreamExpiryMsStr != "" {
		envExpiryMs, parseErr := strconv.Atoi(valkeyStreamExpiryMsStr)
		if parseErr != nil {
			logger.Fatal("failed to parse valkeyStreamExpiryMs env var", zap.Error(parseErr))
		}
		valkeyAuditStreamExpiry = time.Duration(envExpiryMs) * time.Millisecond
		logger.Info("Using custom "+mdaiHubEventHistoryStreamName+" expiration threshold MS", zap.Int64("valkeyAuditStreamExpiryMs", valkeyAuditStreamExpiry.Milliseconds()))
	}

	auditAdapter := audit.NewAuditAdapter(logger, valkeyClient, valkeyAuditStreamExpiry)

	clientset, err := dcorekube.NewK8sClient(logger)
	if err != nil {
		logger.Fatal("ailed to create k8s client", zap.Error(err))
	}

	configMgr, err := dcorekube.NewConfigMapController(dcorekube.AutomationConfigMapType, corev1.NamespaceAll, clientset, logger)
	if err != nil {
		logger.Fatal("Failed to create ConfigMap manager", zap.Error(err))
	}
	if confMgrRunErr := configMgr.Run(); confMgrRunErr != nil {
		logger.Fatal("failed to run  ConfigMap manager", zap.Error(confMgrRunErr))
	}

	mdaiInterface := handlers.MdaiInterface{
		Data:                corehandlers.NewHandlerAdapter(valkeyClient, logger),
		Logger:              logger,
		Namespace:           metav1.NamespaceDefault,
		Kube:                clientset,
		AuditAdapter:        auditAdapter,
		ConfigMapController: configMgr,
	}

	cleanup = func() {
		logger.Info("Closing client connections...")
		configMgr.Stop()
		valkeyClient.Close()
		logger.Info("Cleanup complete.")
	}

	return mdaiInterface, cleanup
}
