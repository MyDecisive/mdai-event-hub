package main

import (
	"context"

	"github.com/decisiveai/mdai-data-core/audit"
	corehandlers "github.com/decisiveai/mdai-data-core/handlers"
	"github.com/decisiveai/mdai-data-core/interpolation"
	dcorekube "github.com/decisiveai/mdai-data-core/kube"
	"github.com/decisiveai/mdai-data-core/valkey"
	"github.com/decisiveai/mdai-event-hub/internal/eventhub"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
)

func initDependencies(ctx context.Context, logger *zap.Logger) (eventHub *eventhub.EventHub, cleanup func()) { //nolint:nonamedreturns
	valkeyClient, err := valkey.Init(ctx, logger, valkey.NewConfig())
	if err != nil {
		logger.Fatal("failed to initialize ValKey client", zap.Error(err))
	}

	auditAdapter := audit.NewAuditAdapter(logger, valkeyClient)

	clientset, err := dcorekube.NewK8sClient(logger)
	if err != nil {
		logger.Fatal("failed to create k8s client", zap.Error(err))
	}

	configMgr, err := dcorekube.NewConfigMapController(dcorekube.AutomationConfigMapType, corev1.NamespaceAll, clientset, logger)
	if err != nil {
		logger.Fatal("Failed to create ConfigMap manager", zap.Error(err))
	}
	if confMgrRunErr := configMgr.Run(); confMgrRunErr != nil {
		logger.Fatal("failed to run  ConfigMap manager", zap.Error(confMgrRunErr))
	}

	eventHub = &eventhub.EventHub{
		VarsAdapter: eventhub.VarDeps{
			Logger:         logger,
			HandlerAdapter: corehandlers.NewHandlerAdapter(valkeyClient, logger),
		},
		Logger:              logger,
		Kube:                clientset,
		AuditAdapter:        auditAdapter,
		ConfigMapController: configMgr,
		InterpolationEngine: interpolation.NewEngine(logger),
	}

	cleanup = func() {
		logger.Info("Closing client connections...")
		configMgr.Stop()
		valkeyClient.Close()
		logger.Info("Cleanup complete.")
	}

	return eventHub, cleanup
}
