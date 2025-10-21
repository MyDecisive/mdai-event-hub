package main

import (
	"context"

	"github.com/decisiveai/mdai-data-core/audit"
	datacorepublisher "github.com/decisiveai/mdai-data-core/eventing/publisher"
	corehandlers "github.com/decisiveai/mdai-data-core/handlers"
	"github.com/decisiveai/mdai-data-core/interpolation"
	dcorekube "github.com/decisiveai/mdai-data-core/kube"
	"github.com/decisiveai/mdai-data-core/valkey"
	"github.com/decisiveai/mdai-event-hub/internal/eventhub"
	"github.com/kelseyhightower/envconfig"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
)

const publisherClientName = "publisher-mdai-event-hub"

type Config struct {
	HopLimit int `default:"2" envconfig:"HOP_LIMIT"`
}

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

	publisher, err := datacorepublisher.NewPublisher(ctx, logger, publisherClientName)
	if err != nil {
		logger.Fatal("failed to start NATS publisher", zap.Error(err))
	}

	var config Config
	if err := envconfig.Process("", &config); err != nil {
		logger.Fatal("failed to process env config", zap.Error(err))
	}

	eventHub = &eventhub.EventHub{
		VarsAdapter: eventhub.VarDeps{
			Logger:         logger,
			HandlerAdapter: corehandlers.NewHandlerAdapter(valkeyClient, logger, publisher),
		},
		Logger:              logger,
		Kube:                clientset,
		AuditAdapter:        auditAdapter,
		ConfigMapController: configMgr,
		InterpolationEngine: interpolation.NewEngine(logger),
		HopLimit:            config.HopLimit,
	}

	cleanup = func() {
		logger.Info("Closing client connections...")
		configMgr.Stop()
		valkeyClient.Close()
		logger.Info("Cleanup complete.")
	}

	return eventHub, cleanup
}
