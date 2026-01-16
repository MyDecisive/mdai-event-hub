package main

import (
	"context"
	"github.com/decisiveai/mdai-data-core/opamp"
	"github.com/google/uuid"
	"github.com/open-telemetry/opamp-go/protobufs"
	"github.com/open-telemetry/opamp-go/server/types"
	"net/http"
	"os"

	"github.com/decisiveai/mdai-data-core/audit"
	datacorepublisher "github.com/decisiveai/mdai-data-core/eventing/publisher"
	corehandlers "github.com/decisiveai/mdai-data-core/handlers"
	"github.com/decisiveai/mdai-data-core/interpolation"
	dcorekube "github.com/decisiveai/mdai-data-core/kube"
	"github.com/decisiveai/mdai-data-core/valkey"
	"github.com/decisiveai/mdai-event-hub/internal/eventhub"
	mdaiclientset "github.com/decisiveai/mdai-operator/pkg/generated/clientset/versioned/typed/api/v1"
	"github.com/kelseyhightower/envconfig"
	opampserver "github.com/open-telemetry/opamp-go/server"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
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

	cfg, err := getKubeConfig(logger, os.UserHomeDir)
	if err != nil {
		logger.Fatal("Failed to get kube config", zap.Error(err))
	}
	mdaiClientset, err := mdaiclientset.NewForConfig(cfg)
	if err != nil {
		logger.Fatal("Failed to create mdai clientset", zap.Error(err))
	}

	configMgr, err := dcorekube.NewConfigMapController(dcorekube.AutomationConfigMapType, corev1.NamespaceAll, clientset, logger)
	if err != nil {
		logger.Fatal("Failed to create ConfigMap manager", zap.Error(err))
	}
	if confMgrRunErr := configMgr.Run(); confMgrRunErr != nil {
		logger.Fatal("failed to run ConfigMap manager", zap.Error(confMgrRunErr))
	}

	publisher, err := datacorepublisher.NewPublisher(ctx, logger, publisherClientName)
	if err != nil {
		logger.Fatal("failed to start NATS publisher", zap.Error(err))
	}

	var config Config
	if err := envconfig.Process("", &config); err != nil {
		logger.Fatal("failed to process env config", zap.Error(err))
	}

	opampServer := opampserver.New(opamp.NewLoggerFromZap(logger, "opamp-server"))
	opampConnectionManager := opamp.NewAgentConnectionManager()

	settings := opampserver.StartSettings{
		ListenEndpoint: "127.0.0.1:4320",
		Settings: opampserver.Settings{
			Callbacks: types.Callbacks{
				OnConnecting: func(request *http.Request) types.ConnectionResponse {
					return types.ConnectionResponse{
						Accept: true,
						ConnectionCallbacks: types.ConnectionCallbacks{
							OnConnected: func(ctx context.Context, conn types.Connection) {
								// TODO: switch to debug
								logger.Info("Connected to Opamp Agent (collector)")
							},
							OnMessage: func(ctx context.Context, conn types.Connection, message *protobufs.AgentToServer) *protobufs.ServerToAgent {
								// TODO: switch to debug
								instanceID, uuidErr := uuid.FromBytes(message.GetInstanceUid())
								if uuidErr != nil {
									logger.Warn("Failed to parse instance uuid", zap.Error(uuidErr))
								}
								logger.Info("Message from collector agent", zap.String("ID", instanceID.String()))

								opampConnectionManager.AddConnection(conn, string(message.GetInstanceUid()))
								// TODO: handle message from agent
								return &protobufs.ServerToAgent{
									InstanceUid: message.GetInstanceUid(),
								}
							},
							OnConnectionClose: func(conn types.Connection) {
								// TODO: switch to debug
								logger.Info("Disconnected from Opamp Server")
								opampConnectionManager.RemoveConnection(conn)
							},
						},
					}
				},
			},
		},
	}

	if err = opampServer.Start(settings); err != nil {
		logger.Fatal("failed to start opamp server", zap.Error(err))
	}
	logger.Info("opamp server started")

	eventHub = &eventhub.EventHub{
		VarsAdapter: eventhub.VarDeps{
			Logger:         logger,
			HandlerAdapter: corehandlers.NewHandlerAdapter(valkeyClient, logger, publisher),
		},
		Logger:                 logger,
		Kube:                   clientset,
		MdaiClientset:          mdaiClientset,
		AuditAdapter:           auditAdapter,
		ConfigMapController:    configMgr,
		InterpolationEngine:    interpolation.NewEngine(logger),
		AgentConnectionManager: opampConnectionManager,
		HopLimit:               config.HopLimit,
	}

	cleanup = func() {
		logger.Info("Closing client connections...")
		configMgr.Stop()
		valkeyClient.Close()
		if err = opampServer.Stop(ctx); err != nil {
			logger.Error("failed to stop opamp server", zap.Error(err))
		}
		logger.Info("Cleanup complete.")
	}

	return eventHub, cleanup
}

type HomeDirGetterFunc func() (string, error)

// TODO: Refactor this and above type to live in data core and use Data Core's exported version of them (ENG-930).
func getKubeConfig(logger *zap.Logger, homeDirGetterFunc HomeDirGetterFunc) (*rest.Config, error) {
	config, inClusterErr := rest.InClusterConfig()
	if inClusterErr != nil {
		// Try fetching config from the default file location
		homeDir, homeDirErr := homeDirGetterFunc()
		if homeDirErr != nil {
			logger.Error("Failed to load home directory for loading k8s config", zap.Error(homeDirErr))
			return nil, homeDirErr
		}

		fileConfig, kubeConfigFromFileErr := clientcmd.BuildConfigFromFlags("", homeDir+"/.kube/config")
		if kubeConfigFromFileErr != nil {
			logger.Error("Failed to build k8s config", zap.Error(kubeConfigFromFileErr))
			return nil, kubeConfigFromFileErr
		}
		config = fileConfig
	}
	return config, nil
}
