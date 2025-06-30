package main

import (
	"context"
	"fmt"
	datacore "github.com/decisiveai/mdai-data-core/handlers"
	eventHub "github.com/decisiveai/mdai-event-hub/internal/eventhub"
	"github.com/decisiveai/mdai-event-hub/internal/handlers"
	valkeyWrapper "github.com/decisiveai/mdai-event-hub/internal/valkey"
	configMapMgr "github.com/decisiveai/mdai-event-hub/pkg/configMap"
	"github.com/decisiveai/mdai-event-hub/pkg/eventing"
	v1 "github.com/decisiveai/mdai-operator/api/v1"

	"os"
	"strings"

	"github.com/valkey-io/valkey-go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	logger *zap.Logger
)

const (
	automationConfigMapNamePostfix = "-automation"
)

func init() {
	// Define custom encoder configuration
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "timestamp"                   // Rename the time field
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder // Use human-readable timestamps
	encoderConfig.CallerKey = "caller"                    // Show caller file and line number
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig), // JSON logging with readable timestamps
		zapcore.Lock(os.Stdout),               // Output to stdout
		zap.DebugLevel,                        // Log info and above
	)

	logger = zap.New(core, zap.AddCaller())
	// don't really care about failing of defer that is the last thing run before the program exists
	// nolint:all
	defer logger.Sync() // Flush logs before exiting
}

// ProcessEvent handles an MdaiEvent according to configured workflows
func ProcessEvent(ctx context.Context, client valkey.Client, configMgr configMapMgr.ConfigMapManagerInterface, logger *zap.Logger) eventing.HandlerInvoker {
	dataAdapter := datacore.NewHandlerAdapter(client, logger)

	mdaiInterface := handlers.MdaiInterface{
		Data:   dataAdapter,
		Logger: logger,
	}

	return func(event eventing.MdaiEvent) error {
		hubName := event.HubName
		if hubName == "" {
			return fmt.Errorf("no hub name provided")
		}
		logger.Info("Processing event for hub",
			zap.String("hubName", event.HubName),
			zap.String("eventName", event.Name),
		)

		if event.Source == eventing.ManualVariablesEventSource {
			err := handlers.HandleManualVariablesActions(ctx, mdaiInterface, event)
			if err != nil {
				return err
			}
			return nil
		}

		workflowMap, err := configMgr.GetConfigMapForHub(ctx, event.HubName)
		if err != nil {
			return fmt.Errorf("error getting ConfigMap for hub %s: %w", event.HubName, err)
		}

		var workflowFound bool
		var steps []v1.AutomationStep
		// Match on whole name, e.g. "NoisyServiceAlert.firing"
		if workflow, exists := workflowMap[event.Name]; exists {
			workflowFound = true
			steps = workflow
			// Match on alert name regardless of status, e.g. NoisyServiceAlert
		} else if nameparts := strings.Split(event.Name, "."); len(nameparts) > 1 {
			if workflow, exists := workflowMap[nameparts[0]]; exists {
				workflowFound = true
				steps = workflow
			}
		}

		if !workflowFound {
			logger.Warn("No configured automation for event", zap.String("name", event.Name))
			return nil // Don't treat this as an error, just log a warning
		}

		for _, automationStep := range steps {
			if err := safePerformAutomationStep(mdaiInterface, automationStep, event); err != nil {
				logger.Error("Automation step failed",
					zap.String("hubName", event.HubName),
					zap.String("name", event.Name),
					zap.String("handlerRef", automationStep.HandlerRef),
					zap.Error(err),
				)
				return err
			}
		}
		return nil
	}
}

func safePerformAutomationStep(mdai handlers.MdaiInterface, autoStep v1.AutomationStep, event eventing.MdaiEvent) (err error) {
	// handle panics
	defer func() {
		if r := recover(); r != nil {
			mdai.Logger.Error(
				"Panic inside automation handler",
				zap.Any("panicValue", r),
				zap.String("handlerRef", autoStep.HandlerRef),
				zap.String("eventName", event.Name),
				zap.String("hubName", event.HubName),
			)
			err = fmt.Errorf("panic in handler %s: %v", autoStep.HandlerRef, r)
		}
	}()

	handlerName := handlers.HandlerName(autoStep.HandlerRef)

	if handlerFn, exists := handlers.SupportedHandlers[handlerName]; exists {
		// TODO add event audit here
		if err := handlerFn(mdai, event, autoStep.Arguments); err != nil {
			return fmt.Errorf("handler %s failed: %w", handlerName, err)
		}
		return nil
	}
	return fmt.Errorf("handler %s not supported", handlerName)
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize ValKeyClient with retry logic
	valkeyClient, err := valkeyWrapper.Init(ctx, logger)
	if err != nil {
		logger.Fatal("failed to get valkey client", zap.Error(err))
	}
	defer valkeyClient.Close()

	hub, err := eventHub.Init(ctx, logger)
	if err != nil {
		logger.Fatal("Failed to create RmqBackend", zap.Error(err))
	}
	defer hub.Close()

	configMgr, err := configMapMgr.NewConfigMapManager(logger, automationConfigMapNamePostfix)
	if err != nil {
		logger.Fatal("Failed to create ConfigMap manager", zap.Error(err))
	}
	defer configMgr.Cleanup()

	// Start listening and block until termination signal
	err = hub.ListenUntilSignal(ProcessEvent(ctx, valkeyClient, configMgr, logger))
	if err != nil {
		logger.Fatal("Failed to start event listener", zap.Error(err))
	}

	logger.Info("Service shutting down")
}
