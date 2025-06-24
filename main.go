package main

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	datacore "github.com/decisiveai/mdai-data-core/handlers"
	"github.com/decisiveai/mdai-event-hub/eventing"
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
	valkeyEndpointEnvVarKey = "VALKEY_ENDPOINT"
	valkeyPasswordEnvVarKey = "VALKEY_PASSWORD"

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
	//nolint:all
	defer logger.Sync() // Flush logs before exiting
}

// ProcessEvent handles an MdaiEvent according to configured workflows
func ProcessEvent(ctx context.Context, client valkey.Client, configMgr ConfigMapManagerInterface, logger *zap.Logger) eventing.HandlerInvoker {
	dataAdapter := datacore.NewHandlerAdapter(client, logger)

	mdaiInterface := MdaiInterface{
		data:   dataAdapter,
		logger: logger,
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
			err := handleManualVariablesActions(ctx, mdaiInterface, event)
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

func safePerformAutomationStep(mdai MdaiInterface, autoStep v1.AutomationStep, event eventing.MdaiEvent) (err error) {
	// handle panics
	defer func() {
		if r := recover(); r != nil {
			mdai.logger.Error(
				"Panic inside automation handler",
				zap.Any("panicValue", r),
				zap.String("handlerRef", autoStep.HandlerRef),
				zap.String("eventName", event.Name),
				zap.String("hubName", event.HubName),
			)
			err = fmt.Errorf("panic in handler %s: %v", autoStep.HandlerRef, r)
		}
	}()

	handlerName := HandlerName(autoStep.HandlerRef)

	if handlerFn, exists := SupportedHandlers[handlerName]; exists {
		// TODO add event audit here
		if err := handlerFn(mdai, event, autoStep.Arguments); err != nil {
			return fmt.Errorf("handler %s failed: %w", handlerName, err)
		}
		return nil
	}
	return fmt.Errorf("handler %s not supported", handlerName)
}

func getEnvVariableWithDefault(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Initialize ValKeyClient with retry logic
	valkeyClient, err := initValKeyClient(ctx, logger)
	if err != nil {
		logger.Fatal("failed to get valkey client", zap.Error(err))
	}
	defer valkeyClient.Close()

	hub, err := initNatsEventHub()
	if err != nil {
		logger.Fatal("Failed to create EventHub", zap.Error(err))
	}
	defer hub.Close()

	configMgr, err := NewConfigMapManager(automationConfigMapNamePostfix)
	if err != nil {
		logger.Fatal("Failed to create ConfigMap manager", zap.Error(err))
	}
	defer configMgr.Cleanup()

	err = hub.Subscribe(ctx, ProcessEvent(ctx, valkeyClient, configMgr, logger))
	if err != nil {
		logger.Fatal("Failed to start event listener", zap.Error(err))
	}

	<-ctx.Done()
	logger.Info("Service shutting down")
}

func initValKeyClient(ctx context.Context, logger *zap.Logger) (valkey.Client, error) {
	valKeyEndpoint := getEnvVariableWithDefault(valkeyEndpointEnvVarKey, "")
	valkeyPassword := getEnvVariableWithDefault(valkeyPasswordEnvVarKey, "")

	logger.Info(fmt.Sprintf("Initializing valkey client with endpoint %s", valKeyEndpoint))

	initializer := func() (valkey.Client, error) {
		return valkey.NewClient(valkey.ClientOption{
			InitAddress: []string{valKeyEndpoint},
			Password:    valkeyPassword,
		})
	}

	return RetryInitializer(
		ctx,
		logger,
		"valkey client",
		initializer,
		3*time.Minute,
		5*time.Second,
	)
}

func initNatsEventHub() (*eventing.EventBus, error) {
	return eventing.New(eventing.Config{})
}
