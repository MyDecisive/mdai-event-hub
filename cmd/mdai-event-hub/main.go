package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/decisiveai/mdai-data-core/audit"
	datacore "github.com/decisiveai/mdai-data-core/handlers"
	dcorekube "github.com/decisiveai/mdai-data-core/kube"
	"github.com/decisiveai/mdai-event-hub/internal/eventhub"
	"github.com/decisiveai/mdai-event-hub/internal/handlers"
	internalvalkey "github.com/decisiveai/mdai-event-hub/internal/valkey"
	"github.com/decisiveai/mdai-event-hub/pkg/eventing"
	v1 "github.com/decisiveai/mdai-operator/api/v1"
	"github.com/valkey-io/valkey-go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	corev1 "k8s.io/api/core/v1"
)

var logger *zap.Logger

const (
	valkeyEndpointEnvVarKey            = "VALKEY_ENDPOINT"
	valkeyPasswordEnvVarKey            = "VALKEY_PASSWORD"
	valkeyAuditStreamExpiryMSEnvVarKey = "VALKEY_AUDIT_STREAM_EXPIRY_MS"
	mdaiHubEventHistoryStreamName      = "mdai_hub_event_history"
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

// ProcessEvent handles an MdaiEvent according to configured workflows.
func ProcessEvent(ctx context.Context, client valkey.Client, configMgr *dcorekube.ConfigMapController, logger *zap.Logger, auditAdapter *audit.AuditAdapter) eventing.HandlerInvoker {
	dataAdapter := datacore.NewHandlerAdapter(client, logger)

	mdaiInterface := handlers.MdaiInterface{
		Data:   dataAdapter,
		Logger: logger,
	}

	return func(event eventing.MdaiEvent) error {
		hubName := event.HubName
		if hubName == "" {
			return errors.New("no hub name provided")
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

		hubData, err := configMgr.GetHubData(event.HubName)
		if err != nil {
			return fmt.Errorf("error getting ConfigMap data for hub %s: %w", event.HubName, err)
		}

		workflowMap := getWorkflowMap(hubData)

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
			logger.Error("No configured automation for event", zap.String("name", event.Name))
			return fmt.Errorf("no configured automation for event: %s", event.Name)
		}

		for _, automationStep := range steps {
			err := safePerformAutomationStep(mdaiInterface, automationStep, event)

			if auditAdapter != nil {
				if auditErr := recordAuditEventFromMdaiEvent(ctx, logger, auditAdapter, event, automationStep, err == nil); auditErr != nil {
					logger.Error("Failed to write audit event for automation step",
						zap.String("hubName", event.HubName),
						zap.String("name", event.Name),
						zap.String("handlerRef", automationStep.HandlerRef),
						zap.String("eventCorrelationId", event.CorrelationId),
						zap.Error(err),
					)
				}
			}

			if err != nil {
				logger.Error("Automation step failed",
					zap.String("hubName", event.HubName),
					zap.String("name", event.Name),
					zap.String("handlerRef", automationStep.HandlerRef),
					zap.String("eventCorrelationId", event.CorrelationId),
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
				zap.String("eventCorrelationId", event.CorrelationId),
			)
			err = fmt.Errorf("panic in handler %s: %v", autoStep.HandlerRef, r)
		}
	}()

	handlerName := handlers.HandlerName(autoStep.HandlerRef)

	if handlerFn, exists := handlers.SupportedHandlers[handlerName]; exists {
		if err := handlerFn(mdai, event, autoStep.Arguments); err != nil {
			return fmt.Errorf("handler %s failed: %w", handlerName, err)
		}
		return nil
	}
	return fmt.Errorf("handler %s not supported", handlerName)
}

func recordAuditEventFromMdaiEvent(ctx context.Context, logger *zap.Logger, auditAdapter *audit.AuditAdapter, event eventing.MdaiEvent, automationStep v1.AutomationStep, automationSucceeded bool) error {
	eventMap := map[string]string{
		"id":                     event.Id,
		"name":                   event.Name,
		"timestamp":              event.Timestamp.UTC().Format(time.RFC3339),
		"payload":                event.Payload,
		"source":                 event.Source,
		"sourceId":               event.SourceId,
		"correlation_id":         event.CorrelationId,
		"hub_name":               event.HubName,
		"automation_succeeded":   strconv.FormatBool(automationSucceeded),
		"automation_handler_ref": automationStep.HandlerRef,
	}
	logger.Info("AUDIT: MdaiEvent handled", zap.String("mdai-logstream", "audit"), zap.Any("mdaiEvent", eventMap))
	return auditAdapter.InsertAuditLogEventFromMap(ctx, eventMap)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Initialize ValKeyClient with retry logic
	valkeyClient, err := internalvalkey.Init(ctx, logger)
	if err != nil {
		logger.Fatal("failed to get valkey client", zap.Error(err))
	}
	defer valkeyClient.Close()

	valkeyAuditStreamExpiry := 30 * 24 * time.Hour
	valkeyStreamExpiryMsStr := os.Getenv(valkeyAuditStreamExpiryMSEnvVarKey)
	if valkeyStreamExpiryMsStr != "" {
		envExpiryMs, err := strconv.Atoi(valkeyStreamExpiryMsStr)
		if err != nil {
			logger.Fatal("Failed to parse valkeyStreamExpiryMs env var", zap.Error(err))
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
		if err := subscriber.Close(); err != nil {
			logger.Warn("failed to close NATS subscriber", zap.Error(err))
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
	if err := configMgr.Run(); err != nil {
		logger.Fatal("Failed to run  ConfigMap manager", zap.Error(err))
	}
	defer configMgr.Stop()

	err = subscriber.Subscribe(ctx, ProcessEvent(ctx, valkeyClient, configMgr, logger, auditAdapter))
	if err != nil {
		logger.Fatal("Failed to start event listener", zap.Error(err))
	}

	<-ctx.Done()
	logger.Info("Service shutting down")
}

func getWorkflowMap(hubData []map[string]string) map[string][]v1.AutomationStep {
	result := make(map[string][]v1.AutomationStep, len(hubData))

	for _, v := range hubData {
		for k, v := range v {
			var workflow []v1.AutomationStep

			dec := json.NewDecoder(strings.NewReader(v))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&workflow); err != nil {
				logger.Warn("could not unmarshall workflow", zap.String("key", k), zap.Error(err))
				continue
			}

			result[k] = workflow
		}
	}
	return result
}
