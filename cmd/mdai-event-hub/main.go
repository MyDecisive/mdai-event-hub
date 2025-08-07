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
	operator "github.com/decisiveai/mdai-operator/api/v1"
	"github.com/valkey-io/valkey-go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	corev1 "k8s.io/api/core/v1"
)

const (
	valkeyAuditStreamExpiryMSEnvVarKey = "VALKEY_AUDIT_STREAM_EXPIRY_MS"
	mdaiHubEventHistoryStreamName      = "mdai_hub_event_history"
	defaultValkeyAuditStreamExpiry     = 30 * 24 * time.Hour
)

// ProcessEvent handles an MdaiEvent according to configured workflows.
func ProcessEvent(ctx context.Context, client valkey.Client, configMgr *dcorekube.ConfigMapController, logger *zap.Logger, auditAdapter *audit.AuditAdapter, handlerMap handlers.HandlerMap) eventing.HandlerInvoker {
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

		workflowMap := getWorkflowMap(logger, hubData)

		var workflowFound bool
		var steps []operator.AutomationStep
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
			err := safePerformAutomationStep(handlerMap, mdaiInterface, automationStep, event)

			if auditAdapter != nil {
				if auditErr := recordAuditEventFromMdaiEvent(ctx, logger, auditAdapter, event, automationStep, err == nil); auditErr != nil {
					logger.Error("Failed to write audit event for automation step",
						zap.String("hubName", event.HubName),
						zap.String("name", event.Name),
						zap.String("handlerRef", automationStep.HandlerRef),
						zap.String("eventCorrelationId", event.CorrelationID),
						zap.Error(err),
					)
				}
			}

			if err != nil {
				logger.Error("Automation step failed",
					zap.String("hubName", event.HubName),
					zap.String("name", event.Name),
					zap.String("handlerRef", automationStep.HandlerRef),
					zap.String("eventCorrelationId", event.CorrelationID),
					zap.Error(err),
				)
				return err
			}
		}
		return nil
	}
}

func safePerformAutomationStep(handlerMap handlers.HandlerMap, mdai handlers.MdaiInterface, autoStep operator.AutomationStep, event eventing.MdaiEvent) (err error) {
	// handle panics
	defer func() {
		if r := recover(); r != nil {
			mdai.Logger.Error(
				"Panic inside automation handler",
				zap.Reflect("panicValue", r),
				zap.String("handlerRef", autoStep.HandlerRef),
				zap.String("eventName", event.Name),
				zap.String("hubName", event.HubName),
				zap.String("eventCorrelationId", event.CorrelationID),
			)
			err = fmt.Errorf("panic in handler %s: %v", autoStep.HandlerRef, r)
		}
	}()

	handlerName := handlers.HandlerName(autoStep.HandlerRef)

	if handlerFn, exists := handlerMap[handlerName]; exists {
		if err := handlerFn(mdai, event, autoStep.Arguments); err != nil {
			return fmt.Errorf("handler %s failed: %w", handlerName, err)
		}
		return nil
	}
	return fmt.Errorf("handler %s not supported", handlerName)
}

func recordAuditEventFromMdaiEvent(ctx context.Context, logger *zap.Logger, auditAdapter *audit.AuditAdapter, event eventing.MdaiEvent, automationStep operator.AutomationStep, automationSucceeded bool) error {
	eventMap := map[string]string{
		"id":                     event.ID,
		"name":                   event.Name,
		"timestamp":              event.Timestamp.UTC().Format(time.RFC3339),
		"payload":                event.Payload,
		"source":                 event.Source,
		"sourceId":               event.SourceID,
		"correlation_id":         event.CorrelationID,
		"hub_name":               event.HubName,
		"automation_succeeded":   strconv.FormatBool(automationSucceeded),
		"automation_handler_ref": automationStep.HandlerRef,
	}
	logger.Info(
		"AUDIT: MdaiEvent handled",
		zap.String("mdai-logstream", "audit"),
		zap.Object("mdaiEvent", &event),
		zap.Bool("automation_succeeded", automationSucceeded),
		zap.String("automation_handler_ref", automationStep.HandlerRef),
	)
	return auditAdapter.InsertAuditLogEventFromMap(ctx, eventMap)
}

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

	handlerMap := handlers.GetSupportedHandlers(nil) // TODO what is this?  why it's nil?

	err = subscriber.Subscribe(ctx, eventing.AlertConsumerGroupName, "alert", ProcessEvent(ctx, valkeyClient, configMgr, logger, auditAdapter, handlerMap))
	if err != nil {
		logger.Fatal("Failed to start Alerts event listener", zap.Error(err))
	}

	// TODO add Vars and Mdai event listeners as well.
	// is it okay to process different event types in parallel?

	<-ctx.Done()
	logger.Info("Service shutting down")
}

func getWorkflowMap(logger *zap.Logger, hubData []map[string]string) map[string][]operator.AutomationStep {
	result := make(map[string][]operator.AutomationStep, len(hubData))

	for _, v := range hubData {
		for k, v := range v {
			var workflow []operator.AutomationStep

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
