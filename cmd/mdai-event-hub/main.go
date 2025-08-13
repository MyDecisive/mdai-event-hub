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
	"github.com/decisiveai/mdai-data-core/events"
	"github.com/decisiveai/mdai-data-core/events/triggers"
	datacore "github.com/decisiveai/mdai-data-core/handlers"
	dcorekube "github.com/decisiveai/mdai-data-core/kube"
	"github.com/decisiveai/mdai-event-hub/internal/eventhub"
	"github.com/decisiveai/mdai-event-hub/internal/handlers"
	internalvalkey "github.com/decisiveai/mdai-event-hub/internal/valkey"
	"github.com/decisiveai/mdai-event-hub/pkg/eventing"
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

// ProcessAlertingEvent handles an MdaiEvent according to configured workflows.
func ProcessAlertingEvent(ctx context.Context, client valkey.Client, configMgr *dcorekube.ConfigMapController, logger *zap.Logger, auditAdapter *audit.AuditAdapter, handlerMap handlers.HandlerMap) eventing.HandlerInvoker {
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
		logger.Info("Processing alerting event for hub",
			zap.String("hubName", event.HubName),
			zap.String("eventName", event.Name),
		)

		if event.Source != eventing.PrometheusAlertsEventSource {
			logger.Error("Unsupported Alerts event source", zap.String("source", event.Source), zap.String("eventName", event.Name), zap.String("eventID", event.ID))
			return errors.New("unsupported Alerts event source")
		}

		hubData, err := configMgr.GetHubData(event.HubName)
		if err != nil {
			return fmt.Errorf("error getting ConfigMap data for hub %s: %w", event.HubName, err)
		}

		// matches event type which should be alert name plus status with rules keys
		baseRules := func(eventName string, rulesMap map[string]events.Rule) []events.Rule {
			alertName, alertStatus, _ := strings.Cut(eventName, ".")

			eventCtx := triggers.Context{
				Alert: &triggers.AlertCtx{Name: alertName, Status: alertStatus},
			}

			matched := make([]events.Rule, 0, len(rulesMap))
			for _, rule := range rulesMap {
				if at, ok := rule.Trigger.(*triggers.AlertTrigger); ok && at != nil && at.Match(eventCtx) {
					matched = append(matched, rule)
				}
			}
			return matched
		}

		rules := baseRules(event.Name, getRulesMap(logger, hubData))
		if len(rules) == 0 {
			err := fmt.Errorf("no matching configured automation for event: %s", event.Name)
			logger.Error("No configured automation for event", zap.String("type", event.Name), zap.Error(err))
			return err
		}

		for _, rule := range rules {
			// match the event with the rule

			err := safePerformAutomationStep(handlerMap, mdaiInterface, rule, event)

			if auditAdapter != nil {
				if auditErr := recordAuditEventFromMdaiEvent(ctx, logger, auditAdapter, event, rule, err == nil); auditErr != nil {
					logger.Error("Failed to write audit event for automation step",
						zap.String("hubName", event.HubName),
						zap.String("name", event.Name),
						zap.String("rule", rule.Name),
						zap.String("eventCorrelationId", event.CorrelationID),
						zap.Error(err),
					)
				}
			}

			if err != nil {
				logger.Error("Automation step failed",
					zap.String("hubName", event.HubName),
					zap.String("name", event.Name),
					// FIXME
					//zap.String("handlerRef", rule.HandlerRef),
					zap.String("eventCorrelationId", event.CorrelationID),
					zap.Error(err),
				)
				return err
			}
		}
		return nil
	}
}

// ProcessVariableEvent handles an MdaiEvent according to configured workflows.
func ProcessVariableEvent(ctx context.Context, client valkey.Client, logger *zap.Logger) eventing.HandlerInvoker {
	dataAdapter := datacore.NewHandlerAdapter(client, logger)
	mdaiInterface := handlers.MdaiInterface{
		Data:   dataAdapter,
		Logger: logger,
	}

	return func(event eventing.MdaiEvent) error {
		logger.Info("Processing variable event", zap.String("hubName", event.HubName), zap.String("eventName", event.Name))

		err := handlers.HandleManualVariablesActions(ctx, mdaiInterface, event)
		if err != nil {
			return err
		}

		logger.Info("Variable event processed successfully", zap.String("hubName", event.HubName), zap.String("eventName", event.Name))
		return nil
	}
}

func safePerformAutomationStep(handlerMap handlers.HandlerMap, mdai handlers.MdaiInterface, rule events.Rule, event eventing.MdaiEvent) (err error) {
	// handle panics
	defer func() {
		if r := recover(); r != nil {
			mdai.Logger.Error(
				"Panic inside automation handler",
				zap.Reflect("panicValue", r), // FIXME
				//zap.Inline("trigger", rule.Trigger),
				zap.String("eventName", event.Name),
				zap.String("hubName", event.HubName),
				zap.String("eventCorrelationId", event.CorrelationID),
			)
			err = fmt.Errorf("panic in handler %s: %v", rule.Name, r)
		}
	}()

	// FIXME
	mdai.Logger.Info("<< Executing automation step >>")
	return nil
	//handlerName := handlers.HandlerName(rule.Name)

	//if handlerFn, exists := handlerMap[handlerName]; exists {
	//	if err := handlerFn(mdai, event, rule.Arguments); err != nil {
	//		return fmt.Errorf("handler %s failed: %w", handlerName, err)
	//	}
	//	return nil
	//}
	//return fmt.Errorf("handler %s not supported", handlerName)
}

func recordAuditEventFromMdaiEvent(ctx context.Context, logger *zap.Logger, auditAdapter *audit.AuditAdapter, event eventing.MdaiEvent, rule events.Rule, automationSucceeded bool) error {
	eventMap := map[string]string{
		"id":                   event.ID,
		"name":                 event.Name,
		"timestamp":            event.Timestamp.UTC().Format(time.RFC3339),
		"payload":              event.Payload,
		"source":               event.Source,
		"sourceId":             event.SourceID,
		"correlation_id":       event.CorrelationID,
		"hub_name":             event.HubName,
		"automation_succeeded": strconv.FormatBool(automationSucceeded),
		"automation_name":      rule.Name,
	}
	logger.Info(
		"AUDIT: MdaiEvent handled",
		zap.String("mdai-logstream", "audit"),
		zap.Object("mdaiEvent", &event),
		zap.Bool("automation_succeeded", automationSucceeded),
		zap.String("automation_name", rule.Name),
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

	err = subscriber.Subscribe(ctx, eventing.AlertConsumerGroupName, "alert", ProcessAlertingEvent(ctx, valkeyClient, configMgr, logger, auditAdapter, handlerMap))
	if err != nil {
		logger.Fatal("Failed to start Alerts event listener", zap.Error(err))
	}

	// is it okay to process different event types in parallel?
	err = subscriber.Subscribe(ctx, eventing.VarsConsumerGroupName, "var", ProcessVariableEvent(ctx, valkeyClient, logger))
	if err != nil {
		logger.Fatal("Failed to start Alerts event listener", zap.Error(err))
	}

	<-ctx.Done()
	logger.Info("Service shutting down")
}

func getRulesMap(logger *zap.Logger, hubData []map[string]string) map[string]events.Rule {
	result := make(map[string]events.Rule, len(hubData))

	for _, data := range hubData {
		for ruleName, ruleJson := range data { // key is the rule name
			// Decode into a wire struct first; Trigger stays raw.
			var wireRule struct {
				Name     string           `json:"name"`
				Trigger  json.RawMessage  `json:"trigger"`
				Commands []events.Command `json:"commands"`
			}
			dec := json.NewDecoder(strings.NewReader(ruleJson))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&wireRule); err != nil {
				logger.Warn("could not unmarshall rule", zap.String("key", ruleName), zap.Error(err))
				continue
			}

			trigger, err := triggers.BuildTrigger(wireRule.Trigger) // returns a *concrete that implements Trigger
			if err != nil || trigger == nil {
				logger.Warn("invalid trigger", zap.String("key", ruleName), zap.Error(err))
				continue
			}

			var rule events.Rule
			rule.Name = wireRule.Name
			if rule.Name == "" {
				rule.Name = ruleName // fall back to ConfigMap key
			}
			rule.Trigger = trigger // store pointer so only one assertion form exists later
			rule.Commands = wireRule.Commands

			result[ruleName] = rule
		}
	}
	return result
}
