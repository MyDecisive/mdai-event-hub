package eventhub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/decisiveai/mdai-data-core/audit"
	"github.com/decisiveai/mdai-data-core/events"
	"github.com/decisiveai/mdai-data-core/events/triggers"
	"github.com/decisiveai/mdai-data-core/kube"
	"github.com/decisiveai/mdai-event-hub/internal/handlers"
	"github.com/decisiveai/mdai-event-hub/pkg/eventing"
	"github.com/decisiveai/mdai-event-hub/pkg/eventing/nats"
	"go.uber.org/zap"
)

type EventHubDeps struct {
	Logger              *zap.Logger
	Subscriber          *nats.EventSubscriber
	ConfigMapController *kube.ConfigMapController
	AuditAdapter        *audit.AuditAdapter
	Mdai                handlers.MdaiInterface
}

// ProcessAlertingEvent handles an MdaiEvent according to configured workflows.
func ProcessAlertingEvent(ctx context.Context, mdai handlers.MdaiInterface) eventing.HandlerInvoker {
	return func(event eventing.MdaiEvent) error {
		hubName := event.HubName
		if hubName == "" {
			return errors.New("no hub name provided")
		}
		mdai.Logger.Info("Processing alerting event for hub",
			zap.String("hubName", event.HubName),
			zap.String("eventName", event.Name),
		)

		if event.Source != eventing.PrometheusAlertsEventSource {
			mdai.Logger.Error("Unsupported Alerts event source", zap.String("source", event.Source), zap.String("eventName", event.Name), zap.String("eventID", event.ID))
			return errors.New("unsupported Alerts event source")
		}

		automationConfig, err := mdai.ConfigMapController.GetConfigMapByHubName(event.HubName)
		if err != nil {
			return fmt.Errorf("error getting ConfigMap data for hub %s: %w", event.HubName, err)
		}

		// we need a hub namespace to get secrets and configMaps
		mdai.Namespace = automationConfig.Namespace
		hubData := automationConfig.Data

		// matches event type which should be alert name plus status with rules keys
		matchedRules := func(eventName string, rulesMap map[string]events.Rule) []events.Rule {
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

		// TODO change informer logic to cache rules so we don't need to process it here every time
		rules := matchedRules(event.Name, getRulesMap(mdai.Logger, hubData))
		if len(rules) == 0 {
			mdai.Logger.Warn("No configured automation for event, skipping", zap.String("eventID", event.ID), zap.String("eventName", event.Name))
			return nil
		}

		// this is temporarily connecting new subjects to old handlers
		// one event can trigger several rules
		for _, rule := range rules {
			if err := processRuleForAlertingEvent(ctx, event, mdai, rule, mdai.AuditAdapter); err != nil {
				return err
			}
		}
		return nil
	}
}

func processRuleForAlertingEvent(ctx context.Context, event eventing.MdaiEvent, mdai handlers.MdaiInterface, rule events.Rule, auditAdapter *audit.AuditAdapter) error {
	mdai.Logger.Info("Processing automation rule", zap.String("rule", rule.Name))
	for _, cmd := range rule.Commands {
		cmdType := cmd.Type
		mdai.Logger.Info("Processing automation command", zap.String("commandType", cmdType))
		var err error
		switch cmdType {
		// TODO make a data-core constants
		case "variable.set.add":
			err = safePerformAutomationStep(handlers.HandleAddNoisyServiceToSet, mdai, cmd, event)
		case "variable.set.remove":
			err = safePerformAutomationStep(handlers.HandleRemoveNoisyServiceFromSet, mdai, cmd, event)
		case "webhook.call":
			err = safePerformAutomationStep(handlers.HandleCallSlackWebhookFn, mdai, cmd, event)
		default:
			mdai.Logger.Error("Unsupported command type", zap.String("commandType", cmdType))
			return fmt.Errorf("unsupported command type: %s", cmdType)
		}

		if auditAdapter != nil {
			if auditErr := recordAuditEventFromMdaiEvent(ctx, mdai.Logger, auditAdapter, event, rule, err == nil); auditErr != nil {
				mdai.Logger.Error("Failed to write audit event for automation step",
					zap.String("hubName", event.HubName),
					zap.String("name", event.Name),
					zap.String("rule", rule.Name),
					zap.String("eventCorrelationId", event.CorrelationID),
					zap.Error(auditErr),
				)
			}
		}

		if err != nil {
			mdai.Logger.Error("Automation step failed",
				zap.String("hubName", event.HubName),
				zap.String("name", event.Name),
				zap.String("rule", rule.Name),
				zap.String("eventCorrelationId", event.CorrelationID),
				zap.Error(err),
			)
			return err
		}
	}
	return nil
}

// ProcessVariableEvent handles an MdaiEvent according to configured workflows.
func ProcessVariableEvent(ctx context.Context, mdai handlers.MdaiInterface) eventing.HandlerInvoker {
	return func(event eventing.MdaiEvent) error {
		mdai.Logger.Info("Processing variable event", zap.String("hubName", event.HubName), zap.String("eventName", event.Name))

		if event.Source != eventing.ManualVariablesEventSource {
			mdai.Logger.Error("Unsupported manual variable update event source", zap.String("source", event.Source), zap.String("eventName", event.Name), zap.String("eventID", event.ID))
			return errors.New("unsupported manual variable update event source")
		}

		// TODO issue a command event here
		if err := handlers.HandleManualVariablesActions(ctx, mdai, event); err != nil {
			mdai.Logger.Error("Error processing manual variable update event", zap.String("hubName", event.HubName), zap.String("eventName", event.Name), zap.String("eventID", event.ID), zap.Error(err))
			return err
		}

		mdai.Logger.Info("Variable event processed successfully", zap.String("hubName", event.HubName), zap.String("eventName", event.Name))
		return nil
	}
}

func safePerformAutomationStep(handlerFn handlers.HandlerFunc, mdai handlers.MdaiInterface, command events.Command, event eventing.MdaiEvent) (err error) {
	// handle panics
	defer func() {
		if r := recover(); r != nil {
			mdai.Logger.Error(
				"Panic inside automation handler",
				zap.Reflect("panicValue", r),
				zap.String("command", command.Type),
				zap.String("eventName", event.Name),
				zap.String("hubName", event.HubName),
				zap.String("eventCorrelationId", event.CorrelationID),
			)
			err = fmt.Errorf("panic executing command %s: %v", command.Type, r)
		}
	}()

	mdai.Logger.Info("<< Executing automation step >>")

	if err := handlerFn(mdai, event, command.Inputs); err != nil {
		return fmt.Errorf("command %s failed: %w", command.Type, err)
	}
	return nil
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

func getRulesMap(logger *zap.Logger, hubData map[string]string) map[string]events.Rule {
	result := make(map[string]events.Rule, len(hubData))

	for ruleName, ruleJSON := range hubData { // key is the rule name
		// Decode into a wire struct first; Trigger stays raw.
		var wireRule struct {
			Name     string           `json:"name"`
			Trigger  json.RawMessage  `json:"trigger"`
			Commands []events.Command `json:"commands"`
		}
		dec := json.NewDecoder(strings.NewReader(ruleJSON))
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

	return result
}
