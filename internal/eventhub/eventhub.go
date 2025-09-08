package eventhub

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/decisiveai/mdai-data-core/audit"
	"github.com/decisiveai/mdai-data-core/eventing"
	"github.com/decisiveai/mdai-data-core/eventing/rule"
	"github.com/decisiveai/mdai-data-core/eventing/triggers"
	"github.com/decisiveai/mdai-data-core/kube"
	auditutils "github.com/decisiveai/mdai-event-hub/internal/audit"
	"github.com/decisiveai/mdai-event-hub/internal/handlers"
	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"
)

type EventHub struct {
	Logger              *zap.Logger
	HandlerAdapter      handlers.IHandlerAdapter
	Kube                kubernetes.Interface
	AuditAdapter        *audit.AuditAdapter
	ConfigMapController *kube.ConfigMapController
}

func (h *EventHub) GetLogger() *zap.Logger {
	return h.Logger
}

// GetHandlerAdapter exposes the adapter to VarDeps.
//
//nolint:ireturn // VarDeps intentionally abstracts via interface for tests and decoupling
func (h *EventHub) GetHandlerAdapter() handlers.IHandlerAdapter {
	return h.HandlerAdapter
}

func WithRecover(log *zap.Logger, next eventing.HandlerInvoker) eventing.HandlerInvoker {
	return func(event eventing.MdaiEvent) (err error) {
		defer func() {
			if r := recover(); r != nil {
				log.Error("handler panic",
					zap.String("panic", fmt.Sprint(r)),
					zap.String("eventID", event.ID),
				)
				err = fmt.Errorf("panic: %v", r)
			}
		}()
		return next(event)
	}
}

const (
	fldComponent     = "component"
	fldEventID       = "event_id"
	fldEventName     = "event_name"
	fldHubName       = "hub_name"
	fldSource        = "source"
	fldCorrelationID = "correlation_id"
	fldRule          = "rule"
	fldCommandType   = "command_type"
)

// ProcessAlertingEvent handles an MdaiEvent according to configured workflows.
func (h *EventHub) ProcessAlertingEvent(ctx context.Context) eventing.HandlerInvoker {
	return func(event eventing.MdaiEvent) error {
		logger := h.withEvent(event, "alerting")
		logger.Info("Processing alerting event for hub")

		if err := event.Validate(); err != nil {
			return err
		}

		if event.Source != eventing.PrometheusAlertsEventSource {
			logger.Warn("Unsupported Alerts event source; skipping")
			return nil // non-transient; don’t send to DLQ
		}

		automationConfig, err := h.ConfigMapController.GetConfigMapByHubName(event.HubName)
		if err != nil {
			return fmt.Errorf("error getting ConfigMap data for hub %s: %w", event.HubName, err)
		}

		// TODO change informer logic to cache rules so we don't need to process it here every time
		rules := matchedRules(event.Name, getRulesMap(logger, automationConfig.Data))
		if len(rules) == 0 {
			logger.Warn("No configured automation for event, skipping")
			return nil
		}

		payloadData, err := handlers.ProcessEventPayload(event)
		if err != nil {
			return fmt.Errorf("parse payload: %w", err)
		}

		// one event can trigger several rules
		for _, r := range rules {
			if err := h.processRuleForAlertingEvent(ctx, event, r, automationConfig.Namespace, payloadData); err != nil {
				return err
			}
		}
		return nil
	}
}

// ProcessVariableEvent handles an MdaiEvent according to configured workflows.
func (h *EventHub) ProcessVariableEvent(ctx context.Context) eventing.HandlerInvoker {
	return func(event eventing.MdaiEvent) error {
		logger := h.withEvent(event, "vars")
		logger.Info("Processing variable event")

		if event.Source != eventing.ManualVariablesEventSource {
			logger.Warn("Unsupported manual variable update event source,skipping")
			return nil // non-transient
		}

		// TODO issue a command event here
		if err := handlers.HandleManualVariablesActions(ctx, h, event); err != nil {
			logger.Error("Error processing manual variable update event", zap.Error(err))
			return err
		}

		logger.Info("Variable event processed successfully")
		return nil
	}
}

// withEvent returns a child logger enriched with stable event fields.
func (h *EventHub) withEvent(e eventing.MdaiEvent, component string) *zap.Logger {
	return h.Logger.With(
		zap.String(fldComponent, component),
		zap.String(fldEventID, e.ID),
		zap.String(fldEventName, e.Name),
		zap.String(fldHubName, e.HubName),
		zap.String(fldSource, e.Source),
		zap.String(fldCorrelationID, e.CorrelationID),
	)
}

// matchedRules matches event type which should be alert name plus status with rules keys.
func matchedRules(eventName string, rulesMap map[string]rule.Rule) []rule.Rule {
	alertName, alertStatus, _ := strings.Cut(eventName, ".")

	eventCtx := triggers.Context{
		Alert: &triggers.AlertCtx{Name: alertName, Status: alertStatus},
	}

	matched := make([]rule.Rule, 0, len(rulesMap))
	for _, r := range rulesMap {
		if at, ok := r.Trigger.(*triggers.AlertTrigger); ok && at != nil && at.Match(eventCtx) {
			matched = append(matched, r)
		}
	}
	return matched
}

func (h *EventHub) processRuleForAlertingEvent(ctx context.Context, event eventing.MdaiEvent, r rule.Rule, namespace string, payloadData map[string]any) error {
	logger := h.withEvent(event, "alerting").With(zap.String(fldRule, r.Name))
	logger.Info("Processing rule")

	reg := h.registry()
	for _, cmd := range r.Commands {
		clog := logger.With(zap.String(fldCommandType, cmd.Type))
		clog.Info("Processing command")

		handler, ok := reg[cmd.Type]
		if !ok {
			clog.Error("unsupported command type")
			return fmt.Errorf("unsupported command type: %s", cmd.Type)
		}

		err := handler(ctx, event, namespace, cmd, payloadData)

		if auditErr := auditutils.RecordAuditEventFromMdaiEvent(ctx, h.Logger, h.AuditAdapter, event, r, err == nil); auditErr != nil {
			clog.Error("audit write failed", zap.Error(auditErr))
		}

		if err != nil {
			clog.Error("command failed", zap.Error(err))
			return err
		}
	}

	return nil
}

func getRulesMap(logger *zap.Logger, hubData map[string]string) map[string]rule.Rule {
	result := make(map[string]rule.Rule, len(hubData))

	for ruleName, ruleJSON := range hubData { // key is the rule name
		// Decode into a wire struct first; Trigger stays raw.
		var wireRule struct {
			Name     string          `json:"name"`
			Trigger  json.RawMessage `json:"trigger"`
			Commands []rule.Command  `json:"commands"`
		}
		dec := json.NewDecoder(strings.NewReader(ruleJSON))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&wireRule); err != nil {
			logger.Warn("could not unmarshal rule", zap.String("key", ruleName), zap.Error(err))
			continue
		}

		trigger, err := triggers.BuildTrigger(wireRule.Trigger) // returns a *concrete that implements Trigger
		if err != nil || trigger == nil {
			logger.Warn("invalid trigger", zap.String("key", ruleName), zap.Error(err))
			continue
		}

		var r rule.Rule
		r.Name = wireRule.Name
		if r.Name == "" {
			r.Name = ruleName // fall back to ConfigMap key
		}
		r.Trigger = trigger // store pointer so only one assertion form exists later
		r.Commands = wireRule.Commands

		result[ruleName] = r
	}

	return result
}

var _ handlers.VarDeps = (*EventHub)(nil)
