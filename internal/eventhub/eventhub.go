package eventhub

import (
	"context"
	"encoding/json"
	"fmt"
	"k8s.io/client-go/dynamic"
	"runtime/debug"
	"strings"

	"github.com/decisiveai/mdai-data-core/audit"
	"github.com/decisiveai/mdai-data-core/eventing"
	"github.com/decisiveai/mdai-data-core/eventing/rule"
	"github.com/decisiveai/mdai-data-core/eventing/triggers"
	"github.com/decisiveai/mdai-data-core/interpolation"
	"github.com/decisiveai/mdai-data-core/kube"
	auditutils "github.com/decisiveai/mdai-event-hub/internal/audit"
	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"
)

type EventHub struct {
	Logger              *zap.Logger
	VarsAdapter         VarDeps
	Kube                kubernetes.Interface
	DynamicClient       dynamic.Interface
	AuditAdapter        *audit.AuditAdapter
	ConfigMapController kube.ConfigMapStore
	InterpolationEngine *interpolation.Engine
	HopLimit            int
}

func WithRecover(log *zap.Logger, next eventing.HandlerInvoker) eventing.HandlerInvoker {
	return func(event eventing.MdaiEvent) (err error) {
		defer func() {
			if r := recover(); r != nil {
				log.Error("handler panic",
					zap.String("panic", fmt.Sprint(r)),
					zap.String("eventID", event.ID),
					zap.String("correlation_id", event.CorrelationID),
					zap.ByteString("stack", debug.Stack()),
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

	varsEventType     = "vars"
	alertingEventType = "alerting"
)

// ProcessVariableEvent handles an MdaiEvent for manual variables' workflow.
func (h *EventHub) ProcessVariableEvent(ctx context.Context) eventing.HandlerInvoker {
	return func(event eventing.MdaiEvent) error {
		logger := h.withEvent(event, varsEventType)
		logger.Info("Processing variable event")

		if event.Source != eventing.ManualVariablesEventSource {
			logger.Warn("Unsupported manual variable update event source, skipping")
			return nil // non-transient
		}

		cmds, err := h.VarsAdapter.BuildCommandFromEvent(event)
		if err != nil {
			logger.Error("Error building command from manual variable event", zap.Error(err))
			return err
		}

		// TODO this is bulk update for the same variable, we can introduce a bulk command for such cases to reduce possibility of multiple collector restarts
		if err := h.processCommandsForEvent(ctx, event, cmds, "default", nil, varsEventType); err != nil {
			logger.Error("Error processing manual variable update event", zap.Error(err))
			return err
		}

		logger.Info("Variable event processed successfully")
		return nil
	}
}

// ProcessAlertingEvent handles an MdaiEvent according to configured workflows.
func (h *EventHub) ProcessAlertingEvent(ctx context.Context) eventing.HandlerInvoker {
	return func(event eventing.MdaiEvent) error {
		logger := h.withEvent(event, alertingEventType)
		logger.Info("Processing alerting event")

		if err := event.Validate(); err != nil {
			return err
		}

		if event.Source != eventing.PrometheusAlertsEventSource {
			logger.Warn("Unsupported Alerts event source; skipping")
			return nil // non-transient; don’t send to DLQ
		}

		return h.processMdaiEvent(ctx, event, logger, h.matchedRulesByAlertCtx)
	}
}

func (h *EventHub) ProcessTriggerEvent(ctx context.Context) eventing.HandlerInvoker {
	return func(event eventing.MdaiEvent) error {
		logger := h.withEvent(event, varsEventType)
		logger.Info("Processing trigger event")

		return h.processMdaiEvent(ctx, event, logger, h.matchedRulesByVariableCtx)
	}
}

func (h *EventHub) processMdaiEvent(ctx context.Context, event eventing.MdaiEvent, logger *zap.Logger, matchRules RuleMatcher) error {
	// To prevent infinite processing loops, check the event's RecursionDepth.
	// This value is incremented for each new event generated from a previously consumed one.
	// If the depth exceeds the configured HopLimit, we stop processing.
	hops := event.RecursionDepth
	if hops >= h.HopLimit {
		logger.Warn("hop limit reached, stopping processing to prevent infinite loop", zap.Int("hops", hops))
		return nil
	}

	automationConfig, err := h.ConfigMapController.GetConfigMapByHubName(event.HubName)
	if err != nil {
		return fmt.Errorf("error getting ConfigMap data for hub %s: %w", event.HubName, err)
	}

	// TODO change informer logic to cache rules so we don't need to process it here every time
	rules := matchRules(event, getRulesMap(h.Logger, automationConfig.Data))
	if len(rules) == 0 {
		logger.Info("No configured automation for event, skipping", zap.String("event_name", event.Name))
		if auditErr := auditutils.RecordAuditEventFromMdaiEvent(ctx, h.Logger, h.AuditAdapter, event, nil, true); auditErr != nil {
			h.Logger.Error("audit write failed", zap.Error(auditErr))
		}
		return nil
	}

	payloadData, err := processEventPayload(event)
	if err != nil {
		return fmt.Errorf("parse payload: %w", err)
	}

	// one event can trigger several rules
	for _, r := range rules {
		clog := h.withEvent(event, alertingEventType).With(zap.String(fldRule, r.Name))
		clog.Info("Processing rule")
		err := h.processCommandsForEvent(ctx, event, r.Commands, automationConfig.Namespace, payloadData, alertingEventType)
		success := err == nil
		if auditErr := auditutils.RecordAuditEventFromMdaiEvent(ctx, h.Logger, h.AuditAdapter, event, &r, success); auditErr != nil {
			clog.Error("audit write failed", zap.Error(auditErr))
		}
		if err != nil {
			return err
		}
	}
	return nil
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

type RuleMatcher func(event eventing.MdaiEvent, rules map[string]rule.Rule) []rule.Rule

// matchedRulesByAlertCtx matches event type which should be alert name plus status with rules keys.
func (*EventHub) matchedRulesByAlertCtx(event eventing.MdaiEvent, rulesMap map[string]rule.Rule) []rule.Rule {
	alertName, alertStatus, _ := strings.Cut(event.Name, ".")

	eventCtx := triggers.Context{
		Alert: &triggers.AlertCtx{Name: alertName, Status: alertStatus},
	}

	matched := make([]rule.Rule, 0, len(rulesMap))
	for _, r := range rulesMap { // iterate over all alerting rules, order is not guaranteed
		if at, ok := r.Trigger.(*triggers.AlertTrigger); ok && at != nil && at.Match(eventCtx) {
			matched = append(matched, r)
		}
	}
	return matched
}

// matchedRulesByVariableCtx matches event type which should be alert name plus status with rules keys.
func (h *EventHub) matchedRulesByVariableCtx(event eventing.MdaiEvent, rulesMap map[string]rule.Rule) []rule.Rule {
	var payload eventing.VariablesActionPayload
	if err := json.Unmarshal([]byte(event.Payload), &payload); err != nil {
		// payload is malformed → nothing matches
		h.Logger.Warn("could not unmarshal variable event payload",
			zap.String(fldEventID, event.ID),
			zap.String(fldCorrelationID, event.CorrelationID),
			zap.Error(err),
		)
		return nil
	}

	eventCtx := triggers.Context{
		Variable: &triggers.VariableCtx{Name: payload.VariableRef, UpdateType: payload.Operation},
	}

	matched := make([]rule.Rule, 0, len(rulesMap))
	for _, r := range rulesMap { // iterate over all rules, order is not guaranteed
		if at, ok := r.Trigger.(*triggers.VariableTrigger); ok && at != nil && at.Match(eventCtx) {
			matched = append(matched, r)
		}
	}
	return matched
}

func processEventPayload(event eventing.MdaiEvent) (map[string]any, error) {
	if event.Payload == "" {
		return map[string]any{}, nil
	}

	var payloadData map[string]any

	if err := json.Unmarshal([]byte(event.Payload), &payloadData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal payload %q: %w", event.Payload, err)
	}

	return payloadData, nil
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
