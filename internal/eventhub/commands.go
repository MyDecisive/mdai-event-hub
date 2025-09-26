package eventhub

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/decisiveai/mdai-data-core/eventing"
	"github.com/decisiveai/mdai-data-core/eventing/rule"
	mdaiv1 "github.com/decisiveai/mdai-operator/api/v1"
	"go.uber.org/zap"
)

type cmdHandler func(h *EventHub, ctx context.Context, ev eventing.MdaiEvent, ns string, cmd rule.Command, payload map[string]any) error

//nolint:gochecknoglobals
var commandDispatch = map[rule.CommandType]cmdHandler{
	rule.CmdVarSetAdd:       (*EventHub).cmdVarSetAdd,
	rule.CmdVarSetRemove:    (*EventHub).cmdVarSetRemove,
	rule.CmdVarScalarUpdate: (*EventHub).cmdVarScalarUpdate,
	rule.CmdVarMapAdd:       (*EventHub).cmdVarMapAdd,
	rule.CmdVarMapRemove:    (*EventHub).cmdVarMapRemove,

	rule.CmdWebhookCall: (*EventHub).cmdWebhookCall,
}

//nolint:unparam // retained for future contexts (alerting|replay|manual)
func (h *EventHub) processCommandsForEvent(ctx context.Context, event eventing.MdaiEvent, commands []rule.Command, namespace string, payloadData map[string]any, eventType string) error {
	logger := h.withEvent(event, eventType).With(zap.String("namespace", namespace))

	for i, cmd := range commands {
		clog := logger.With(zap.Int("cmdIndex", i), zap.Stringer(fldCommandType, cmd.Type))
		clog.Info("Processing command")

		handler, ok := commandDispatch[cmd.Type]
		if !ok {
			err := fmt.Errorf("unsupported command type: %q", cmd.Type)
			clog.Error("unsupported command type", zap.Error(err))
			return err
		}

		start := time.Now()
		if err := handler(h, ctx, event, namespace, cmd, payloadData); err != nil {
			clog.Error("command failed", zap.Error(err), zap.Duration("elapsed", time.Since(start)))
			return fmt.Errorf("command %d (%s) failed: %w", i, cmd.Type, err)
		}
		clog.Debug("command processed", zap.Duration("elapsed", time.Since(start)))
	}

	return nil
}

// interpolate evaluates a template string against the current event/payload using the configured engine.
func (h *EventHub) interpolate(tmpl, opName, what string, event eventing.MdaiEvent) (string, error) {
	if h.InterpolationEngine == nil {
		return "", fmt.Errorf("%s: interpolate %s: interpolation engine is not configured", opName, what)
	}
	value := h.InterpolationEngine.Interpolate(tmpl, &event) // TODO update to the new api and use everywhere instead of direct calls
	if strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("%s: interpolate %s produced empty (template=%q)", opName, what, tmpl)
	}
	return value, nil
}

type setOp func(ctx context.Context, variableKey, hubName, value, correlationID string) error

func (h *EventHub) execVarSetOp(
	ctx context.Context,
	opName string,
	ev eventing.MdaiEvent,
	cmd rule.Command,
	op setOp,
) error {
	var in mdaiv1.SetAction
	if err := DecodeInputs(cmd.Inputs, &in); err != nil {
		return fmt.Errorf("%s: decode: %w", opName, err)
	}
	if in.Value == "" {
		return fmt.Errorf("%s: inputs.value is empty", opName)
	}
	if in.Set == "" {
		return fmt.Errorf("%s: inputs.set is empty", opName)
	}

	val, err := h.interpolate(in.Value, opName, "value", ev)
	if err != nil {
		return err
	}

	return op(ctx, in.Set, ev.HubName, val, ev.CorrelationID)
}

func (h *EventHub) cmdVarSetAdd(
	ctx context.Context, ev eventing.MdaiEvent, _ string, cmd rule.Command, _ map[string]any,
) error {
	return h.execVarSetOp(ctx, string(rule.CmdVarSetAdd), ev, cmd, h.VarsAdapter.HandlerAdapter.AddElementToSet)
}

func (h *EventHub) cmdVarSetRemove(
	ctx context.Context, ev eventing.MdaiEvent, _ string, cmd rule.Command, _ map[string]any,
) error {
	return h.execVarSetOp(ctx, string(rule.CmdVarSetRemove), ev, cmd, h.VarsAdapter.HandlerAdapter.RemoveElementFromSet)
}

func (h *EventHub) cmdVarMapAdd(
	ctx context.Context, event eventing.MdaiEvent, _ string, cmd rule.Command, _ map[string]any,
) error {
	opName := rule.CmdVarMapAdd.String()
	var in mdaiv1.MapAction
	if err := DecodeInputs(cmd.Inputs, &in); err != nil {
		return fmt.Errorf("%s: decode: %w", opName, err)
	}
	if in.Value == nil || *in.Value == "" {
		return fmt.Errorf("%s: inputs.value is empty", opName)
	}
	if in.Map == "" {
		return fmt.Errorf("%s: inputs.map is empty", opName)
	}

	key, err := h.interpolate(in.Key, opName, "key", event)
	if err != nil {
		return err
	}
	val, err := h.interpolate(*in.Value, opName, "value", event)
	if err != nil {
		return err
	}

	return h.VarsAdapter.HandlerAdapter.SetMapEntry(ctx, in.Map, event.HubName, key, val, event.CorrelationID)
}

func (h *EventHub) cmdVarMapRemove(
	ctx context.Context, event eventing.MdaiEvent, _ string, cmd rule.Command, _ map[string]any,
) error {
	opName := rule.CmdVarMapRemove.String()
	var in mdaiv1.MapAction
	if err := DecodeInputs(cmd.Inputs, &in); err != nil {
		return fmt.Errorf("%s: decode: %w", opName, err)
	}
	if in.Map == "" {
		return fmt.Errorf("%s: inputs.map is empty", opName)
	}

	key, err := h.interpolate(in.Key, opName, "key", event)
	if err != nil {
		return err
	}

	return h.VarsAdapter.HandlerAdapter.RemoveMapEntry(ctx, in.Map, event.HubName, key, event.CorrelationID)
}

func (h *EventHub) cmdVarScalarUpdate(ctx context.Context, ev eventing.MdaiEvent, _ string, cmd rule.Command, _ map[string]any) error {
	return h.execVarScalarOp(ctx, string(rule.CmdVarScalarUpdate), ev, cmd, h.VarsAdapter.HandlerAdapter.SetStringValue)
}

func (h *EventHub) execVarScalarOp(
	ctx context.Context,
	opName string,
	event eventing.MdaiEvent,
	cmd rule.Command,
	op setOp,
) error {
	var in mdaiv1.ScalarAction
	if err := DecodeInputs(cmd.Inputs, &in); err != nil {
		return fmt.Errorf("%s: decode: %w", opName, err)
	}
	if in.Value == "" {
		return fmt.Errorf("%s: inputs.value is empty", opName)
	}
	if in.Scalar == "" {
		return fmt.Errorf("%s: inputs.scalar is empty", opName)
	}

	val, err := h.interpolate(in.Value, opName, "value", event)
	if err != nil {
		return err
	}

	return op(ctx, in.Scalar, event.HubName, val, event.CorrelationID)
}

func (h *EventHub) cmdWebhookCall(ctx context.Context, ev eventing.MdaiEvent, ns string, cmd rule.Command, payload map[string]any) error {
	return h.HandleCallWebhookFn(ctx, h.Kube, ns, ev, cmd.Inputs, payload)
}

func DecodeInputs[T any](raw json.RawMessage, out *T) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	return dec.Decode(out)
}
