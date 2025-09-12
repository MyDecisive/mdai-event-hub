package eventhub

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/decisiveai/mdai-data-core/eventing"
	"github.com/decisiveai/mdai-data-core/eventing/rule"
	mdaiv1 "github.com/decisiveai/mdai-operator/api/v1"
	"go.uber.org/zap"
)

type cmdHandler func(h *EventHub, ctx context.Context, ev eventing.MdaiEvent, ns string, cmd rule.Command, payload map[string]any) error

var commandDispatch = map[rule.CommandType]cmdHandler{
	rule.CmdVarSetAdd:       (*EventHub).cmdVarSetAdd,
	rule.CmdVarSetRemove:    (*EventHub).cmdVarSetRemove,
	rule.CmdVarScalarUpdate: (*EventHub).cmdVarScalarUpdate,
	rule.CmdVarMapAdd:       (*EventHub).cmdVarMapAdd,
	rule.CmdVarMapRemove:    (*EventHub).cmdVarMapRemove,

	rule.CmdWebhookCall: (*EventHub).cmdWebhookCall,
}

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

type setOp func(ctx context.Context, variableKey, hubName, value, correlationID string) error

func execVarSetOp(
	ctx context.Context,
	opName string,
	ev eventing.MdaiEvent,
	cmd rule.Command,
	payload map[string]any,
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

	labels, err := ReadLabels(payload)
	if err != nil {
		return fmt.Errorf("%s: %w", opName, err)
	}
	val, ok := labels[in.Value] // TODO change to extrapolation logic
	if !ok {
		return fmt.Errorf("%s: label %q not found in payload labels", opName, in.Value)
	}

	return op(ctx, in.Set, ev.HubName, val, ev.CorrelationID)
}

func (h *EventHub) cmdVarSetAdd(
	ctx context.Context, ev eventing.MdaiEvent, _ string, cmd rule.Command, payload map[string]any,
) error {
	return execVarSetOp(ctx, string(rule.CmdVarSetAdd), ev, cmd, payload, h.VarsAdapter.HandlerAdapter.AddElementToSet)
}

func (h *EventHub) cmdVarSetRemove(
	ctx context.Context, ev eventing.MdaiEvent, _ string, cmd rule.Command, payload map[string]any,
) error {
	return execVarSetOp(ctx, string(rule.CmdVarSetRemove), ev, cmd, payload, h.VarsAdapter.HandlerAdapter.RemoveElementFromSet)
}

func (h *EventHub) cmdVarMapAdd(
	ctx context.Context, ev eventing.MdaiEvent, _ string, cmd rule.Command, payload map[string]any,
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

	labels, err := ReadLabels(payload)
	if err != nil {
		return fmt.Errorf("%s: %w", opName, err)
	}

	field, ok := labels[in.Key] // TODO change to extrapolation logic
	if !ok {
		return fmt.Errorf("%s: label %q not found in payload labels", opName, in.Key)
	}

	val := in.Value // TODO change to extrapolation logic
	if val == nil {
		return fmt.Errorf("%s: inputs.value is nil", opName)
	}

	return h.VarsAdapter.HandlerAdapter.SetMapEntry(ctx, in.Map, ev.HubName, field, *val, ev.CorrelationID)
}

func (h *EventHub) cmdVarMapRemove(
	ctx context.Context, ev eventing.MdaiEvent, _ string, cmd rule.Command, payload map[string]any,
) error {
	opName := rule.CmdVarMapRemove.String()
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

	labels, err := ReadLabels(payload)
	if err != nil {
		return fmt.Errorf("%s: %w", opName, err)
	}

	field, ok := labels[in.Key] // TODO change to extrapolation logic
	if !ok {
		return fmt.Errorf("%s: label %q not found in payload labels", opName, in.Key)
	}

	return h.VarsAdapter.HandlerAdapter.RemoveMapEntry(ctx, in.Map, ev.HubName, field, ev.CorrelationID)
}

func (h *EventHub) cmdWebhookCall(ctx context.Context, ev eventing.MdaiEvent, ns string, cmd rule.Command, payload map[string]any) error {
	return HandleCallSlackWebhookFn(ctx, h.Kube, ns, ev, cmd.Inputs, payload)
}

func (h *EventHub) cmdVarScalarUpdate(ctx context.Context, ev eventing.MdaiEvent, _ string, cmd rule.Command, payload map[string]any) error {
	return execVarScalarOp(ctx, string(rule.CmdVarScalarUpdate), ev, cmd, payload, h.VarsAdapter.HandlerAdapter.SetStringValue)
}

func execVarScalarOp(
	ctx context.Context,
	opName string,
	ev eventing.MdaiEvent,
	cmd rule.Command,
	payload map[string]any,
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

	labels, err := ReadLabels(payload)
	if err != nil {
		return fmt.Errorf("%s: %w", opName, err)
	}
	val, ok := labels[in.Value] // TODO change to extrapolation logic
	if !ok {
		return fmt.Errorf("%s: label %q not found in payload labels", opName, in.Value)
	}

	return op(ctx, in.Scalar, ev.HubName, val, ev.CorrelationID)
}

func ReadLabels(payloadData map[string]any) (map[string]string, error) {
	labelsRaw, ok := payloadData["labels"]
	if !ok {
		return nil, errors.New("labels not found in payload")
	}

	labelsMap, ok := labelsRaw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("payload.labels has type %T, want map[string]any", labelsRaw)
	}

	labels := make(map[string]string)
	for k, v := range labelsMap {
		strValue, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("label value for key %s is not a string", k)
		}
		labels[k] = strValue
	}

	return labels, nil
}

func DecodeInputs[T any](raw json.RawMessage, out *T) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	return dec.Decode(out)
}
