package eventhub

import (
	"context"
	"fmt"

	"github.com/decisiveai/mdai-data-core/eventing"
	"github.com/decisiveai/mdai-data-core/eventing/rule"
	"github.com/decisiveai/mdai-event-hub/internal/handlers"
	mdaiv1 "github.com/decisiveai/mdai-operator/api/v1"
)

const (
	CmdVarSetAdd    = "variable.set.add"
	CmdVarSetRemove = "variable.set.remove"
	CmdWebhookCall  = "webhook.call"
)

type cmdHandler func(ctx context.Context, ev eventing.MdaiEvent, ns string, cmd rule.Command, payload map[string]any) error

func (h *EventHub) registry() map[string]cmdHandler {
	return map[string]cmdHandler{
		CmdVarSetAdd:    h.cmdVarSetAdd,
		CmdVarSetRemove: h.cmdVarSetRemove,
		CmdWebhookCall:  h.cmdWebhookCall,
	}
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
	if err := handlers.DecodeInputs(cmd.Inputs, &in); err != nil {
		return fmt.Errorf("%s: decode: %w", opName, err)
	}
	if in.Value == "" {
		return fmt.Errorf("%s: inputs.value is empty", opName)
	}
	if in.Set == "" {
		return fmt.Errorf("%s: inputs.set is empty", opName)
	}

	labels, err := handlers.ReadLabels(payload)
	if err != nil {
		return fmt.Errorf("%s: %w", opName, err)
	}
	val, ok := labels[in.Value]
	if !ok {
		return fmt.Errorf("%s: label %q not found in payload labels", opName, in.Value)
	}

	return op(ctx, in.Set, ev.HubName, val, ev.CorrelationID)
}

func (h *EventHub) cmdVarSetAdd(
	ctx context.Context, ev eventing.MdaiEvent, _ string, cmd rule.Command, payload map[string]any,
) error {
	return execVarSetOp(ctx, CmdVarSetAdd, ev, cmd, payload, h.HandlerAdapter.AddElementToSet)
}

func (h *EventHub) cmdVarSetRemove(
	ctx context.Context, ev eventing.MdaiEvent, _ string, cmd rule.Command, payload map[string]any,
) error {
	return execVarSetOp(ctx, CmdVarSetRemove, ev, cmd, payload, h.HandlerAdapter.RemoveElementFromSet)
}

func (h *EventHub) cmdWebhookCall(ctx context.Context, ev eventing.MdaiEvent, ns string, cmd rule.Command, payload map[string]any) error {
	return handlers.HandleCallSlackWebhookFn(ctx, h.Kube, ns, ev, cmd.Inputs, payload)
}
