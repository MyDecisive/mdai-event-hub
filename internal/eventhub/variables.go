package eventhub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/decisiveai/mdai-data-core/eventing"
	"github.com/decisiveai/mdai-data-core/eventing/rule"
	"go.uber.org/zap"
)

type VarDeps struct {
	Logger         *zap.Logger
	HandlerAdapter iHandlerAdapter
}

type iHandlerAdapter interface {
	AddElementToSet(ctx context.Context, variableKey, hubName, value, correlationID string) error
	RemoveElementFromSet(ctx context.Context, variableKey, hubName, value, correlationID string) error
	SetMapEntry(ctx context.Context, variableKey, hubName, field, value, correlationID string) error
	RemoveMapEntry(ctx context.Context, variableKey, hubName, field, correlationID string) error
	SetStringValue(ctx context.Context, variableKey, hubName, value, correlationID string) error
}

func (v *VarDeps) HandleManualVariablesActions(ctx context.Context, event eventing.MdaiEvent) error {
	var payloadObj eventing.ManualVariablesActionPayload
	if err := json.Unmarshal([]byte(event.Payload), &payloadObj); err != nil {
		return err
	}

	v.Logger.Info("Received static variable payload", zap.Reflect("value", payloadObj.Data))
	correlationID := event.CorrelationID

	var cmd rule.CommandType
	var err error
	if payloadObj.DataType == "string" || payloadObj.DataType == "int" || payloadObj.DataType == "boolean" {
		cmd = rule.CmdVarScalarUpdate
	} else {
		cmd, err = rule.ParseCommandType("variable" + "." + payloadObj.DataType + "." + payloadObj.Operation)
		if err != nil {
			return err
		}
	}

	switch cmd {
	case rule.CmdVarSetAdd:
		return v.processSetValues(ctx, payloadObj, event.HubName, correlationID, v.HandlerAdapter.AddElementToSet, "Setting value")
	case rule.CmdVarSetRemove:
		return v.processSetValues(ctx, payloadObj, event.HubName, correlationID, v.HandlerAdapter.RemoveElementFromSet, "Removing value")
	case rule.CmdVarMapAdd:
		return v.handleMapAdd(ctx, payloadObj, event.HubName, correlationID)
	case rule.CmdVarMapRemove:
		return v.handleMapRemove(ctx, payloadObj, event.HubName, correlationID)
	case rule.CmdVarScalarUpdate:
		value, ok := payloadObj.Data.(string)
		if !ok {
			return errors.New("data should be a string")
		}
		return v.HandlerAdapter.SetStringValue(ctx, payloadObj.VariableRef, event.HubName, value, correlationID)
	default:
		return fmt.Errorf("unsupported data type: %s", payloadObj.DataType)
	}
}

type SetOperation func(ctx context.Context, variableKey, hubName, value, correlationID string) error

func (v *VarDeps) processSetValues(
	ctx context.Context,
	payloadObj eventing.ManualVariablesActionPayload,
	hubName,
	correlationID string,
	operation SetOperation,
	logMessage string,
) error {
	values, ok := payloadObj.Data.([]any)
	if !ok {
		return errors.New("data should be a list of strings")
	}

	for _, val := range values {
		str, ok := val.(string)
		if !ok {
			return fmt.Errorf("expected string, got %T", val)
		}

		v.Logger.Info(logMessage, zap.String("value", str))
		if err := operation(ctx, payloadObj.VariableRef, hubName, str, correlationID); err != nil {
			return err
		}
	}
	return nil
}

func (v *VarDeps) handleMapAdd(ctx context.Context, payload eventing.ManualVariablesActionPayload, hubName, correlationID string) error {
	values, ok := payload.Data.(map[string]any)
	if !ok {
		return errors.New("data should be a map[string]string")
	}

	for key, val := range values {
		str, ok := val.(string)
		if !ok {
			return fmt.Errorf("expected string, got %T", val)
		}

		v.Logger.Info("Setting value", zap.String("field", key), zap.String("value", str))
		if err := v.HandlerAdapter.SetMapEntry(ctx, payload.VariableRef, hubName, key, str, correlationID); err != nil {
			return err
		}
	}
	return nil
}

func (v *VarDeps) handleMapRemove(ctx context.Context, payload eventing.ManualVariablesActionPayload, hubName, correlationID string) error {
	values, ok := payload.Data.([]any)
	if !ok {
		return errors.New("data should be a slice of strings")
	}

	for _, key := range values {
		k, ok := key.(string)
		if !ok {
			return fmt.Errorf("expected string, got %T", key)
		}

		v.Logger.Info("Deleting field", zap.String("field", k))
		if err := v.HandlerAdapter.RemoveMapEntry(ctx, payload.VariableRef, hubName, k, correlationID); err != nil {
			return err
		}
	}
	return nil
}
