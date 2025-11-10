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

type HandlerAdapter interface {
	AddElementToSet(ctx context.Context, variableKey, hubName, value, correlationID string, recursionDepth int) error
	RemoveElementFromSet(ctx context.Context, variableKey, hubName, value, correlationID string, recursionDepth int) error
	SetMapEntry(ctx context.Context, variableKey, hubName, field, value, correlationID string, recursionDepth int) error
	RemoveMapEntry(ctx context.Context, variableKey, hubName, field, correlationID string, recursionDepth int) error
	SetStringValue(ctx context.Context, variableKey, hubName, value, correlationID string, recursionDepth int) error
}

type VarDeps struct {
	Logger         *zap.Logger
	HandlerAdapter HandlerAdapter
}

type setInputs struct {
	Set   string `json:"set"`
	Value string `json:"value"`
}

type scalarInputs struct {
	Scalar string `json:"scalar"`
	Value  string `json:"value"`
}

type mapAddInputs struct {
	Map   string `json:"map"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

type mapRemoveInputs struct {
	Map string `json:"map"`
	Key string `json:"key"`
}

// BuildCommandFromEvent creates a rule.Command from a manual variable update event.
func (v *VarDeps) BuildCommandFromEvent(event eventing.MdaiEvent) ([]rule.Command, error) {
	var payloadObj eventing.VariablesActionPayload
	if err := json.Unmarshal([]byte(event.Payload), &payloadObj); err != nil {
		return nil, fmt.Errorf("failed to unmarshal payload: %w", err)
	}

	v.Logger.Info("Building command from manual variable payload", zap.Reflect("value", payloadObj.Data))

	cmdType, err := determineCommandType(payloadObj.DataType, payloadObj.Operation)
	if err != nil {
		return nil, err
	}

	switch cmdType {
	case rule.CmdVarSetAdd, rule.CmdVarSetRemove:
		return buildSetCommands(cmdType, payloadObj)
	case rule.CmdVarScalarUpdate:
		return buildScalarCommand(cmdType, payloadObj)
	case rule.CmdVarMapAdd:
		return buildMapAddCommands(cmdType, payloadObj)
	case rule.CmdVarMapRemove:
		return buildMapRemoveCommands(cmdType, payloadObj)
	case rule.CmdWebhookCall:
		return nil, nil // No commands to build for webhook call in this context
	case rule.CmdDeployReplay:
		return nil, nil
	case rule.CmdCleanUpReplay:
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported command type for manual action: %s", cmdType)
	}
}

func buildSetCommands(cmdType rule.CommandType, payload eventing.VariablesActionPayload) ([]rule.Command, error) {
	rawValues, ok := payload.Data.([]any)
	if !ok || len(rawValues) == 0 {
		return nil, errors.New("data for set operation should be a non-empty array")
	}

	var commands []rule.Command
	for _, item := range rawValues {
		value, ok := item.(string)
		if !ok {
			return nil, errors.New("set value must be a string")
		}
		inputs, err := json.Marshal(setInputs{Set: payload.VariableRef, Value: value})
		if err != nil {
			return nil, fmt.Errorf("failed to build command inputs for set: %w", err)
		}
		commands = append(commands, rule.Command{Type: cmdType, Inputs: inputs})
	}
	return commands, nil
}

func buildScalarCommand(cmdType rule.CommandType, payload eventing.VariablesActionPayload) ([]rule.Command, error) {
	value, ok := payload.Data.(string)
	if !ok {
		return nil, errors.New("scalar data should be a string")
	}
	if payload.Operation == "remove" {
		value = ""
	}
	inputs, err := json.Marshal(scalarInputs{Scalar: payload.VariableRef, Value: value})
	if err != nil {
		return nil, fmt.Errorf("failed to build command inputs for scalar: %w", err)
	}

	return []rule.Command{{Type: cmdType, Inputs: inputs}}, nil
}

func buildMapAddCommands(cmdType rule.CommandType, payload eventing.VariablesActionPayload) ([]rule.Command, error) {
	dataMap, ok := payload.Data.(map[string]any)
	if !ok {
		return nil, errors.New("map add data should be a map")
	}

	var commands []rule.Command
	for key, val := range dataMap {
		value, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("map add value for key '%s' must be a string", key)
		}
		inputs, err := json.Marshal(mapAddInputs{Map: payload.VariableRef, Key: key, Value: value})
		if err != nil {
			return nil, fmt.Errorf("failed to build command inputs for map add: %w", err)
		}
		commands = append(commands, rule.Command{Type: cmdType, Inputs: inputs})
	}
	return commands, nil
}

func buildMapRemoveCommands(cmdType rule.CommandType, payload eventing.VariablesActionPayload) ([]rule.Command, error) {
	keys, ok := payload.Data.([]any)
	if !ok {
		return nil, errors.New("map remove data should be an array of keys (strings)")
	}

	var commands []rule.Command
	for _, item := range keys {
		key, ok := item.(string)
		if !ok {
			return nil, errors.New("map remove key must be a string")
		}
		inputs, err := json.Marshal(mapRemoveInputs{Map: payload.VariableRef, Key: key})
		if err != nil {
			return nil, fmt.Errorf("failed to build command inputs for map remove: %w", err)
		}
		commands = append(commands, rule.Command{Type: cmdType, Inputs: inputs})
	}
	return commands, nil
}

func determineCommandType(dataType, operation string) (rule.CommandType, error) {
	switch dataType {
	case "string", "int", "boolean":
		return rule.CmdVarScalarUpdate, nil
	default:
		t, err := rule.ParseCommandType("variable." + dataType + "." + operation)
		if err != nil {
			return "", fmt.Errorf("parse command type from dataType=%q operation=%q: %w", dataType, operation, err)
		}
		return t, nil
	}
}
