package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	datacore "github.com/decisiveai/mdai-data-core/handlers"
	"github.com/decisiveai/mdai-event-hub/pkg/eventing"

	"go.uber.org/zap"
)

type MdaiInterface struct {
	*zap.Logger
	*datacore.HandlerAdapter
}

type HandlerName string

type HandlerFunc func(MdaiInterface, eventing.MdaiEvent, map[string]string) error

type HandlerMap map[HandlerName]HandlerFunc

const (
	HandleAddNoisyServiceToSet      HandlerName = "HandleAddNoisyServiceToSet"
	HandleRemoveNoisyServiceFromSet HandlerName = "HandleRemoveNoisyServiceFromSet"
	HandleNoisyServiceAlert         HandlerName = "HandleNoisyServiceAlert"
)

// SupportedHandlers Go doesn't support dynamic accessing of exports. So this is a workaround.
// The handler library will have to export a map that can be dynamically accessed.
// To enforce this, handlers are declared with a lower case first character so they
// are not exported directly but can only be accessed through the map
var SupportedHandlers = HandlerMap{
	HandleAddNoisyServiceToSet:      handleAddNoisyServiceToSet,
	HandleRemoveNoisyServiceFromSet: handleRemoveNoisyServiceFromSet,
	HandleNoisyServiceAlert:         HandleUpdateSetByComparison,
}

func ProcessEventPayload(event eventing.MdaiEvent) (map[string]any, error) {
	if event.Payload == "" {
		return map[string]any{}, nil
	}

	var payloadData map[string]any

	err := json.Unmarshal([]byte(event.Payload), &payloadData)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal payload %q: %w", event.Payload, err)
	}

	return payloadData, nil
}

func getArgsValueWithDefault(key string, defaultValue string, args map[string]string) string {
	if val, ok := args[key]; ok {
		return val
	}
	return defaultValue
}

func getString(m map[string]any, key string) (string, error) {
	v, ok := m[key]
	if !ok {
		return "", fmt.Errorf("key %s not found", key)
	}

	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("key %s exists but value is not a string", key)
	}

	return s, nil
}

func HandleUpdateSetByComparison(mdai MdaiInterface, event eventing.MdaiEvent, args map[string]string) error {
	ctx := context.Background()
	payloadData, err := ProcessEventPayload(event)
	if err != nil {
		return fmt.Errorf("failed to process payload: %w", err)
	}
	mdai.Debug("handleNoisyServiceList ", zap.Any("event", event), zap.Any("payload", payloadData), zap.Any("args", args))

	payloadValueKey := getArgsValueWithDefault("payload_val_ref", "service_name", args)
	payloadComparableKey := getArgsValueWithDefault("payload_comparable_ref", "status", args)
	variableRef := getArgsValueWithDefault("variable_ref", "service_list", args)

	comp, err := getString(payloadData, payloadComparableKey)
	if err != nil {
		return fmt.Errorf("failed to get payload comparable key: %w", err)
	}
	payloadValue, err := getString(payloadData, payloadValueKey)
	if err != nil {
		return fmt.Errorf("failed to get payload value key: %w", err)
	}

	switch comp {
	case "firing":
		if err = mdai.AddElementToSet(ctx, variableRef, event.HubName, payloadValue); err != nil {
			return err
		}
	case "resolved":
		if err = mdai.RemoveElementFromSet(ctx, variableRef, event.HubName, payloadValue); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown alert status: %s", comp)
	}
	return nil
}

func handleAddNoisyServiceToSet(mdai MdaiInterface, event eventing.MdaiEvent, args map[string]string) error {
	ctx := context.Background()
	payloadData, err := ProcessEventPayload(event)
	if err != nil {
		return fmt.Errorf("failed to process payload: %w", err)
	}
	mdai.Debug("handleAddNoisyServiceToSet ", zap.Any("event", event), zap.Any("payload", payloadData), zap.Any("args", args))

	payloadValueKey := getArgsValueWithDefault("payload_val_ref", "service_name", args)
	variableRef := getArgsValueWithDefault("variable_ref", "service_list", args)

	value, err := getString(payloadData, payloadValueKey)
	if err != nil {
		return fmt.Errorf("failed to get payload value key: %w", err)
	}

	if err := mdai.AddElementToSet(ctx, variableRef, event.HubName, value); err != nil {
		return err
	}
	// TODO: Debug Log new var val

	return nil
}

func handleRemoveNoisyServiceFromSet(mdai MdaiInterface, event eventing.MdaiEvent, args map[string]string) error {
	ctx := context.Background()
	payloadData, err := ProcessEventPayload(event)
	if err != nil {
		return fmt.Errorf("failed to process payload: %w", err)
	}
	mdai.Debug("handleRemoveNoisyServiceFromSet ", zap.Any("event", event), zap.Any("payload", payloadData), zap.Any("args", args))

	payloadValueKey := getArgsValueWithDefault("payload_val_ref", "service_name", args)
	variableRef := getArgsValueWithDefault("variable_ref", "service_list", args)

	value, err := getString(payloadData, payloadValueKey)
	if err != nil {
		return fmt.Errorf("failed to get payload value key: %w", err)
	}

	if err := mdai.RemoveElementFromSet(ctx, variableRef, event.HubName, value); err != nil {
		return err
	}
	// TODO: Debug Log new var val

	return nil
}

func HandleManualVariablesActions(ctx context.Context, mdai MdaiInterface, event eventing.MdaiEvent) error {
	var payloadObj eventing.ManualVariablesActionPayload
	if err := json.Unmarshal([]byte(event.Payload), &payloadObj); err != nil {
		return err
	}
	mdai.Info("Received static variable payload", zap.Any("Value", payloadObj.Data))
	switch payloadObj.DataType {
	case "set":
		values, ok := payloadObj.Data.([]any)
		if !ok {
			return fmt.Errorf("data should be a list of strings")
		}
		{
			switch payloadObj.Operation {
			case "add":
				{
					for _, val := range values {
						mdai.Info("Setting value", zap.String("Value", val.(string)))
						if err := mdai.AddElementToSet(ctx, payloadObj.VariableRef, event.HubName, val.(string)); err != nil {
							return err
						}
					}
				}
			case "remove":
				{
					for _, val := range values {
						mdai.Info("Setting value", zap.String("Value", val.(string)))
						if err := mdai.RemoveElementFromSet(ctx, payloadObj.VariableRef, event.HubName, val.(string)); err != nil {
							return err
						}
					}
				}
			}
		}
	case "map":
		{
			switch payloadObj.Operation {
			case "add":
				{
					values, ok := payloadObj.Data.(map[string]any)
					if !ok {
						return fmt.Errorf("data should be a map[string]string")
					}
					for key, val := range values {
						mdai.Info("Setting value", zap.String("Field", key), zap.String("Value", val.(string)))
						if err := mdai.AddSetMapElement(ctx, payloadObj.VariableRef, event.HubName, key, val.(string)); err != nil {
							return err
						}
					}
				}
			case "remove":
				{
					values, ok := payloadObj.Data.([]any)
					if !ok {
						return fmt.Errorf("data should be a slice of strings")
					}
					for _, key := range values {
						mdai.Info("Deleting  field", zap.String("Field", key.(string)))
						if err := mdai.RemoveElementFromMap(ctx, payloadObj.VariableRef, event.HubName, key.(string)); err != nil {
							return err
						}
					}
				}
			}
		}
	case "string", "int", "boolean":
		{
			value, ok := payloadObj.Data.(string)
			if !ok {
				return fmt.Errorf("data should be a string")
			}
			mdai.Info("Setting string", zap.String("value", value))
			if err := mdai.SetStringValue(ctx, payloadObj.VariableRef, event.HubName, value); err != nil {
				return err
			}
		}
	}
	return nil
}
