package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/decisiveai/mdai-event-hub/pkg/eventing"
	"go.uber.org/zap"
)

const (
	HandleAddNoisyServiceToSet      HandlerName = "HandleAddNoisyServiceToSet"
	HandleRemoveNoisyServiceFromSet HandlerName = "HandleRemoveNoisyServiceFromSet"
	HandleNoisyServiceAlert         HandlerName = "HandleNoisyServiceAlert"
	HandleCallSlackWebhook          HandlerName = "HandleCallSlackWebhook"
)

func GetDefaultHandlers() HandlerMap {
	return HandlerMap{
		HandleAddNoisyServiceToSet:      handleAddNoisyServiceToSet,
		HandleRemoveNoisyServiceFromSet: handleRemoveNoisyServiceFromSet,
		HandleNoisyServiceAlert:         HandleUpdateSetByComparison,
		HandleCallSlackWebhook:          HandleCallSlackWebhookFn,
	}
}

// GetSupportedHandlers Go doesn't support dynamic accessing of exports. So this is a workaround.
// The handler library will have to export a map that can be dynamically accessed.
// To enforce this, handlers are declared with a lower case first character so they
// are not exported directly but can only be accessed through the map.
func GetSupportedHandlers(h HandlerMap) HandlerMap {
	if h != nil {
		return h
	}
	return GetDefaultHandlers()
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
	mdai.Logger.Debug("handleNoisyServiceList ", zap.Object("event", &event), zap.Reflect("payload", payloadData), zap.Reflect("args", args))

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
		if err := mdai.Data.AddElementToSet(ctx, variableRef, event.HubName, payloadValue, event.CorrelationID); err != nil {
			return err
		}
	case "resolved":
		if err := mdai.Data.RemoveElementFromSet(ctx, variableRef, event.HubName, payloadValue, event.CorrelationID); err != nil {
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
	mdai.Logger.Debug("handleAddNoisyServiceToSet ", zap.Object("event", &event), zap.Reflect("payload", payloadData), zap.Reflect("args", args))

	payloadValueKey := getArgsValueWithDefault("payload_val_ref", "service_name", args)
	variableRef := getArgsValueWithDefault("variable_ref", "service_list", args)

	value, err := getString(payloadData, payloadValueKey)
	if err != nil {
		return fmt.Errorf("failed to get payload value key: %w", err)
	}

	if err := mdai.Data.AddElementToSet(ctx, variableRef, event.HubName, value, event.CorrelationID); err != nil {
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
	mdai.Logger.Debug("handleRemoveNoisyServiceFromSet ", zap.Object("event", &event), zap.Reflect("payload", payloadData), zap.Reflect("args", args))

	payloadValueKey := getArgsValueWithDefault("payload_val_ref", "service_name", args)
	variableRef := getArgsValueWithDefault("variable_ref", "service_list", args)

	value, err := getString(payloadData, payloadValueKey)
	if err != nil {
		return fmt.Errorf("failed to get payload value key: %w", err)
	}

	if err := mdai.Data.RemoveElementFromSet(ctx, variableRef, event.HubName, value, event.CorrelationID); err != nil {
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

	mdai.Logger.Info("Received static variable payload", zap.Reflect("Value", payloadObj.Data))
	correlationID := event.CorrelationID

	switch payloadObj.DataType {
	case "set":
		return handleSetOperations(ctx, mdai, payloadObj, event.HubName, correlationID)
	case "map":
		return handleMapOperations(ctx, mdai, payloadObj, event.HubName, correlationID)
	case "string", "int", "boolean":
		return handleScalarOperations(ctx, mdai, payloadObj, event.HubName, correlationID)
	default:
		return fmt.Errorf("unsupported data type: %s", payloadObj.DataType)
	}
}

func handleSetOperations(ctx context.Context, mdai MdaiInterface, payload eventing.ManualVariablesActionPayload, hubName, correlationID string) error {
	values, ok := payload.Data.([]any)
	if !ok {
		return errors.New("data should be a list of strings")
	}

	switch payload.Operation {
	case "add":
		return processSetValues(ctx, mdai, values, payload.VariableRef, hubName, correlationID, mdai.Data.AddElementToSet, "Setting value")
	case "remove":
		return processSetValues(ctx, mdai, values, payload.VariableRef, hubName, correlationID, mdai.Data.RemoveElementFromSet, "Removing value")
	default:
		return fmt.Errorf("unsupported set operation: %s", payload.Operation)
	}
}

func handleMapOperations(ctx context.Context, mdai MdaiInterface, payload eventing.ManualVariablesActionPayload, hubName, correlationID string) error {
	switch payload.Operation {
	case "add":
		return handleMapAdd(ctx, mdai, payload, hubName, correlationID)
	case "remove":
		return handleMapRemove(ctx, mdai, payload, hubName, correlationID)
	default:
		return fmt.Errorf("unsupported map operation: %s", payload.Operation)
	}
}

func handleScalarOperations(ctx context.Context, mdai MdaiInterface, payload eventing.ManualVariablesActionPayload, hubName, correlationID string) error {
	value, ok := payload.Data.(string)
	if !ok {
		return errors.New("data should be a string")
	}

	mdai.Logger.Info("Setting string", zap.String("value", value))
	return mdai.Data.SetStringValue(ctx, payload.VariableRef, hubName, value, correlationID)
}

type SetOperation func(ctx context.Context, variableKey, hubName, value, correlationID string) error

func processSetValues(
	ctx context.Context,
	mdai MdaiInterface,
	values []any,
	variableRef,
	hubName,
	correlationID string,
	operation SetOperation,
	logMessage string,
) error {
	for _, val := range values {
		str, ok := val.(string)
		if !ok {
			return fmt.Errorf("expected string, got %T", val)
		}

		mdai.Logger.Info(logMessage, zap.String("Value", str))
		if err := operation(ctx, variableRef, hubName, str, correlationID); err != nil {
			return err
		}
	}
	return nil
}

func handleMapAdd(ctx context.Context, mdai MdaiInterface, payload eventing.ManualVariablesActionPayload, hubName, correlationID string) error {
	values, ok := payload.Data.(map[string]any)
	if !ok {
		return errors.New("data should be a map[string]string")
	}

	for key, val := range values {
		str, ok := val.(string)
		if !ok {
			return fmt.Errorf("expected string, got %T", val)
		}

		mdai.Logger.Info("Setting value", zap.String("Field", key), zap.String("Value", str))
		if err := mdai.Data.AddSetMapElement(ctx, payload.VariableRef, hubName, key, str, correlationID); err != nil {
			return err
		}
	}
	return nil
}

func handleMapRemove(ctx context.Context, mdai MdaiInterface, payload eventing.ManualVariablesActionPayload, hubName, correlationID string) error {
	values, ok := payload.Data.([]any)
	if !ok {
		return errors.New("data should be a slice of strings")
	}

	for _, key := range values {
		k, ok := key.(string)
		if !ok {
			return fmt.Errorf("expected string, got %T", key)
		}

		mdai.Logger.Info("Deleting field", zap.String("Field", k))
		if err := mdai.Data.RemoveElementFromMap(ctx, payload.VariableRef, hubName, k, correlationID); err != nil {
			return err
		}
	}
	return nil
}

type SlackPayload struct {
	Text   string           `json:"text"`
	Blocks []map[string]any `json:"blocks,omitempty"`
}

func HandleCallSlackWebhookFn(mdai MdaiInterface, event eventing.MdaiEvent, args map[string]string) error {
	ctx := context.Background()
	webhookURL, webhookURLExists := args["webhook_url"]
	if !webhookURLExists || webhookURL == "" {
		return errors.New("webhook_url is a required arg value, cannot call webhook")
	}

	payloadData, err := ProcessEventPayload(event)
	if err != nil {
		return fmt.Errorf("failed to process payload: %w", err)
	}

	payload, err := buildSlackPayload(args, event, payloadData)
	if err != nil {
		return err
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, webhookURL, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("non-200 response: %s", resp.Status)
	}

	return nil
}

func addPayloadField(fields []map[string]string, args map[string]string, payloadData map[string]any, key string) ([]map[string]string, error) {
	payloadKey, exists := args[key]
	if !exists || payloadKey == "" {
		return fields, nil
	}

	payloadValue, err := getString(payloadData, payloadKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get %s from payload with error: %w", payloadKey, err)
	}

	return append(fields, map[string]string{
		"type": "mrkdwn",
		"text": fmt.Sprintf("*%s* - %s", payloadKey, payloadValue),
	}), nil
}

func buildSlackPayload(args map[string]string, event eventing.MdaiEvent, payloadData map[string]any) (SlackPayload, error) {
	message, messageExists := args["message"]
	if !messageExists || message == "" {
		message = fmt.Sprintf("MDAI Hub Event - %s - %s", event.HubName, event.Name)
	}
	payload := SlackPayload{
		Text: message,
		Blocks: []map[string]any{
			{
				"type": "section",
				"text": map[string]string{
					"type": "mrkdwn",
					"text": fmt.Sprintf("*%s*", message),
				},
			},
		},
	}

	fields := []map[string]string{
		{
			"type": "mrkdwn",
			"text": fmt.Sprintf("*%s* - %s", "Alert timestamp", event.Timestamp),
		},
	}

	var err error
	if fields, err = addPayloadField(fields, args, payloadData, "payload_val_ref_primary"); err != nil {
		return SlackPayload{}, err
	}
	if fields, err = addPayloadField(fields, args, payloadData, "payload_val_ref_secondary"); err != nil {
		return SlackPayload{}, err
	}
	if fields, err = addPayloadField(fields, args, payloadData, "payload_val_ref_tertiary"); err != nil {
		return SlackPayload{}, err
	}

	if len(fields) > 0 {
		payload.Blocks = append(payload.Blocks, map[string]any{
			"type":   "section",
			"fields": fields,
		})
	}

	linkText, linkTextExists := args["link_text"]
	linkURL, linkURLExists := args["link_url"]
	if linkURLExists && linkURL != "" {
		if !linkTextExists || linkText == "" {
			linkText = "See more"
		}
		payload.Blocks = append(payload.Blocks, map[string]any{
			"type": "actions",
			"elements": []map[string]any{
				{
					"type": "button",
					"text": map[string]string{
						"type": "plain_text",
						"text": linkText,
					},
					"style": "primary",
					"url":   linkURL,
				},
			},
		})
	}
	return payload, nil
}
