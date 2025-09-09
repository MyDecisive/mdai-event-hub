package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/decisiveai/mdai-data-core/eventing"
	mdaiv1 "github.com/decisiveai/mdai-operator/api/v1"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type VarDeps interface {
	GetLogger() *zap.Logger
	GetHandlerAdapter() IHandlerAdapter
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

func HandleManualVariablesActions(ctx context.Context, mdai VarDeps, event eventing.MdaiEvent) error {
	var payloadObj eventing.ManualVariablesActionPayload
	if err := json.Unmarshal([]byte(event.Payload), &payloadObj); err != nil {
		return err
	}

	mdai.GetLogger().Info("Received static variable payload", zap.Reflect("value", payloadObj.Data))
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

func handleSetOperations(ctx context.Context, mdai VarDeps, payload eventing.ManualVariablesActionPayload, hubName, correlationID string) error {
	values, ok := payload.Data.([]any)
	if !ok {
		return errors.New("data should be a list of strings")
	}

	switch payload.Operation {
	case "add":
		return processSetValues(ctx, mdai, values, payload.VariableRef, hubName, correlationID, mdai.GetHandlerAdapter().AddElementToSet, "Setting value")
	case "remove":
		return processSetValues(ctx, mdai, values, payload.VariableRef, hubName, correlationID, mdai.GetHandlerAdapter().RemoveElementFromSet, "Removing value")
	default:
		return fmt.Errorf("unsupported set operation: %s", payload.Operation)
	}
}

func handleMapOperations(ctx context.Context, mdai VarDeps, payload eventing.ManualVariablesActionPayload, hubName, correlationID string) error {
	switch payload.Operation {
	case "add":
		return handleMapAdd(ctx, mdai, payload, hubName, correlationID)
	case "remove":
		return handleMapRemove(ctx, mdai, payload, hubName, correlationID)
	default:
		return fmt.Errorf("unsupported map operation: %s", payload.Operation)
	}
}

func handleScalarOperations(ctx context.Context, mdai VarDeps, payload eventing.ManualVariablesActionPayload, hubName, correlationID string) error {
	value, ok := payload.Data.(string)
	if !ok {
		return errors.New("data should be a string")
	}

	mdai.GetLogger().Info("Setting string", zap.String("value", value))
	return mdai.GetHandlerAdapter().SetStringValue(ctx, payload.VariableRef, hubName, value, correlationID)
}

type SetOperation func(ctx context.Context, variableKey, hubName, value, correlationID string) error

func processSetValues(
	ctx context.Context,
	mdai VarDeps,
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

		mdai.GetLogger().Info(logMessage, zap.String("value", str))
		if err := operation(ctx, variableRef, hubName, str, correlationID); err != nil {
			return err
		}
	}
	return nil
}

func handleMapAdd(ctx context.Context, mdai VarDeps, payload eventing.ManualVariablesActionPayload, hubName, correlationID string) error {
	values, ok := payload.Data.(map[string]any)
	if !ok {
		return errors.New("data should be a map[string]string")
	}

	for key, val := range values {
		str, ok := val.(string)
		if !ok {
			return fmt.Errorf("expected string, got %T", val)
		}

		mdai.GetLogger().Info("Setting value", zap.String("field", key), zap.String("value", str))
		if err := mdai.GetHandlerAdapter().AddSetMapElement(ctx, payload.VariableRef, hubName, key, str, correlationID); err != nil {
			return err
		}
	}
	return nil
}

func handleMapRemove(ctx context.Context, mdai VarDeps, payload eventing.ManualVariablesActionPayload, hubName, correlationID string) error {
	values, ok := payload.Data.([]any)
	if !ok {
		return errors.New("data should be a slice of strings")
	}

	for _, key := range values {
		k, ok := key.(string)
		if !ok {
			return fmt.Errorf("expected string, got %T", key)
		}

		mdai.GetLogger().Info("Deleting field", zap.String("field", k))
		if err := mdai.GetHandlerAdapter().RemoveElementFromMap(ctx, payload.VariableRef, hubName, k, correlationID); err != nil {
			return err
		}
	}
	return nil
}

type SlackPayload struct {
	Text   string           `json:"text"`
	Blocks []map[string]any `json:"blocks,omitempty"`
}

func HandleCallSlackWebhookFn(ctx context.Context, kube kubernetes.Interface, namespace string, event eventing.MdaiEvent, raw json.RawMessage, payloadData map[string]any) error {
	var in mdaiv1.CallWebhookAction
	if err := DecodeInputs(raw, &in); err != nil {
		return fmt.Errorf("decode call.webhook: %w", err)
	}

	webhookURL, err := resolveStringOrFrom(ctx, kube, namespace, in.URL)
	if err != nil {
		return fmt.Errorf("resolve webhook url: %w", err)
	}
	if webhookURL == "" {
		return errors.New("webhook_url must be a non-empty string")
	}
	if _, err = url.ParseRequestURI(webhookURL); err != nil {
		return fmt.Errorf("invalid webhook url: %w", err)
	}

	payload, err := buildSlackPayload(in.TemplateValues, event, payloadData)
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

func resolveStringOrFrom(ctx context.Context, kube kubernetes.Interface, namespace string, stringOrFrom mdaiv1.StringOrFrom) (string, error) {
	if stringOrFrom.Value != nil {
		return strings.TrimSpace(*stringOrFrom.Value), nil
	}
	if stringOrFrom.ValueFrom == nil {
		return "", errors.New("neither value nor valueFrom set")
	}

	if secretKeyRef := stringOrFrom.ValueFrom.SecretKeyRef; secretKeyRef != nil {
		return readSecretKey(ctx, kube, namespace, *secretKeyRef)
	}
	if configMapKeyRef := stringOrFrom.ValueFrom.ConfigMapKeyRef; configMapKeyRef != nil {
		return readConfigMapKey(ctx, kube, namespace, *configMapKeyRef)
	}
	return "", errors.New("valueFrom has neither secretKeyRef nor configMapKeyRef")
}

func readSecretKey(ctx context.Context, kube kubernetes.Interface, ns string, ref corev1.SecretKeySelector) (string, error) {
	sec, err := kube.CoreV1().Secrets(ns).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("read secret %q: %w", ref.Name, err)
	}
	raw, ok := sec.Data[ref.Key]
	if !ok {
		return "", fmt.Errorf("secret %q missing key %q", ref.Name, ref.Key)
	}
	return strings.TrimSpace(string(raw)), nil
}

func readConfigMapKey(ctx context.Context, kube kubernetes.Interface, ns string, ref corev1.ConfigMapKeySelector) (string, error) {
	cm, err := kube.CoreV1().ConfigMaps(ns).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("read configmap %q: %w", ref.Name, err)
	}
	if value, ok := cm.Data[ref.Key]; ok {
		return strings.TrimSpace(value), nil
	}
	if binaryData, ok := cm.BinaryData[ref.Key]; ok {
		return strings.TrimSpace(string(binaryData)), nil
	}
	return "", fmt.Errorf("configmap %q missing key %q", ref.Name, ref.Key)
}

func addPayloadFieldByKey(fields []map[string]string, payloadData map[string]any, key string) ([]map[string]string, error) {
	payloadValue, err := getString(payloadData, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get %s from payload with error: %w", key, err)
	}

	return append(fields, map[string]string{
		"type": "mrkdwn",
		"text": fmt.Sprintf("*%s* - %s", key, payloadValue),
	}), nil
}

func addPayloadFieldByKeyFromLabels(fields []map[string]string, payloadData map[string]any, key string) ([]map[string]string, error) {
	labels, err := ReadLabels(payloadData)
	if err != nil {
		return nil, fmt.Errorf("failed to read labels from payload with error: %w", err)
	}

	payloadValue, ok := labels[key]
	if !ok {
		payloadValue = "Unknown"
	}

	return append(fields, map[string]string{
		"type": "mrkdwn",
		"text": fmt.Sprintf("*%s* - %s", key, payloadValue),
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
	// FIXME in case of error append n/a instead of error and log error
	key, ok := args["labels_val_ref_primary"]
	if ok {
		if fields, err = addPayloadFieldByKeyFromLabels(fields, payloadData, key); err != nil {
			return SlackPayload{}, err
		}
	}
	if fields, err = addPayloadFieldByKeyFromLabels(fields, payloadData, "alertname"); err != nil {
		return SlackPayload{}, err
	}
	if fields, err = addPayloadFieldByKey(fields, payloadData, "status"); err != nil {
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
		linkTextStr := "See more"
		if linkTextExists {
			linkTextStr = linkText
		}
		payload.Blocks = append(payload.Blocks, map[string]any{
			"type": "actions",
			"elements": []map[string]any{
				{
					"type": "button",
					"text": map[string]string{
						"type": "plain_text",
						"text": linkTextStr,
					},
					"style": "primary",
					"url":   linkURL,
				},
			},
		})
	}
	return payload, nil
}

func DecodeInputs[T any](raw json.RawMessage, out *T) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	return dec.Decode(out)
}
