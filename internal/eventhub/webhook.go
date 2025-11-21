package eventhub

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/textproto"
	"net/url"
	"strings"

	"github.com/decisiveai/mdai-data-core/eventing"
	"github.com/decisiveai/mdai-data-core/interpolation"
	mdaiv1 "github.com/decisiveai/mdai-operator/api/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const maxResponseBodySize = 4096

type SlackPayload struct {
	Text   string           `json:"text"`
	Blocks []map[string]any `json:"blocks,omitempty"`
}

func (h *EventHub) HandleCallWebhookFn(ctx context.Context, kube kubernetes.Interface, namespace string, event eventing.MdaiEvent, raw json.RawMessage, payloadData map[string]any) error {
	var inputAction mdaiv1.CallWebhookAction
	if err := DecodeInputs(raw, &inputAction); err != nil {
		return fmt.Errorf("decode call.webhook: %w", err)
	}

	webhookURL, err := resolveStringOrFrom(ctx, kube, namespace, inputAction.URL)
	if err != nil {
		return fmt.Errorf("resolve webhook url: %w", err)
	}

	if webhookURL == "" {
		return errors.New("webhook_url must be a non-empty string")
	}
	if _, err = url.ParseRequestURI(webhookURL); err != nil {
		return fmt.Errorf("invalid webhook url: %w", err)
	}

	callCtx := ctx
	cancel := func() {}
	if inputAction.Timeout != nil && inputAction.Timeout.Duration > 0 {
		callCtx, cancel = context.WithTimeout(ctx, inputAction.Timeout.Duration)
	}
	defer cancel()

	// resolve template values (from config map or secret plus literals; literals override)
	templateValues, err := resolveAllTemplateValues(callCtx, kube, namespace, inputAction.TemplateValues, inputAction.TemplateValuesFrom)
	if err != nil {
		return err
	}

	// interpolate template values with event as source before injecting into the payload
	h.InterpolationEngine.InterpolateMapWithSources(templateValues, &interpolation.TriggerSource{Event: &event})

	body, err := h.buildPayload(callCtx, kube, namespace, inputAction, event, payloadData, templateValues)
	if err != nil {
		return err
	}

	// we do not support interpolation for headers
	headers, err := resolveAllHeaders(callCtx, kube, namespace, inputAction.Headers, inputAction.HeadersFrom)
	if err != nil {
		return err
	}

	return sendWebhookRequest(callCtx, webhookURL, body, headers)
}

func sendWebhookRequest(ctx context.Context, webhookURL string, body []byte, headers http.Header) error {
	// Ensure Content-Type if we have a body and caller didn't set it
	if len(body) > 0 && headers.Get("Content-Type") == "" {
		headers.Set("Content-Type", "application/json")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, webhookURL, bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header = headers

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	// Accept any 2xx (e.g., GitHub dispatch returns 204)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodySize))
		return fmt.Errorf("non-2xx response: %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}

	return nil
}

func resolveAllHeaders(
	ctx context.Context,
	kube kubernetes.Interface,
	ns string,
	hdr map[string]string,
	from map[string]mdaiv1.ValueFromSource,
) (http.Header, error) {
	out := make(http.Header, len(hdr)+len(from))

	// 1) Secret/ConfigMap-backed headers
	for k, src := range from {
		key := textproto.CanonicalMIMEHeaderKey(k)
		switch {
		case src.SecretKeyRef != nil:
			v, err := readSecretKey(ctx, kube, ns, *src.SecretKeyRef)
			if err != nil {
				return nil, fmt.Errorf("headersFrom[%s] secret: %w", k, err)
			}
			out.Set(key, v)
		case src.ConfigMapKeyRef != nil:
			v, err := readConfigMapKey(ctx, kube, ns, *src.ConfigMapKeyRef)
			if err != nil {
				return nil, fmt.Errorf("headersFrom[%s] configmap: %w", k, err)
			}
			out.Set(key, v)
		default:
			// already enforced by CRD/webhook
		}
	}

	for k, v := range hdr {
		key := textproto.CanonicalMIMEHeaderKey(k)
		out.Set(key, v)
	}

	return out, nil
}

func (h *EventHub) buildPayload(
	ctx context.Context,
	kube kubernetes.Interface,
	ns string,
	in mdaiv1.CallWebhookAction,
	event eventing.MdaiEvent,
	payloadData map[string]any,
	templateValues map[string]string,
) ([]byte, error) {
	switch in.TemplateRef {
	case mdaiv1.TemplateRefSlack:
		return buildSlackPayloadAndMarshal(templateValues, event, payloadData)
	case mdaiv1.TemplateRefJSON:
		return h.buildJSONTemplatePayload(ctx, kube, ns, in, event, templateValues)
	default:
		return nil, fmt.Errorf("marshal payload: unknown template reference: %q", in.TemplateRef)
	}
}

func buildSlackPayloadAndMarshal(args map[string]string, event eventing.MdaiEvent, payloadData map[string]any) ([]byte, error) {
	payload, err := buildSlackPayload(args, event, payloadData)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal slack payload: %w", err)
	}
	return b, nil
}

func (h *EventHub) buildJSONTemplatePayload(ctx context.Context, kube kubernetes.Interface, ns string, in mdaiv1.CallWebhookAction, event eventing.MdaiEvent, templateValues map[string]string) ([]byte, error) {
	if in.PayloadTemplate == nil {
		return nil, errors.New("payloadTemplate must be provided")
	}
	rawTemplate, err := resolveStringOrFrom(ctx, kube, ns, *in.PayloadTemplate)
	if err != nil {
		return nil, fmt.Errorf("resolve payloadTemplate: %w", err)
	}

	// ${template:*} && ${trigger:*}
	rawTemplate = h.InterpolationEngine.InterpolateWithSources(rawTemplate, interpolation.TemplateSource{Values: templateValues}, &interpolation.TriggerSource{Event: &event})

	b := []byte(rawTemplate)
	if !json.Valid(b) {
		return nil, errors.New("payloadTemplate is not valid JSON")
	}
	return []byte(rawTemplate), nil
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

func addPayloadFieldByKeyFromLabels(fields []map[string]string, payloadData map[string]any, key string) ([]map[string]string, error) {
	labels, err := readLabels(payloadData) // TODO see if we should use interpolation here
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

func readLabels(payloadData map[string]any) (map[string]string, error) {
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

func resolveStringOrFrom(ctx context.Context, kube kubernetes.Interface, namespace string, stringOrFrom mdaiv1.StringOrFrom) (string, error) {
	if stringOrFrom.Value != nil {
		return *stringOrFrom.Value, nil
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

func resolveAllTemplateValues(ctx context.Context, kube kubernetes.Interface, ns string, vals map[string]string, from map[string]mdaiv1.ValueFromSource) (map[string]string, error) {
	out := make(map[string]string, len(vals)+len(from))
	for k, src := range from {
		switch {
		case src.SecretKeyRef != nil:
			v, err := readSecretKey(ctx, kube, ns, *src.SecretKeyRef)
			if err != nil {
				return nil, fmt.Errorf("templateValuesFrom[%s] secret: %w", k, err)
			}
			out[k] = v
		case src.ConfigMapKeyRef != nil:
			v, err := readConfigMapKey(ctx, kube, ns, *src.ConfigMapKeyRef)
			if err != nil {
				return nil, fmt.Errorf("templateValuesFrom[%s] configmap: %w", k, err)
			}
			out[k] = v
		default:
			return nil, fmt.Errorf("templateValuesFrom[%s]: no source", k)
		}
	}
	maps.Copy(out, vals)
	return out, nil
}
