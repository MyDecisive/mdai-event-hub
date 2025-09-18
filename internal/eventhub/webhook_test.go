package eventhub

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/decisiveai/mdai-data-core/eventing"
	mdaiv1 "github.com/decisiveai/mdai-operator/api/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"
)

func TestBuildSlackPayload(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		desc     string
		args     map[string]string
		event    eventing.MdaiEvent
		payload  map[string]any
		expected SlackPayload
	}{
		{
			desc: "build minimal slack payload",
			args: map[string]string{},
			event: eventing.MdaiEvent{
				HubName:   "foobar",
				Name:      "barbaz",
				Timestamp: time.Date(2021, time.September, 21, 9, 21, 9, 21, time.UTC),
			},
			payload: map[string]any{
				"status": "whoa",
				"labels": map[string]any{},
			},
			expected: SlackPayload{
				Text: "MDAI Hub Event - foobar - barbaz",
				Blocks: []map[string]any{
					{
						"type": "section",
						"text": map[string]string{
							"type": "mrkdwn",
							"text": "*MDAI Hub Event - foobar - barbaz*",
						},
					},
					{
						"type": "section",
						"fields": []map[string]string{
							{
								"type": "mrkdwn",
								"text": "*Alert timestamp* - 2021-09-21 09:21:09.000000021 +0000 UTC",
							},
							{
								"type": "mrkdwn",
								"text": "*alertname* - Unknown",
							},
							{
								"type": "mrkdwn",
								"text": "*status* - whoa",
							},
						},
					},
				},
			},
		},
		{
			desc: "build more complex slack payload",
			args: map[string]string{
				"labels_val_ref_primary": "lol",
				"message":                "SLACKY MCSLACKFACE LOL",
				"link_text":              "CLICK HERE FOR FREE IPAD!",
				"link_url":               "https://www.example.com",
			},
			event: eventing.MdaiEvent{
				HubName:   "foobaz",
				Name:      "barbar",
				Timestamp: time.Date(2021, time.September, 21, 9, 21, 9, 21, time.UTC),
			},
			payload: map[string]any{
				"status": "whoa",
				"labels": map[string]any{
					"lol":       "wut",
					"alertname": "k.",
				},
			},
			expected: SlackPayload{
				Text: "SLACKY MCSLACKFACE LOL",
				Blocks: []map[string]any{
					{
						"type": "section",
						"text": map[string]string{
							"type": "mrkdwn",
							"text": "*SLACKY MCSLACKFACE LOL*",
						},
					},
					{
						"type": "section",
						"fields": []map[string]string{
							{
								"type": "mrkdwn",
								"text": "*Alert timestamp* - 2021-09-21 09:21:09.000000021 +0000 UTC",
							},
							{
								"type": "mrkdwn",
								"text": "*lol* - wut",
							},
							{
								"type": "mrkdwn",
								"text": "*alertname* - k.",
							},
							{
								"type": "mrkdwn",
								"text": "*status* - whoa",
							},
						},
					},
					{
						"type": "actions",
						"elements": []map[string]any{
							{
								"type": "button",
								"text": map[string]string{
									"type": "plain_text",
									"text": "CLICK HERE FOR FREE IPAD!",
								},
								"style": "primary",
								"url":   "https://www.example.com",
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()
			actual, err := buildSlackPayload(tc.args, tc.event, tc.payload)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestHandleCallSlackWebhook_Success_WithLiteralURL(t *testing.T) {
	t.Parallel()

	var gotMethod, gotCT string
	var recv SlackPayload
	var handlerErr error

	// Fake Slack endpoint
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotCT = r.Header.Get("Content-Type")
		body, err := io.ReadAll(r.Body)
		_ = r.Body.Close()
		if err != nil {
			handlerErr = err
			http.Error(w, "read body", http.StatusBadRequest)
			return
		}
		if err := json.Unmarshal(body, &recv); err != nil {
			handlerErr = err
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	kube := k8sfake.NewClientset()

	// Minimal, valid inputs JSON for CallWebhookAction with direct URL value
	raw := json.RawMessage(`{
		"url": {"value": "` + srv.URL + `"},
		"templateValues": {"message":"hello"}
	}`)

	ev := eventing.MdaiEvent{HubName: "h", Name: "n"}
	payload := map[string]any{"status": "ok", "labels": map[string]any{}}

	err := HandleCallSlackWebhookFn(context.Background(), kube, "ns1", ev, raw, payload)
	require.NoError(t, err)
	require.NoError(t, handlerErr)
	require.Equal(t, http.MethodPost, gotMethod)
	require.Equal(t, "application/json", gotCT)
	require.Equal(t, "hello", recv.Text)
}

func TestHandleCallSlackWebhook_Success_WithSecretRef(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "slack-webhook-secret", Namespace: "ns1"},
		Data:       map[string][]byte{"url": []byte("  " + srv.URL + "  ")}, // exercise TrimSpace
	}
	kube := k8sfake.NewClientset(sec)

	raw := json.RawMessage(`{
		"url": {"valueFrom":{"secretKeyRef":{"name":"slack-webhook-secret","key":"url"}}},
		"templateValues": {"message":"ok"}
	}`)
	ev := eventing.MdaiEvent{}
	payload := map[string]any{"status": "ok", "labels": map[string]any{}}

	err := HandleCallSlackWebhookFn(context.Background(), kube, "ns1", ev, raw, payload)
	require.NoError(t, err)
}

func TestHandleCallSlackWebhook_Success_WithConfigMapBinaryData(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "cfg", Namespace: "ns1"},
		BinaryData: map[string][]byte{"hook": []byte(srv.URL)},
	}
	kube := k8sfake.NewClientset(cm)

	raw := json.RawMessage(`{
		"url": {"valueFrom":{"configMapKeyRef":{"name":"cfg","key":"hook"}}},
		"templateValues": {"message":"ok"}
	}`)
	ev := eventing.MdaiEvent{}
	payload := map[string]any{"status": "ok", "labels": map[string]any{}}

	err := HandleCallSlackWebhookFn(context.Background(), kube, "ns1", ev, raw, payload)
	require.NoError(t, err)
}

func TestHandleCallSlackWebhook_Non200_IsError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer srv.Close()

	kube := k8sfake.NewClientset()
	raw := json.RawMessage(`{"url":{"value":"` + srv.URL + `"},"templateValues":{}}`)
	ev := eventing.MdaiEvent{}
	payload := map[string]any{"status": "ok", "labels": map[string]any{}}

	err := HandleCallSlackWebhookFn(context.Background(), kube, "ns1", ev, raw, payload)
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-200 response: 500")
}

func TestHandleCallSlackWebhook_InvalidOrEmptyURL(t *testing.T) {
	t.Parallel()

	kube := k8sfake.NewClientset()
	ev := eventing.MdaiEvent{}
	payload := map[string]any{"status": "ok", "labels": map[string]any{}}

	// Empty
	rawEmpty := json.RawMessage(`{"url":{"value":""},"templateValues":{}}`)
	err := HandleCallSlackWebhookFn(context.Background(), kube, "ns1", ev, rawEmpty, payload)
	require.Error(t, err)
	require.Contains(t, err.Error(), "webhook_url must be a non-empty string")

	// Invalid format
	rawBad := json.RawMessage(`{"url":{"value":"::not-a-url"},"templateValues":{}}`)
	err = HandleCallSlackWebhookFn(context.Background(), kube, "ns1", ev, rawBad, payload)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid webhook url")
}

func TestHandleCallSlackWebhook_DecodeError_And_BuildPayloadError(t *testing.T) {
	t.Parallel()

	kube := k8sfake.NewClientset()

	// Malformed inputs JSON → decode error
	rawBadJSON := json.RawMessage(`{"url":{"value":"http://example.com"}`)
	err := HandleCallSlackWebhookFn(context.Background(), kube, "ns1", eventing.MdaiEvent{}, rawBadJSON, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "decode call.webhook:")

	// Missing "status" in payload → buildSlackPayload error via addPayloadFieldByKey
	rawOK := json.RawMessage(`{"url":{"value":"http://example.com"},"templateValues":{}}`)
	err = HandleCallSlackWebhookFn(context.Background(), kube, "ns1", eventing.MdaiEvent{}, rawOK, map[string]any{
		"labels": map[string]any{},
		// "status" omitted
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to get status")
}

func TestResolveStringOrFrom_Branches(t *testing.T) {
	t.Parallel()

	kube := k8sfake.NewClientset()

	// Neither value nor valueFrom
	out, err := resolveStringOrFrom(context.Background(), kube, "ns", mdaiv1.StringOrFrom{})
	require.Error(t, err)
	require.Empty(t, out)

	// valueFrom without refs
	out, err = resolveStringOrFrom(context.Background(), kube, "ns", mdaiv1.StringOrFrom{
		ValueFrom: &mdaiv1.ValueFromSource{}, // zero: no secret/configMap
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "neither secretKeyRef nor configMapKeyRef")
	require.Empty(t, out)

	// Secret exists but missing key
	_ = kube.CoreV1().Secrets("ns").Delete(context.Background(), "s1", metav1.DeleteOptions{})
	_, _ = kube.CoreV1().Secrets("ns").Create(context.Background(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "s1"},
		Data:       map[string][]byte{"other": []byte("x")},
	}, metav1.CreateOptions{})
	out, err = resolveStringOrFrom(context.Background(), kube, "ns", mdaiv1.StringOrFrom{
		ValueFrom: &mdaiv1.ValueFromSource{SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "s1"},
			Key:                  "missing",
		}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing key")
	require.Empty(t, out)

	// ConfigMap exists but missing key
	_, _ = kube.CoreV1().ConfigMaps("ns").Create(context.Background(), &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "cm1"},
		Data:       map[string]string{"other": "y"},
	}, metav1.CreateOptions{})
	out, err = resolveStringOrFrom(context.Background(), kube, "ns", mdaiv1.StringOrFrom{
		ValueFrom: &mdaiv1.ValueFromSource{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "cm1"},
			Key:                  "missing",
		}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing key")
	require.Empty(t, out)
}

func TestReadLabels_ErrorCases(t *testing.T) {
	t.Parallel()

	_, err := readLabels(map[string]any{}) // no "labels"
	require.Error(t, err)
	require.Contains(t, err.Error(), "labels not found")

	_, err = readLabels(map[string]any{"labels": "oops"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "payload.labels has type string")

	_, err = readLabels(map[string]any{
		"labels": map[string]any{"ok": "v", "bad": 123},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "label value for key bad is not a string")
}

func TestAddPayloadFieldByKey_ErrorCases(t *testing.T) {
	t.Parallel()

	// Missing key
	_, err := addPayloadFieldByKey(nil, map[string]any{"status": "ok"}, "missing")
	require.Error(t, err)
	require.Contains(t, err.Error(), "key missing not found")

	// Wrong type
	_, err = addPayloadFieldByKey(nil, map[string]any{"status": 123}, "status")
	require.Error(t, err)
	require.Contains(t, err.Error(), "value is not a string")
}

func TestBuildSlackPayload_UnknownsAndLink(t *testing.T) {
	t.Parallel()

	args := map[string]string{
		"labels_val_ref_primary": "lol",
		"link_text":              "open",
		"link_url":               "https://x.y",
	}
	ev := eventing.MdaiEvent{}
	payload := map[string]any{
		"status": "ok",
		"labels": map[string]any{
			// "lol" missing → should render "Unknown" if we asked via labels_val_ref_primary,
			// but buildSlackPayload treats missing primary as "Unknown" silently
			"alertname": "name",
		},
	}

	got, err := buildSlackPayload(args, ev, payload)
	require.NoError(t, err)

	// Sanity: must include the actions block with a button URL
	foundURL := false
	for _, b := range got.Blocks {
		if b["type"] == "actions" {
			el, ok := b["elements"].([]map[string]any)
			require.True(t, ok, "blocks[actions].elements must be []map[string]any")
			if len(el) == 1 && el[0]["url"] == "https://x.y" {
				foundURL = true
			}
		}
	}
	require.True(t, foundURL, "expected actions block with link button")
}

func TestHandleCallSlackWebhook_PropagatesHTTPDoError(t *testing.T) {
	t.Parallel()

	// Use an un-routable URL to force http.Do error (quickly)
	badURL := "http://127.0.0.1:1"
	kube := k8sfake.NewClientset()
	raw := json.RawMessage(`{"url":{"value":"` + badURL + `"},"templateValues":{}}`)

	err := HandleCallSlackWebhookFn(context.Background(), kube, "ns", eventing.MdaiEvent{}, raw, map[string]any{
		"status": "ok",
		"labels": map[string]any{},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to send request")
}

// Negative: configmap/secret not found.
func TestReadSecretKeyAndConfigMapKey_NotFound(t *testing.T) {
	t.Parallel()

	kube := k8sfake.NewClientset()
	_, err := readSecretKey(context.Background(), kube, "ns", corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "nope"},
		Key:                  "k",
	})
	require.Error(t, err)
	_, err = readConfigMapKey(context.Background(), kube, "ns", corev1.ConfigMapKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "nope"},
		Key:                  "k",
	})
	require.Error(t, err)
}
