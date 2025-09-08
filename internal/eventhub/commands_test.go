package eventhub

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/decisiveai/mdai-data-core/eventing"
	"github.com/decisiveai/mdai-data-core/eventing/rule"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type mockHandlerAdapter struct {
	calls map[string][]map[string]string
}

func newMockAdapter() *mockHandlerAdapter {
	return &mockHandlerAdapter{calls: make(map[string][]map[string]string)}
}

func (m *mockHandlerAdapter) AddElementToSet(_ context.Context, variableKey, hubName, value, correlationID string) error {
	m.calls["AddElementToSet"] = append(m.calls["AddElementToSet"], map[string]string{
		"variableKey":   variableKey,
		"hubName":       hubName,
		"value":         value,
		"correlationID": correlationID,
	})
	return nil
}

func (m *mockHandlerAdapter) RemoveElementFromSet(_ context.Context, variableKey, hubName, value, correlationID string) error {
	m.calls["RemoveElementFromSet"] = append(m.calls["RemoveElementFromSet"], map[string]string{
		"variableKey":   variableKey,
		"hubName":       hubName,
		"value":         value,
		"correlationID": correlationID,
	})
	return nil
}

func (m *mockHandlerAdapter) AddSetMapElement(_ context.Context, variableKey, hubName, field, value, correlationID string) error {
	m.calls["AddSetMapElement"] = append(m.calls["AddSetMapElement"], map[string]string{
		"variableKey":   variableKey,
		"hubName":       hubName,
		"field":         field,
		"value":         value,
		"correlationID": correlationID,
	})
	return nil
}

func (m *mockHandlerAdapter) RemoveElementFromMap(_ context.Context, variableKey, hubName, field, correlationID string) error {
	m.calls["RemoveElementFromMap"] = append(m.calls["RemoveElementFromMap"], map[string]string{
		"variableKey":   variableKey,
		"hubName":       hubName,
		"field":         field,
		"correlationID": correlationID,
	})
	return nil
}

func (m *mockHandlerAdapter) SetStringValue(_ context.Context, variableKey, hubName, value, correlationID string) error {
	m.calls["SetStringValue"] = append(m.calls["SetStringValue"], map[string]string{
		"variableKey":   variableKey,
		"hubName":       hubName,
		"value":         value,
		"correlationID": correlationID,
	})
	return nil
}

func newHubWithAdapter(t *testing.T) (*EventHub, *mockHandlerAdapter) {
	t.Helper()
	ma := newMockAdapter()
	h := &EventHub{
		Logger:         zap.NewNop(),
		HandlerAdapter: ma,
		// Kube nil is fine for the webhook missing-URL test (we fail earlier).
	}
	return h, ma
}

func TestRegistry_ContainsExpectedCommands(t *testing.T) {
	h, _ := newHubWithAdapter(t)
	reg := h.registry()

	require.Contains(t, reg, CmdVarSetAdd)
	require.Contains(t, reg, CmdVarSetRemove)
	require.Contains(t, reg, CmdWebhookCall)
}

func TestCmdVarSetAdd_Success(t *testing.T) {
	h, ma := newHubWithAdapter(t)

	ev := eventing.MdaiEvent{
		HubName:       "hubA",
		CorrelationID: "cid-1",
		// payload map is passed separately (no need for JSON here)
	}
	payload := map[string]any{
		"labels": map[string]any{
			"key": "valX",
		},
	}
	cmd := rule.Command{
		Type:   CmdVarSetAdd,
		Inputs: json.RawMessage(`{"set":"myset","value":"key"}`),
	}

	handler := h.registry()[CmdVarSetAdd]
	require.NotNil(t, handler)

	err := handler(context.Background(), ev, "ns1", cmd, payload)
	require.NoError(t, err)

	assert.Contains(t, ma.calls["AddElementToSet"], map[string]string{
		"variableKey":   "myset",
		"hubName":       "hubA",
		"value":         "valX",
		"correlationID": "cid-1",
	})
}

func TestCmdVarSetAdd_LabelMissing(t *testing.T) {
	h, _ := newHubWithAdapter(t)

	ev := eventing.MdaiEvent{HubName: "hubA", CorrelationID: "cid-1"}
	payload := map[string]any{
		"labels": map[string]any{
			// no "key" present
		},
	}
	cmd := rule.Command{
		Type:   CmdVarSetAdd,
		Inputs: json.RawMessage(`{"set":"myset","value":"key"}`),
	}

	handler := h.registry()[CmdVarSetAdd]
	require.NotNil(t, handler)

	err := handler(context.Background(), ev, "ns1", cmd, payload)
	require.Error(t, err)
	require.ErrorContains(t, err, "label")
	require.ErrorContains(t, err, "key")
}

func TestCmdVarSetAdd_PayloadMissingLabels(t *testing.T) {
	h, _ := newHubWithAdapter(t)

	ev := eventing.MdaiEvent{HubName: "hubA", CorrelationID: "cid-1"}
	// Payload is missing the "labels" key entirely.
	payload := map[string]any{
		"some_other_key": "some_value",
	}
	cmd := rule.Command{
		Type:   CmdVarSetAdd,
		Inputs: json.RawMessage(`{"set":"myset","value":"key"}`),
	}

	handler := h.registry()[CmdVarSetAdd]
	require.NotNil(t, handler)

	err := handler(context.Background(), ev, "ns1", cmd, payload)
	require.Error(t, err)
	require.ErrorContains(t, err, "variable.set.add: labels not found in payload")
}

func TestCmdVarSetRemove_Success(t *testing.T) {
	h, ma := newHubWithAdapter(t)

	ev := eventing.MdaiEvent{
		HubName:       "hubB",
		CorrelationID: "cid-2",
	}
	payload := map[string]any{
		"labels": map[string]any{
			"svc": "noisy",
		},
	}
	cmd := rule.Command{
		Type:   CmdVarSetRemove,
		Inputs: json.RawMessage(`{"set":"blocklist","value":"svc"}`),
	}

	handler := h.registry()[CmdVarSetRemove]
	require.NotNil(t, handler)

	err := handler(context.Background(), ev, "ns2", cmd, payload)
	require.NoError(t, err)

	assert.Contains(t, ma.calls["RemoveElementFromSet"], map[string]string{
		"variableKey":   "blocklist",
		"hubName":       "hubB",
		"value":         "noisy",
		"correlationID": "cid-2",
	})
}

func TestCmdWebhookCall_URLValidation(t *testing.T) {
	h, _ := newHubWithAdapter(t) // provides h.registry(); kube is nil but never used in these paths

	ev := eventing.MdaiEvent{
		HubName:       "hubC",
		CorrelationID: "cid-3",
		Payload:       `{"labels":{}}`, // keep ev.Payload consistent with payloadData below
	}
	payloadData := map[string]any{"labels": map[string]any{}}

	handler := h.registry()[CmdWebhookCall]
	require.NotNil(t, handler)

	tests := []struct {
		name      string
		inputs    json.RawMessage
		wantError string
	}{
		{
			name:      "no url provided",
			inputs:    json.RawMessage(`{"templateValues":{"message":"hi"}}`),
			wantError: "neither value nor valueFrom set",
		},
		{
			name:      "empty url value",
			inputs:    json.RawMessage(`{"url":{"value":""},"templateValues":{"message":"hi"}}`),
			wantError: "webhook_url must be a non-empty string",
		},
		{
			name:      "invalid url format",
			inputs:    json.RawMessage(`{"url":{"value":"::not-a-url"},"templateValues":{"message":"hi"}}`),
			wantError: "invalid webhook url",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cmd := rule.Command{Type: CmdWebhookCall, Inputs: tc.inputs}

			// signature: func(ctx, ev, ns, cmd, payload) error
			err := handler(context.Background(), ev, "ns3", cmd, payloadData)
			require.Error(t, err)
			require.ErrorContains(t, err, tc.wantError)
		})
	}
}

func TestExecVarSetOp_ErrorCases(t *testing.T) {
	tcases := []struct {
		name            string
		inputsJSON      string
		wantErrEq       string
		wantErrContains string
	}{
		{
			name:       "Missing value",
			inputsJSON: `{"set":"myset"}`,
			wantErrEq:  "test.op: inputs.value is empty",
		},
		{
			name:       "Missing set",
			inputsJSON: `{"value":"some-key"}`,
			wantErrEq:  "test.op: inputs.set is empty",
		},
		{
			name:       "Empty value",
			inputsJSON: `{"set":"myset","value":""}`,
			wantErrEq:  "test.op: inputs.value is empty",
		},
		{
			name:            "Decode error",
			inputsJSON:      `{"set":"myset","value":"mykey"`, // malformed JSON
			wantErrContains: "test.op: decode:",
		},
	}

	for _, tc := range tcases {
		t.Run(tc.name, func(t *testing.T) {
			setCalled := false
			setOp := func(ctx context.Context, variableKey, hubName, value, correlationID string) error {
				setCalled = true
				return nil
			}

			cmd := rule.Command{
				Type:   "test.op",
				Inputs: json.RawMessage(tc.inputsJSON),
			}

			err := execVarSetOp(
				context.Background(),
				"test.op",
				eventing.MdaiEvent{},
				cmd,
				map[string]any{},
				setOp,
			)

			require.Error(t, err)
			if tc.wantErrEq != "" {
				require.Equal(t, tc.wantErrEq, err.Error())
			}
			if tc.wantErrContains != "" {
				require.ErrorContains(t, err, tc.wantErrContains)
			}
			require.False(t, setCalled, "setOp should not have been called")
		})
	}
}
