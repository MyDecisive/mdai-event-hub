package eventhub

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/decisiveai/mdai-data-core/audit"
	"github.com/decisiveai/mdai-data-core/eventing"
	"github.com/decisiveai/mdai-data-core/eventing/rule"
	"github.com/decisiveai/mdai-data-core/interpolation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	vkmock "github.com/valkey-io/valkey-go/mock"
	"go.uber.org/mock/gomock"
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

func (m *mockHandlerAdapter) SetMapEntry(_ context.Context, variableKey, hubName, field, value, correlationID string) error {
	m.calls["AddSetMapElement"] = append(m.calls["AddSetMapElement"], map[string]string{
		"variableKey":   variableKey,
		"hubName":       hubName,
		"field":         field,
		"value":         value,
		"correlationID": correlationID,
	})
	return nil
}

func (m *mockHandlerAdapter) RemoveMapEntry(_ context.Context, variableKey, hubName, field, correlationID string) error {
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

func newHubWithAdapter(t *testing.T) (*EventHub, *mockHandlerAdapter, *vkmock.Client) {
	t.Helper()
	ma := newMockAdapter()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockClient := vkmock.NewClient(ctrl)
	h := &EventHub{
		Logger: zap.NewNop(),
		VarsAdapter: VarDeps{
			Logger:         zap.NewNop(),
			HandlerAdapter: ma,
		},
		AuditAdapter:        audit.NewAuditAdapter(zap.NewNop(), mockClient),
		InterpolationEngine: interpolation.NewEngine(zap.NewNop()),
	}
	return h, ma, mockClient
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

func TestCmdVarSetAdd_Success(t *testing.T) {
	h, ma, _ := newHubWithAdapter(t)

	payload := map[string]any{
		"labels": map[string]any{
			"key": "valX",
		},
	}
	ev := eventing.MdaiEvent{
		HubName:       "hubA",
		CorrelationID: "cid-1",
		Payload:       mustJSON(t, payload),
	}

	cmd := rule.Command{
		Type:   rule.CmdVarSetAdd,
		Inputs: json.RawMessage(`{"set":"myset","value":"${trigger:payload.labels.key}"}`),
	}

	handler := commandDispatch[rule.CmdVarSetAdd]
	require.NotNil(t, handler)

	err := handler(h, context.Background(), ev, "ns1", cmd, nil)
	require.NoError(t, err)

	assert.Contains(t, ma.calls["AddElementToSet"], map[string]string{
		"variableKey":   "myset",
		"hubName":       "hubA",
		"value":         "valX",
		"correlationID": "cid-1",
	})
}

func TestCmdVarSetAdd_LabelMissing(t *testing.T) {
	h, ma, _ := newHubWithAdapter(t)

	// labels present but "key" is missing
	ev := eventing.MdaiEvent{
		HubName:       "hubA",
		CorrelationID: "cid-1",
		Payload: mustJSON(t, map[string]any{
			"labels": map[string]any{},
		}),
	}
	cmd := rule.Command{
		Type:   rule.CmdVarSetAdd,
		Inputs: json.RawMessage(`{"set":"myset","value":"${trigger:payload.labels.key}"}`),
	}

	handler := commandDispatch[rule.CmdVarSetAdd]
	require.NotNil(t, handler)

	// With fallback behavior, this should NOT error; it should pass the original template string through.
	err := handler(h, context.Background(), ev, "ns1", cmd, nil)
	require.NoError(t, err)

	assert.Contains(t, ma.calls["AddElementToSet"], map[string]string{
		"variableKey":   "myset",
		"hubName":       "hubA",
		"value":         "${trigger:payload.labels.key}", // preserved as provided
		"correlationID": "cid-1",
	})
}

func TestCmdVarSetAdd_PayloadMissingLabels(t *testing.T) {
	h, ma, _ := newHubWithAdapter(t)

	// payload has no "labels" at all — still falls back to provided value
	ev := eventing.MdaiEvent{
		HubName:       "hubA",
		CorrelationID: "cid-1",
		Payload:       mustJSON(t, map[string]any{"some_other_key": "some_value"}),
	}
	cmd := rule.Command{
		Type:   rule.CmdVarSetAdd,
		Inputs: json.RawMessage(`{"set":"myset","value":"${trigger:payload.labels.key}"}`),
	}

	handler := commandDispatch[rule.CmdVarSetAdd]
	require.NotNil(t, handler)

	err := handler(h, context.Background(), ev, "ns1", cmd, nil)
	require.NoError(t, err)

	assert.Contains(t, ma.calls["AddElementToSet"], map[string]string{
		"variableKey":   "myset",
		"hubName":       "hubA",
		"value":         "${trigger:payload.labels.key}", // preserved as provided
		"correlationID": "cid-1",
	})
}

func TestCmdVarSetRemove_Success(t *testing.T) {
	h, ma, _ := newHubWithAdapter(t)

	payload := map[string]any{
		"labels": map[string]any{
			"svc": "noisy",
		},
	}
	ev := eventing.MdaiEvent{
		HubName:       "hubB",
		CorrelationID: "cid-2",
		Payload:       mustJSON(t, payload),
	}
	cmd := rule.Command{
		Type:   rule.CmdVarSetRemove,
		Inputs: json.RawMessage(`{"set":"blocklist","value":"${trigger:payload.labels.svc}"}`),
	}

	handler := commandDispatch[rule.CmdVarSetRemove]
	require.NotNil(t, handler)

	err := handler(h, context.Background(), ev, "ns2", cmd, nil)
	require.NoError(t, err)

	assert.Contains(t, ma.calls["RemoveElementFromSet"], map[string]string{
		"variableKey":   "blocklist",
		"hubName":       "hubB",
		"value":         "noisy",
		"correlationID": "cid-2",
	})
}

func TestCmdWebhookCall_URLValidation(t *testing.T) {
	h, _, _ := newHubWithAdapter(t) // kube is nil but not used in these paths

	ev := eventing.MdaiEvent{
		HubName:       "hubC",
		CorrelationID: "cid-3",
		Payload:       `{"labels":{}}`,
	}
	payloadData := map[string]any{"labels": map[string]any{}}

	handler := commandDispatch[rule.CmdWebhookCall]
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
			cmd := rule.Command{Type: rule.CmdWebhookCall, Inputs: tc.inputs}
			err := handler(h, context.Background(), ev, "ns3", cmd, payloadData)
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

	h, _, _ := newHubWithAdapter(t)

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

			err := h.execVarSetOp(
				context.Background(),
				"test.op",
				eventing.MdaiEvent{},
				cmd,
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

func TestExecVarScalarOp(t *testing.T) {
	t.Parallel()
	const opName = "test.scalar.op"

	mkCmd := func(inputs string) rule.Command {
		return rule.Command{Type: opName, Inputs: json.RawMessage(inputs)}
	}

	tests := []struct {
		name              string
		evPayloadJSON     string
		cmd               rule.Command
		setOpReturnErr    error
		expectErr         bool
		expectErrEqual    string
		expectErrContains string
		expectSetOpCalled bool
		expectValuePassed string
	}{
		{
			name:              "DecodeError",
			evPayloadJSON:     `{"k":"v"}`,
			cmd:               mkCmd(`{"scalar":"myscalar","value":"${trigger:payload.k"`), // malformed JSON
			expectErr:         true,
			expectErrContains: opName + ": decode:",
			expectSetOpCalled: false,
		},
		{
			name:              "EmptyScalar",
			evPayloadJSON:     `{"k":"v"}`,
			cmd:               mkCmd(`{"scalar":"","value":"${trigger:payload.k}"}`),
			expectErrEqual:    opName + ": inputs.scalar is empty",
			expectSetOpCalled: false,
		},
		{
			name:              "EmptyValue",
			evPayloadJSON:     `{"k":"v"}`,
			cmd:               mkCmd(`{"scalar":"myscalar","value":""}`),
			expectErrEqual:    opName + ": inputs.value is empty",
			expectSetOpCalled: false,
		},
		{
			name:              "MissingFieldInEventPayload_falls_back_to_provided_value",
			evPayloadJSON:     `{"k":"v"}`,
			cmd:               mkCmd(`{"scalar":"myscalar","value":"${trigger:payload.missing}"}`),
			expectErr:         false,
			expectSetOpCalled: true,
			expectValuePassed: "${trigger:payload.missing}", // preserved as provided
		},
		{
			name:              "SetOpError",
			evPayloadJSON:     `{"k":"v"}`,
			cmd:               mkCmd(`{"scalar":"myscalar","value":"${trigger:payload.k}"}`),
			setOpReturnErr:    errors.New("kaboom"),
			expectErr:         true,
			expectSetOpCalled: true,
			expectValuePassed: "v",
		},
		{
			name:              "Success",
			evPayloadJSON:     `{"k":"v"}`,
			cmd:               mkCmd(`{"scalar":"myscalar","value":"${trigger:payload.k}"}`),
			expectErr:         false,
			expectSetOpCalled: true,
			expectValuePassed: "v",
		},
	}

	h, _, _ := newHubWithAdapter(t)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			setOpCalled := false
			capturedValue := ""
			setOp := func(ctx context.Context, variableKey, hubName, value, correlationID string) error {
				setOpCalled = true
				capturedValue = value
				return tc.setOpReturnErr
			}

			err := h.execVarScalarOp(
				context.Background(),
				opName,
				eventing.MdaiEvent{Payload: tc.evPayloadJSON},
				tc.cmd,
				setOp,
			)

			switch {
			case tc.expectErrEqual != "":
				require.Error(t, err)
				require.Equal(t, tc.expectErrEqual, err.Error())
			case tc.expectErrContains != "":
				require.Error(t, err)
				require.ErrorContains(t, err, tc.expectErrContains)
			case tc.expectErr:
				require.Error(t, err)
			default:
				require.NoError(t, err)
			}

			require.Equal(t, tc.expectSetOpCalled, setOpCalled, "setOp call expectation mismatch")
			if tc.expectSetOpCalled && tc.expectValuePassed != "" {
				require.Equal(t, tc.expectValuePassed, capturedValue)
			}
		})
	}
}

func TestCmdVarMapAdd_Success(t *testing.T) {
	h, ma, _ := newHubWithAdapter(t)

	payload := map[string]any{
		"alert": map[string]any{
			"name": "HighCPU",
		},
		"instance": "server-123",
	}
	ev := eventing.MdaiEvent{
		HubName:       "hub-map-test",
		CorrelationID: "cid-map-1",
		Payload:       mustJSON(t, payload),
	}

	cmd := rule.Command{
		Type: rule.CmdVarMapAdd,
		Inputs: json.RawMessage(`{
			"map": "active_alerts",
			"key": "${trigger:payload.instance}",
			"value": "${trigger:payload.alert.name}"
		}`),
	}

	handler := commandDispatch[rule.CmdVarMapAdd]
	require.NotNil(t, handler)

	err := handler(h, context.Background(), ev, "ns-map", cmd, nil)
	require.NoError(t, err)

	require.Contains(t, ma.calls, "AddSetMapElement")
	require.Len(t, ma.calls["AddSetMapElement"], 1)

	assert.Equal(t, map[string]string{
		"variableKey":   "active_alerts",
		"hubName":       "hub-map-test",
		"field":         "server-123",
		"value":         "HighCPU",
		"correlationID": "cid-map-1",
	}, ma.calls["AddSetMapElement"][0])
}

func TestCmdVarMapRemove_Success(t *testing.T) {
	h, ma, _ := newHubWithAdapter(t)

	payload := map[string]any{
		"instance": "server-456",
	}
	ev := eventing.MdaiEvent{
		HubName:       "hub-map-remove-test",
		CorrelationID: "cid-map-remove-1",
		Payload:       mustJSON(t, payload),
	}

	cmd := rule.Command{
		Type: rule.CmdVarMapRemove,
		Inputs: json.RawMessage(`{
			"map": "stale_alerts",
			"key": "${trigger:payload.instance}"
		}`),
	}

	handler := commandDispatch[rule.CmdVarMapRemove]
	require.NotNil(t, handler)

	err := handler(h, context.Background(), ev, "ns-map-remove", cmd, nil)
	require.NoError(t, err)

	require.Contains(t, ma.calls, "RemoveElementFromMap")
	require.Len(t, ma.calls["RemoveElementFromMap"], 1)

	assert.Equal(t, map[string]string{
		"variableKey":   "stale_alerts",
		"hubName":       "hub-map-remove-test",
		"field":         "server-456",
		"correlationID": "cid-map-remove-1",
	}, ma.calls["RemoveElementFromMap"][0])
}
