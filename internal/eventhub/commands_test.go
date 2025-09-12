package eventhub

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/decisiveai/mdai-data-core/audit"
	"github.com/decisiveai/mdai-data-core/eventing"
	"github.com/decisiveai/mdai-data-core/eventing/rule"
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
		AuditAdapter: audit.NewAuditAdapter(zap.NewNop(), mockClient),
	}
	return h, ma, mockClient
}

func TestCmdVarSetAdd_Success(t *testing.T) {
	h, ma, _ := newHubWithAdapter(t)

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
		Type:   rule.CmdVarSetAdd,
		Inputs: json.RawMessage(`{"set":"myset","value":"key"}`),
	}

	handler := commandDispatch[rule.CmdVarSetAdd]
	require.NotNil(t, handler)

	err := handler(h, context.Background(), ev, "ns1", cmd, payload)
	require.NoError(t, err)

	assert.Contains(t, ma.calls["AddElementToSet"], map[string]string{
		"variableKey":   "myset",
		"hubName":       "hubA",
		"value":         "valX",
		"correlationID": "cid-1",
	})
}

func TestCmdVarSetAdd_LabelMissing(t *testing.T) {
	h, _, _ := newHubWithAdapter(t)

	ev := eventing.MdaiEvent{HubName: "hubA", CorrelationID: "cid-1"}
	payload := map[string]any{
		"labels": map[string]any{
			// no "key" present
		},
	}
	cmd := rule.Command{
		Type:   rule.CmdVarSetAdd,
		Inputs: json.RawMessage(`{"set":"myset","value":"key"}`),
	}

	handler := commandDispatch[rule.CmdVarSetAdd]
	require.NotNil(t, handler)

	err := handler(h, context.Background(), ev, "ns1", cmd, payload)
	require.Error(t, err)
	require.ErrorContains(t, err, "label")
	require.ErrorContains(t, err, "key")
}

func TestCmdVarSetAdd_PayloadMissingLabels(t *testing.T) {
	h, _, _ := newHubWithAdapter(t)

	ev := eventing.MdaiEvent{HubName: "hubA", CorrelationID: "cid-1"}
	// Payload is missing the "labels" key entirely.
	payload := map[string]any{
		"some_other_key": "some_value",
	}
	cmd := rule.Command{
		Type:   rule.CmdVarSetAdd,
		Inputs: json.RawMessage(`{"set":"myset","value":"key"}`),
	}

	handler := commandDispatch[rule.CmdVarSetAdd]
	require.NotNil(t, handler)

	err := handler(h, context.Background(), ev, "ns1", cmd, payload)
	require.Error(t, err)
	require.ErrorContains(t, err, "variable.set.add: labels not found in payload")
}

func TestCmdVarSetRemove_Success(t *testing.T) {
	h, ma, _ := newHubWithAdapter(t)

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
		Type:   rule.CmdVarSetRemove,
		Inputs: json.RawMessage(`{"set":"blocklist","value":"svc"}`),
	}

	handler := commandDispatch[rule.CmdVarSetRemove]
	require.NotNil(t, handler)

	err := handler(h, context.Background(), ev, "ns2", cmd, payload)
	require.NoError(t, err)

	assert.Contains(t, ma.calls["RemoveElementFromSet"], map[string]string{
		"variableKey":   "blocklist",
		"hubName":       "hubB",
		"value":         "noisy",
		"correlationID": "cid-2",
	})
}

func TestCmdWebhookCall_URLValidation(t *testing.T) {
	h, _, _ := newHubWithAdapter(t) // provides h.registry(); kube is nil but never used in these paths

	ev := eventing.MdaiEvent{
		HubName:       "hubC",
		CorrelationID: "cid-3",
		Payload:       `{"labels":{}}`, // keep ev.Payload consistent with payloadData below
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

			// signature: func(ctx, ev, ns, cmd, payload) error
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

func TestExecVarScalarOp(t *testing.T) {
	t.Parallel()

	const opName = "test.scalar.op"

	mkCmd := func(inputs string) rule.Command {
		return rule.Command{Type: opName, Inputs: json.RawMessage(inputs)}
	}

	// Default payload includes valid labels required by ReadLabels.
	withDefaultPayload := func(extra map[string]any) map[string]any {
		p := map[string]any{
			"labels": map[string]any{
				"hub":    "hub-A",
				"source": "unit-test",
			},
		}
		for k, v := range extra {
			p[k] = v
		}
		return p
	}

	withDefaultLabels := func(extraLabels map[string]any) map[string]any {
		lbls := map[string]any{
			"hub":    "hub-A",
			"source": "unit-test",
		}
		for k, v := range extraLabels {
			lbls[k] = v
		}
		return map[string]any{"labels": lbls}
	}

	tests := []struct {
		name              string
		cmd               rule.Command
		payload           map[string]any
		setOpReturnErr    error
		expectErr         bool
		expectErrEqual    string
		expectErrContains string
		expectSetOpCalled bool
	}{
		{
			name:              "DecodeError",
			cmd:               mkCmd(`{"scalar":"myscalar","value":"mykey"`), // malformed JSON
			payload:           withDefaultPayload(nil),
			expectErr:         true,
			expectErrContains: opName + ": decode:",
			expectSetOpCalled: false,
		},
		{
			name:              "EmptyScalar",
			cmd:               mkCmd(`{"scalar":"","value":"some-key"}`),
			payload:           withDefaultPayload(nil),
			expectErrEqual:    opName + ": inputs.scalar is empty",
			expectSetOpCalled: false,
		},
		{
			name:              "EmptyValue",
			cmd:               mkCmd(`{"scalar":"myscalar","value":""}`),
			payload:           withDefaultPayload(nil),
			expectErr:         true,
			expectSetOpCalled: false,
		},
		{
			name:              "MissingValueInPayload",
			cmd:               mkCmd(`{"scalar":"myscalar","value":"missing"}`),
			payload:           withDefaultPayload(nil),
			expectErr:         true,
			expectSetOpCalled: false,
		},
		{
			name:              "NonStringPayloadValue",
			cmd:               mkCmd(`{"scalar":"myscalar","value":"k"}`),
			payload:           withDefaultPayload(map[string]any{"k": 123}),
			expectErr:         true,
			expectSetOpCalled: false,
		},
		{
			name:              "LabelsMissing",
			cmd:               mkCmd(`{"scalar":"s","value":"k"}`),
			payload:           map[string]any{"k": "v"}, // no "labels"
			expectErr:         true,                     // should be "labels not found in payload"
			expectSetOpCalled: false,
		},
		{
			name:              "LabelsWrongType",
			cmd:               mkCmd(`{"scalar":"s","value":"k"}`),
			payload:           map[string]any{"labels": "oops", "k": "v"},
			expectErr:         true, // payload.labels wrong type
			expectSetOpCalled: false,
		},
		{
			name:              "LabelsNonStringValue",
			cmd:               mkCmd(`{"scalar":"s","value":"k"}`),
			payload:           map[string]any{"labels": map[string]any{"hub": 123}, "k": "v"},
			expectErr:         true, // label value not string
			expectSetOpCalled: false,
		},
		{
			name:              "SetOpError",
			cmd:               mkCmd(`{"scalar":"myscalar","value":"k"}`),
			payload:           withDefaultLabels(map[string]any{"k": "v"}),
			setOpReturnErr:    errors.New("kaboom"),
			expectErr:         true,
			expectSetOpCalled: true,
		},
		{
			name:              "Success",
			cmd:               mkCmd(`{"scalar":"myscalar","value":"k"}`),
			payload:           withDefaultLabels(map[string]any{"k": "v"}),
			expectErr:         false,
			expectSetOpCalled: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			setOpCalled := false
			setOp := func(ctx context.Context, variableKey, hubName, value, correlationID string) error {
				setOpCalled = true
				return tc.setOpReturnErr
			}

			err := execVarScalarOp(
				context.Background(),
				opName,
				eventing.MdaiEvent{}, // correlationID can be empty for these tests
				tc.cmd,
				tc.payload,
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
		})
	}
}

func TestReadLabels(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		lbls, err := ReadLabels(map[string]any{
			"labels": map[string]any{"hub": "h1", "source": "unit"},
		})
		require.NoError(t, err)
		require.Equal(t, map[string]string{"hub": "h1", "source": "unit"}, lbls)
	})

	t.Run("Missing", func(t *testing.T) {
		_, err := ReadLabels(map[string]any{})
		require.Error(t, err)
		require.Equal(t, "labels not found in payload", err.Error())
	})

	t.Run("WrongType", func(t *testing.T) {
		_, err := ReadLabels(map[string]any{"labels": "oops"})
		require.Error(t, err)
		require.ErrorContains(t, err, "payload.labels has type")
	})

	t.Run("NonStringValue", func(t *testing.T) {
		_, err := ReadLabels(map[string]any{"labels": map[string]any{"hub": 123}})
		require.Error(t, err)
		require.ErrorContains(t, err, "label value for key hub is not a string")
	})
}
