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
		"variableKey": variableKey, "hubName": hubName, "value": value, "correlationID": correlationID,
	})
	return nil
}

func (m *mockHandlerAdapter) RemoveElementFromSet(_ context.Context, variableKey, hubName, value, correlationID string) error {
	m.calls["RemoveElementFromSet"] = append(m.calls["RemoveElementFromSet"], map[string]string{
		"variableKey": variableKey, "hubName": hubName, "value": value, "correlationID": correlationID,
	})
	return nil
}

func (m *mockHandlerAdapter) SetMapEntry(_ context.Context, variableKey, hubName, field, value, correlationID string) error {
	m.calls["AddSetMapElement"] = append(m.calls["AddSetMapElement"], map[string]string{
		"variableKey": variableKey, "hubName": hubName, "field": field, "value": value, "correlationID": correlationID,
	})
	return nil
}

func (m *mockHandlerAdapter) RemoveMapEntry(_ context.Context, variableKey, hubName, field, correlationID string) error {
	m.calls["RemoveElementFromMap"] = append(m.calls["RemoveElementFromMap"], map[string]string{
		"variableKey": variableKey, "hubName": hubName, "field": field, "correlationID": correlationID,
	})
	return nil
}

func (m *mockHandlerAdapter) SetStringValue(_ context.Context, variableKey, hubName, value, correlationID string) error {
	m.calls["SetStringValue"] = append(m.calls["SetStringValue"], map[string]string{
		"variableKey": variableKey, "hubName": hubName, "value": value, "correlationID": correlationID,
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

func mkEvent(hub, cid string, payload map[string]any) eventing.MdaiEvent {
	return eventing.MdaiEvent{
		HubName:       hub,
		CorrelationID: cid,
		Payload: func() string {
			if payload == nil {
				return ""
			}
			b, _ := json.Marshal(payload)
			return string(b)
		}(),
	}
}

func requireAdapterCall(t *testing.T, ma *mockHandlerAdapter, bucket string, want map[string]string) {
	t.Helper()
	require.Contains(t, ma.calls, bucket, "bucket %q not called", bucket)
	require.Len(t, ma.calls[bucket], 1, "expected exactly one call in %s", bucket)
	require.Equal(t, want, ma.calls[bucket][0])
}

func TestCmdVarSetAdd_Table(t *testing.T) {
	h, ma, _ := newHubWithAdapter(t)

	tests := []struct {
		name       string
		payload    map[string]any
		inputValue string // Inputs.value
		wantValue  string // what adapter should receive after interpolation
	}{
		{
			name:       "resolved label",
			payload:    map[string]any{"labels": map[string]any{"key": "valX"}},
			inputValue: `${trigger:payload.labels.key}`,
			wantValue:  "valX",
		},
		{
			name:       "missing key → fallback preserves template",
			payload:    map[string]any{"labels": map[string]any{}},
			inputValue: `${trigger:payload.labels.key}`,
			wantValue:  `${trigger:payload.labels.key}`,
		},
		{
			name:       "labels missing → fallback preserves template",
			payload:    map[string]any{"some_other_key": "v"},
			inputValue: `${trigger:payload.labels.key}`,
			wantValue:  `${trigger:payload.labels.key}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ev := mkEvent("hubA", "cid-1", tc.payload)
			cmd := rule.Command{Type: rule.CmdVarSetAdd, Inputs: json.RawMessage(`{"set":"myset","value":"` + tc.inputValue + `"}`)}

			handler := commandDispatch[rule.CmdVarSetAdd]
			require.NotNil(t, handler)

			err := handler(h, context.Background(), ev, "ns", cmd, nil)
			require.NoError(t, err)

			requireAdapterCall(t, ma, "AddElementToSet", map[string]string{
				"variableKey": "myset", "hubName": "hubA", "value": tc.wantValue, "correlationID": "cid-1",
			})
			// reset for next run
			ma.calls = make(map[string][]map[string]string)
		})
	}
}

func TestCmdVarMapAdd_Table(t *testing.T) {
	h, ma, _ := newHubWithAdapter(t)

	type tc struct {
		name      string
		evPayload map[string]any
		inputs    string
		wantErr   string
		wantCall  map[string]string // expected call into AddSetMapElement
	}
	cases := []tc{
		{
			name:      "success",
			evPayload: map[string]any{"alert": map[string]any{"name": "HighCPU"}, "instance": "server-123"},
			inputs:    `{"map":"active_alerts","key":"${trigger:payload.instance}","value":"${trigger:payload.alert.name}"}`,
			wantCall: map[string]string{
				"variableKey": "active_alerts", "hubName": "hub-map-test", "field": "server-123", "value": "HighCPU", "correlationID": "cid-map-1",
			},
		},
		{
			name:      "missing value → error",
			evPayload: nil,
			inputs:    `{"map":"active_alerts","key":"some-key"}`,
			wantErr:   "variable.map.add: inputs.value is empty",
		},
		{
			name:      "missing map → error",
			evPayload: nil,
			inputs:    `{"key":"some-key","value":"some-value"}`,
			wantErr:   "variable.map.add: inputs.map is empty",
		},
		{
			name:      "malformed json → decode error",
			evPayload: nil,
			inputs:    `{"map":"active_alerts","key":"some-key"`,
			wantErr:   "variable.map.add: decode:",
		},
		{
			name:      "empty value pointer → error",
			evPayload: nil,
			inputs:    `{"map":"m","key":"k","value":""}`,
			wantErr:   "variable.map.add: inputs.value is empty",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ev := eventing.MdaiEvent{HubName: "hub-map-test", CorrelationID: "cid-map-1", Payload: mustJSON(t, c.evPayload)}
			cmd := rule.Command{Type: rule.CmdVarMapAdd, Inputs: json.RawMessage(c.inputs)}
			handler := commandDispatch[rule.CmdVarMapAdd]
			require.NotNil(t, handler)

			err := handler(h, context.Background(), ev, "ns", cmd, nil)
			if c.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), c.wantErr)
				require.Empty(t, ma.calls, "adapter should not have been called")
			} else {
				require.NoError(t, err)
				requireAdapterCall(t, ma, "AddSetMapElement", c.wantCall)
			}
			ma.calls = make(map[string][]map[string]string)
		})
	}
}

func TestCmdVarMapRemove_Table(t *testing.T) {
	h, ma, _ := newHubWithAdapter(t)

	type tc struct {
		name      string
		evPayload map[string]any
		inputs    string
		wantErr   string
		wantCall  map[string]string
	}
	cases := []tc{
		{
			name:      "success",
			evPayload: map[string]any{"instance": "server-456"},
			inputs:    `{"map":"stale_alerts","key":"${trigger:payload.instance}"}`,
			wantCall: map[string]string{
				"variableKey": "stale_alerts", "hubName": "hub-map-remove-test", "field": "server-456", "correlationID": "cid-map-remove-1",
			},
		},
		{
			name:    "missing map → error",
			inputs:  `{"key":"${trigger:payload.k}"}`,
			wantErr: "variable.map.remove: inputs.map is empty",
		},
		{
			name:    "decode error",
			inputs:  `{"map":"m","key":"x"`,
			wantErr: "variable.map.remove: decode:",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ev := eventing.MdaiEvent{HubName: "hub-map-remove-test", CorrelationID: "cid-map-remove-1", Payload: mustJSON(t, c.evPayload)}
			cmd := rule.Command{Type: rule.CmdVarMapRemove, Inputs: json.RawMessage(c.inputs)}
			handler := commandDispatch[rule.CmdVarMapRemove]
			require.NotNil(t, handler)

			err := handler(h, context.Background(), ev, "ns", cmd, nil)
			if c.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), c.wantErr)
				require.Empty(t, ma.calls["RemoveElementFromMap"])
			} else {
				require.NoError(t, err)
				requireAdapterCall(t, ma, "RemoveElementFromMap", c.wantCall)
			}
			ma.calls = make(map[string][]map[string]string)
		})
	}
}

func TestCmdVarScalarUpdate_Success_Dispatch(t *testing.T) {
	h, ma, _ := newHubWithAdapter(t)

	ev := eventing.MdaiEvent{HubName: "hub-scalar", CorrelationID: "cid-scalar-1", Payload: `{"k":"v"}`}
	cmd := rule.Command{Type: rule.CmdVarScalarUpdate, Inputs: json.RawMessage(`{"scalar":"my-scalar","value":"${trigger:payload.k}"}`)}

	handler := commandDispatch[rule.CmdVarScalarUpdate]
	require.NotNil(t, handler)
	require.NoError(t, handler(h, context.Background(), ev, "ns", cmd, nil))

	requireAdapterCall(t, ma, "SetStringValue", map[string]string{
		"variableKey": "my-scalar", "hubName": "hub-scalar", "value": "v", "correlationID": "cid-scalar-1",
	})
}

func TestProcessCommandsForEvent_Smoke(t *testing.T) {
	h, ma, _ := newHubWithAdapter(t)

	ev := mkEvent("hub-multi", "cid-multi-1", map[string]any{"labels": map[string]any{"a": "foo"}})
	cmds := []rule.Command{
		{Type: rule.CmdVarSetAdd, Inputs: json.RawMessage(`{"set":"s1","value":"${trigger:payload.labels.a}"}`)},
		{Type: rule.CmdVarSetRemove, Inputs: json.RawMessage(`{"set":"s2","value":"${trigger:payload.labels.a}"}`)},
	}
	require.NoError(t, h.processCommandsForEvent(context.Background(), ev, cmds, "ns", nil, "alerting"))

	requireAdapterCall(t, ma, "AddElementToSet", map[string]string{"variableKey": "s1", "hubName": "hub-multi", "value": "foo", "correlationID": "cid-multi-1"})
	requireAdapterCall(t, ma, "RemoveElementFromSet", map[string]string{"variableKey": "s2", "hubName": "hub-multi", "value": "foo", "correlationID": "cid-multi-1"})
}

func TestProcessCommandsForEvent_UnsupportedType(t *testing.T) {
	h, _, _ := newHubWithAdapter(t)

	ev := eventing.MdaiEvent{HubName: "h"}
	cmds := []rule.Command{{Type: rule.CommandType("does.not.exist")}}

	err := h.processCommandsForEvent(context.Background(), ev, cmds, "ns", nil, "alerting")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported command type")
}

func TestProcessCommandsForEvent_HandlerErrorBubbled(t *testing.T) {
	h, _, _ := newHubWithAdapter(t)

	// Missing "set" triggers execVarSetOp validation error → wrapped by processCommandsForEvent.
	ev := eventing.MdaiEvent{HubName: "h", Payload: mustJSON(t, map[string]any{"labels": map[string]any{"k": "v"}})}
	cmds := []rule.Command{{Type: rule.CmdVarSetAdd, Inputs: json.RawMessage(`{"value":"${trigger:payload.labels.k}"}`)}}

	err := h.processCommandsForEvent(context.Background(), ev, cmds, "ns", nil, "alerting")
	require.Error(t, err)
	require.Contains(t, err.Error(), "command 0 (variable.set.add) failed")
}

func TestProcessCommandsForEvent_Success_Multiple(t *testing.T) {
	h, ma, _ := newHubWithAdapter(t)

	ev := eventing.MdaiEvent{
		HubName:       "hub-multi",
		CorrelationID: "cid-multi-1",
		Payload:       mustJSON(t, map[string]any{"labels": map[string]any{"a": "foo"}}),
	}

	cmds := []rule.Command{
		{Type: rule.CmdVarSetAdd, Inputs: json.RawMessage(`{"set":"s1","value":"${trigger:payload.labels.a}"}`)},
		{Type: rule.CmdVarSetRemove, Inputs: json.RawMessage(`{"set":"s2","value":"${trigger:payload.labels.a}"}`)},
	}

	err := h.processCommandsForEvent(context.Background(), ev, cmds, "ns", nil, "alerting")
	require.NoError(t, err)

	require.Len(t, ma.calls["AddElementToSet"], 1)
	require.Equal(t, "s1", ma.calls["AddElementToSet"][0]["variableKey"])
	require.Equal(t, "foo", ma.calls["AddElementToSet"][0]["value"])

	require.Len(t, ma.calls["RemoveElementFromSet"], 1)
	require.Equal(t, "s2", ma.calls["RemoveElementFromSet"][0]["variableKey"])
	require.Equal(t, "foo", ma.calls["RemoveElementFromSet"][0]["value"])
}

func TestProcessCommandsForEvent_HandlerError_BubblesUp(t *testing.T) {
	h, _, _ := newHubWithAdapter(t)

	// This command is invalid: "set" missing ⇒ execVarSetOp error.
	ev := eventing.MdaiEvent{HubName: "h", Payload: mustJSON(t, map[string]any{"labels": map[string]any{"k": "v"}})}
	cmds := []rule.Command{{Type: rule.CmdVarSetAdd, Inputs: json.RawMessage(`{"value":"${trigger:payload.labels.k}"}`)}}

	err := h.processCommandsForEvent(context.Background(), ev, cmds, "ns", nil, "alerting")
	require.Error(t, err)
	// Ensure the wrapper from processCommandsForEvent is present.
	require.ErrorContains(t, err, "command 0 (variable.set.add) failed")
}

func TestExecVarScalarOp(t *testing.T) {
	const opName = "test.scalar.op"

	mkCmd := func(inputs string) rule.Command {
		return rule.Command{Type: opName, Inputs: json.RawMessage(inputs)}
	}

	tests := []struct {
		name            string
		evPayloadJSON   string
		cmd             rule.Command
		setOpReturnErr  error
		wantErrEqual    string
		wantErrContains string
		wantSetOpCalled bool
		wantValuePassed string
	}{
		{
			name:            "DecodeError",
			evPayloadJSON:   `{"k":"v"}`,
			cmd:             mkCmd(`{"scalar":"myscalar","value":"${trigger:payload.k"`), // malformed JSON
			wantErrContains: opName + ": decode:",
			wantSetOpCalled: false,
		},
		{
			name:            "EmptyScalar",
			evPayloadJSON:   `{"k":"v"}`,
			cmd:             mkCmd(`{"scalar":"","value":"${trigger:payload.k}"}`),
			wantErrEqual:    opName + ": inputs.scalar is empty",
			wantSetOpCalled: false,
		},
		{
			name:            "EmptyValue",
			evPayloadJSON:   `{"k":"v"}`,
			cmd:             mkCmd(`{"scalar":"myscalar","value":""}`),
			wantErrEqual:    opName + ": inputs.value is empty",
			wantSetOpCalled: false,
		},
		{
			name:            "MissingFieldInEventPayload_falls_back_to_provided_value",
			evPayloadJSON:   `{"k":"v"}`,
			cmd:             mkCmd(`{"scalar":"myscalar","value":"${trigger:payload.missing}"}`),
			wantSetOpCalled: true,
			wantValuePassed: `${trigger:payload.missing}`, // interpolation preserves original when not found
		},
		{
			name:            "SetOpError",
			evPayloadJSON:   `{"k":"v"}`,
			cmd:             mkCmd(`{"scalar":"myscalar","value":"${trigger:payload.k}"}`),
			setOpReturnErr:  errors.New("kaboom"),
			wantErrContains: "kaboom",
			wantSetOpCalled: true,
			wantValuePassed: "v",
		},
		{
			name:            "Success",
			evPayloadJSON:   `{"k":"v"}`,
			cmd:             mkCmd(`{"scalar":"myscalar","value":"${trigger:payload.k}"}`),
			wantSetOpCalled: true,
			wantValuePassed: "v",
		},
	}

	h, _, _ := newHubWithAdapter(t)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
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
			case tc.wantErrEqual != "":
				require.Error(t, err)
				require.Equal(t, tc.wantErrEqual, err.Error())
			case tc.wantErrContains != "":
				require.Error(t, err)
				require.ErrorContains(t, err, tc.wantErrContains)
			default:
				require.NoError(t, err)
			}

			require.Equal(t, tc.wantSetOpCalled, setOpCalled, "setOp call expectation mismatch")
			if tc.wantSetOpCalled && tc.wantValuePassed != "" {
				require.Equal(t, tc.wantValuePassed, capturedValue)
			}
		})
	}
}

func TestInterpolate_EngineNotConfigured(t *testing.T) {
	h := &EventHub{
		Logger:              zap.NewNop(),
		InterpolationEngine: nil,
	}

	res, err := h.interpolate("template", "test.op", "value", eventing.MdaiEvent{})
	require.Error(t, err)
	require.Equal(t, "test.op: interpolate value: interpolation engine is not configured", err.Error())
	require.Empty(t, res)
}

func TestInterpolate_WhitespaceResult(t *testing.T) {
	h := &EventHub{
		Logger:              zap.NewNop(),
		InterpolationEngine: interpolation.NewEngine(zap.NewNop()),
	}

	tmpl := "   "
	res, err := h.interpolate(tmpl, "test.op", "value", eventing.MdaiEvent{})
	require.Error(t, err)
	require.Equal(t, `test.op: interpolate value produced empty (template="   ")`, err.Error())
	require.Empty(t, res)
}
