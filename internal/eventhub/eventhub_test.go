package eventhub

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/mydecisive/mdai-data-core/eventing"
	"github.com/mydecisive/mdai-data-core/eventing/rule"
	"github.com/mydecisive/mdai-data-core/eventing/triggers"
	"github.com/mydecisive/mdai-data-core/kube"
	"github.com/mydecisive/mdai-data-core/kube/kubetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/valkey-io/valkey-go"
	vkmock "github.com/valkey-io/valkey-go/mock"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type XaddMatcher struct{}

func (XaddMatcher) Matches(x any) bool {
	if cmd, ok := x.(valkey.Completed); ok {
		commands := cmd.Commands()
		return slices.Contains(commands, "XADD") && slices.Contains(commands, "mdai_hub_event_history")
	}
	return false
}
func (XaddMatcher) String() string { return "Wanted XADD to mdai_hub_event_history command" }

func TestProcessAlertingEvent(t *testing.T) {
	h := &EventHub{Logger: zap.NewNop()}
	invoker := h.ProcessAlertingEvent(context.Background())

	cases := []struct {
		name     string
		event    eventing.MdaiEvent
		wantErr  bool
		contains string
	}{
		{
			name: "missing hubName → error",
			event: eventing.MdaiEvent{
				HubName:   "",
				Name:      "anything.firing",
				Payload:   `{}`,
				Timestamp: time.Now().UTC(),
			},
			wantErr:  true,
			contains: "missing required field: hubName",
		},
		{
			name: "unsupported source → skip",
			event: eventing.MdaiEvent{
				HubName:   "test-hub",
				Name:      "x.firing",
				Payload:   `{}`,
				Source:    "not-prometheus",
				Timestamp: time.Now().UTC(),
			},
			wantErr: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := invoker(tc.event)
			if tc.wantErr {
				require.Error(t, err)
				if tc.contains != "" {
					require.Contains(t, err.Error(), tc.contains)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestProcessRuleForAlertingEvent_Basic(t *testing.T) {
	h := &EventHub{Logger: zap.NewNop()}

	cases := []struct {
		name    string
		cmds    []rule.Command
		wantErr bool
		match   string
	}{
		{
			name:    "unsupported command",
			cmds:    []rule.Command{{Type: "unknown.cmd"}},
			wantErr: true,
			match:   "unsupported command type",
		},
		{
			name:    "no commands is ok",
			cmds:    nil,
			wantErr: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := h.processCommandsForEvent(
				context.Background(),
				eventing.MdaiEvent{Name: "x.firing", HubName: "t7y"},
				tc.cmds,
				"ns",
				map[string]any{},
				"alerting",
			)
			if tc.wantErr {
				require.Error(t, err)
				if tc.match != "" {
					require.ErrorContains(t, err, tc.match)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestProcessRuleForAlertingEvent_Success(t *testing.T) {
	h, ma, client := newHubWithAdapter(t)
	client.EXPECT().
		Do(mock.Anything, XaddMatcher{}).
		Return(vkmock.Result(vkmock.ValkeyString(""))).
		AnyTimes()

	payload := map[string]any{"labels": map[string]any{"the_key": "the-value"}}
	event := eventing.MdaiEvent{
		Name:          "test-alert.firing",
		HubName:       "hub-x",
		CorrelationID: "cid-rule-1",
		Payload:       mustJSON(t, payload),
	}

	err := h.processCommandsForEvent(
		context.Background(),
		event,
		[]rule.Command{
			{
				Type:   rule.CmdVarSetAdd,
				Inputs: json.RawMessage(`{"set":"my-test-set","value":"${trigger:payload.labels.the_key}"}`),
			},
		},
		"ns-1",
		nil,
		"alerting",
	)
	require.NoError(t, err)

	// Verify the command handler was called correctly
	calls, ok := ma.calls["AddElementToSet"]
	require.True(t, ok, "AddElementToSet was not called")
	require.Len(t, calls, 1)

	got := calls[0]
	require.Equal(t, "my-test-set", got["variableKey"])
	require.Equal(t, "hub-x", got["hubName"])
	require.Equal(t, "the-value", got["value"])
	require.Equal(t, "cid-rule-1", got["correlationID"])
}

func TestProcessVariableEvent_UnsupportedSource(t *testing.T) {
	h := &EventHub{Logger: zap.NewNop()}

	invoker := h.ProcessVariableEvent(context.Background())
	err := invoker(eventing.MdaiEvent{
		ID:        "1",
		Name:      "any",
		HubName:   "t7y",
		Payload:   `{}`,
		Timestamp: time.Now().UTC(),
		Source:    "something_else",
	})
	require.NoError(t, err)
}

func TestProcessVariableEvent_Success(t *testing.T) {
	h, ma, _ := newHubWithAdapter(t)

	p := eventing.VariablesActionPayload{
		VariableRef: "my-set",
		DataType:    "set",
		Operation:   "add",
		Data:        []string{"new-val"},
	}
	payload, err := json.Marshal(p)
	require.NoError(t, err)

	invoker := h.ProcessVariableEvent(context.Background())
	err = invoker(eventing.MdaiEvent{
		ID:            "2",
		Name:          "variable.set.add",
		HubName:       "t7y",
		Payload:       string(payload),
		Timestamp:     time.Now().UTC(),
		Source:        eventing.ManualVariablesEventSource,
		CorrelationID: "cid-var-1",
	})
	require.NoError(t, err)

	calls, ok := ma.calls["AddElementToSet"]
	require.True(t, ok, "AddElementToSet was not called")
	require.Len(t, calls, 1)

	got := calls[0]
	require.Equal(t, "my-set", got["variableKey"])
	require.Equal(t, "t7y", got["hubName"])
	require.Equal(t, "new-val", got["value"])
	require.Equal(t, "cid-var-1", got["correlationID"])
}

func TestGetRulesMap_BuildsValidRuleAndFallsBackName(t *testing.T) {
	logger := zap.NewNop()
	hubData := map[string]string{
		"ruleA": `{
			"name": "",
			"trigger": {"kind":"alert", "spec": {"name":"anomalous_error_rate","status":"firing"}},
			"commands": [{"type":"variable.set.add","inputs":{"k":"v"}}]
		}`,
	}
	got := getRulesMap(logger, hubData)
	require.Len(t, got, 1)

	r, ok := got["ruleA"]
	require.True(t, ok)
	require.Equal(t, "ruleA", r.Name)

	_, isAlert := r.Trigger.(*triggers.AlertTrigger)
	require.True(t, isAlert)

	require.Len(t, r.Commands, 1)
	require.Equal(t, "variable.set.add", string(r.Commands[0].Type))
}

func TestGetRulesMap_SkipsInvalidEntries(t *testing.T) {
	logger := zap.NewNop()
	hubData := map[string]string{
		"badJSON":        `{"name": "oops", "trigger": 123}`,
		"badTriggerType": `{"name":"bad","trigger":{"type":"unknown","foo":"bar"},"commands":[]}`,
	}
	got := getRulesMap(logger, hubData)
	require.Empty(t, got)
}

func TestWithRecover(t *testing.T) {
	logger := zap.NewNop()

	cases := []struct {
		name    string
		handler eventing.HandlerInvoker
		want    string
	}{
		{"panic string", func(event eventing.MdaiEvent) error { panic("something went wrong") }, "panic: something went wrong"},
		{"panic non-string", func(event eventing.MdaiEvent) error { panic(42) }, "panic: 42"},
		{"return error", func(event eventing.MdaiEvent) error { return errors.New("boom") }, "boom"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wrapped := WithRecover(logger, tc.handler)
			err := wrapped(eventing.MdaiEvent{ID: "e"})
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestProcessEventPayload(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		wantErr bool
		wantLen int
	}{
		{"success", `{"key1":"value1","key2":42,"nested":{"sub":"val"}}`, false, 3},
		{"invalid json", `{"key1":"value1", "key2":}`, true, 0},
		{"empty payload", "", false, 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			event := eventing.MdaiEvent{ID: "e", Name: "n", HubName: "h", Payload: tc.payload}
			result, err := processEventPayload(event)
			if tc.wantErr {
				require.Error(t, err)
				assert.Nil(t, result)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, result)
			if tc.name == "success" {
				assert.Contains(t, result, "key1")
				assert.Contains(t, result, "key2")
				assert.Contains(t, result, "nested")
				assert.Equal(t, "value1", result["key1"])
			}
			require.Len(t, result, tc.wantLen)
		})
	}
}

func TestMatchedRulesByAlertCtx(t *testing.T) {
	h, _, _ := newHubWithAdapter(t)

	base := map[string]rule.Rule{
		"rule1": {Name: "rule1-match", Trigger: &triggers.AlertTrigger{Name: "high_cpu", Status: "firing"}},
		"rule2": {Name: "rule2-no-match-status", Trigger: &triggers.AlertTrigger{Name: "high_cpu", Status: "resolved"}},
		"rule3": {Name: "rule3-match-any-status", Trigger: &triggers.AlertTrigger{Name: "high_cpu", Status: ""}},
		"rule4": {Name: "rule4-no-match-name", Trigger: &triggers.AlertTrigger{Name: "high_memory", Status: "firing"}},
	}

	cases := []struct {
		name      string
		eventName string
		rules     map[string]rule.Rule
		wantNames []string
	}{
		{"empty rules", "any.event", map[string]rule.Rule{}, []string{}},
		{"no dot → match any-status", "high_cpu", base, []string{"rule3-match-any-status"}},
		{"multiple matches", "high_cpu.firing", base, []string{"rule1-match", "rule3-match-any-status"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := h.matchedRulesByAlertCtx(eventing.MdaiEvent{Name: tc.eventName}, tc.rules)
			var got []string
			for _, r := range result {
				got = append(got, r.Name)
			}
			if len(tc.wantNames) == 0 {
				require.Empty(t, got)
				return
			}
			for _, w := range tc.wantNames {
				require.Contains(t, got, w)
			}
		})
	}
}

func TestMatchedRulesByVariableCtx(t *testing.T) {
	h, _, _ := newHubWithAdapter(t)

	rules := map[string]rule.Rule{
		"ok":   {Name: "ok", Trigger: &triggers.VariableTrigger{Name: "my-set", UpdateType: "added"}},
		"nope": {Name: "nope", Trigger: &triggers.VariableTrigger{Name: "other", UpdateType: "added"}},
	}

	cases := []struct {
		name    string
		payload string
		wantLen int
		want    string
	}{
		{"malformed", `{"not json"`, 0, ""},
		{"match", func() string {
			b, err := json.Marshal(eventing.VariablesActionPayload{
				VariableRef: "my-set",
				Operation:   "added",
			})
			require.NoError(t, err)
			return string(b)
		}(), 1, "ok"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := h.matchedRulesByVariableCtx(eventing.MdaiEvent{
				Name:    "variable.set.add",
				Payload: tc.payload,
			}, rules)
			require.Len(t, res, tc.wantLen)
			if tc.wantLen == 1 {
				require.Equal(t, tc.want, res[0].Name)
			}
		})
	}
}

func TestProcessTriggerEvent_Success(t *testing.T) {
	h, ma, client := newHubWithAdapter(t)

	cfg, ok := h.ConfigMapController.(*kubetest.FakeConfigMapStore)
	require.True(t, ok, "expected FakeConfigMapStore")
	ruleJSON := `{
  "name": "rule-on-var-change",
  "trigger": {
    "kind": "variable",
    "spec": { "name": "my-set", "update_type": "added" }
  },
  "commands": [
    { "type": "variable.scalar.update", "inputs": { "scalar": "my-string", "value": "triggered" } }
  ]
}`

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cm-rules",
			Namespace: "ns-1",
			Labels: map[string]string{
				kube.LabelMdaiHubName:   "hub-1",
				kube.ConfigMapTypeLabel: kube.AutomationConfigMapType,
			},
		},
		Data: map[string]string{"rule1": ruleJSON},
	}
	cfg.SetHubConfigMaps("hub-1", []*corev1.ConfigMap{cm})
	require.NoError(t, cfg.Run())

	// Mock the audit event history write
	client.EXPECT().Do(context.Background(), XaddMatcher{}).Return(vkmock.Result(vkmock.ValkeyString(""))).AnyTimes()

	// Create the event that will trigger the rule
	p := eventing.VariablesActionPayload{
		VariableRef: "my-set",
		DataType:    "set",
		Operation:   "added",
		Data:        []string{"new-val"},
	}
	payload, err := json.Marshal(p)
	require.NoError(t, err)

	event := eventing.MdaiEvent{
		Name:          "variable.set.add",
		HubName:       "hub-1",
		Payload:       string(payload),
		CorrelationID: "cid-trigger-1",
		Source:        eventing.ManualVariablesEventSource,
	}

	// Invoke the handler
	invoker := h.ProcessTriggerEvent(context.Background())
	err = invoker(event)
	require.NoError(t, err)

	// Verify the command handler was called correctly
	calls, ok := ma.calls["SetStringValue"]
	require.True(t, ok, "SetStringValue was not called")
	require.Len(t, calls, 1)

	got := calls[0]
	require.Equal(t, "my-string", got["variableKey"])
	require.Equal(t, "hub-1", got["hubName"])
	require.Equal(t, "triggered", got["value"])
	require.Equal(t, "cid-trigger-1", got["correlationID"])
}

func TestProcessMdaiEvent_HopLimitReached(t *testing.T) {
	h := &EventHub{
		Logger:   zap.NewNop(),
		HopLimit: 5,
	}
	event := eventing.MdaiEvent{
		HubName:        "hub-1",
		RecursionDepth: 5, // equal to limit
	}
	err := h.processMdaiEvent(context.Background(), event, h.Logger, nil)
	require.NoError(t, err)
}

func TestProcessAlertingEvent_EndToEnd_Success(t *testing.T) {
	h, ma, client := newHubWithAdapter(t)

	// Seed rule: alert trigger -> set.add command
	ruleJSON := `{
	  "name": "r-alert",
	  "trigger": { "kind": "alert", "spec": { "name": "high_cpu", "status": "firing" } },
	  "commands": [
	    { "type": "variable.set.add", "inputs": { "set": "cpu-alerts", "value": "${trigger:payload.labels.instance}" } }
	  ]
	}`
	cfg, ok := h.ConfigMapController.(*kubetest.FakeConfigMapStore)
	require.True(t, ok, "expected FakeConfigMapStore")
	cfg.SetHubConfigMaps("hub-1", []*corev1.ConfigMap{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "cm-rules",
				Namespace: "ns-1",
				Labels: map[string]string{
					kube.LabelMdaiHubName:   "hub-1",
					kube.ConfigMapTypeLabel: kube.AutomationConfigMapType,
				},
			},
			Data: map[string]string{"r1": ruleJSON},
		},
	})
	require.NoError(t, cfg.Run())

	// Audit write mocked
	client.EXPECT().Do(context.Background(), XaddMatcher{}).Return(vkmock.Result(vkmock.ValkeyString(""))).AnyTimes()

	payload := map[string]any{"labels": map[string]any{"instance": "node-1"}}
	ev := eventing.MdaiEvent{
		Name:      "high_cpu.firing",
		HubName:   "hub-1",
		Source:    eventing.PrometheusAlertsEventSource,
		Timestamp: time.Now().UTC(),
		Payload:   mustJSON(t, payload),
	}

	err := h.ProcessAlertingEvent(context.Background())(ev)
	require.NoError(t, err)

	calls := ma.calls["AddElementToSet"]
	require.Len(t, calls, 1)
	require.Equal(t, "cpu-alerts", calls[0]["variableKey"])
	require.Equal(t, "hub-1", calls[0]["hubName"])
	require.Equal(t, "node-1", calls[0]["value"])
}

func TestProcessMdaiEvent_ConfigMapError(t *testing.T) {
	h := &EventHub{
		Logger:   zap.NewNop(),
		HopLimit: 10,
	}
	// Inject a failing store
	fs := kubetest.NewFakeConfigMapStore().FailGetByHubWith(errors.New("boom"))
	h.ConfigMapController = fs

	err := h.processMdaiEvent(context.Background(), eventing.MdaiEvent{
		Name:    "x.firing",
		HubName: "hub-x",
	}, h.Logger, h.matchedRulesByAlertCtx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "error getting ConfigMap data for hub hub-x")
}

func TestProcessMdaiEvent_BadPayload(t *testing.T) {
	h, _, client := newHubWithAdapter(t)
	cfg, ok := h.ConfigMapController.(*kubetest.FakeConfigMapStore)
	require.True(t, ok, "expected FakeConfigMapStore")

	// Rule exists, but payload is bad JSON, so processEventPayload should fail
	cfg.SeedConfigMap("hub-1", "cm-rules", kube.AutomationConfigMapType, map[string]string{
		"r1": `{
		  "name": "r",
		  "trigger": {"kind":"alert","spec":{"name":"cpu","status":"firing"}},
		  "commands": []
		}`,
	})
	require.NoError(t, cfg.Run())

	client.EXPECT().Do(mock.Anything, XaddMatcher{}).Return(vkmock.Result(vkmock.ValkeyString(""))).AnyTimes()

	ev := eventing.MdaiEvent{
		Name:    "cpu.firing",
		HubName: "hub-1",
		Source:  eventing.PrometheusAlertsEventSource,
		Payload: `{"labels":`, // malformed
	}
	err := h.ProcessAlertingEvent(context.Background())(ev)
	require.Error(t, err)
	require.Contains(t, err.Error(), "parse payload")
}

func TestProcessMdaiEvent_UnsupportedCommandFromRules(t *testing.T) {
	h, _, client := newHubWithAdapter(t)
	cfg, ok := h.ConfigMapController.(*kubetest.FakeConfigMapStore)
	require.True(t, ok, "expected FakeConfigMapStore")

	cfg.SeedConfigMap("hub-1", "cm-rules", kube.AutomationConfigMapType, map[string]string{
		"r-bad": `{
		  "name": "bad",
		  "trigger": {"kind":"alert","spec":{"name":"disk_full","status":"firing"}},
		  "commands": [{"type":"totally.unknown","inputs":{}}]
		}`,
	})
	require.NoError(t, cfg.Run())

	client.EXPECT().Do(context.Background(), XaddMatcher{}).Return(vkmock.Result(vkmock.ValkeyString(""))).AnyTimes()

	ev := eventing.MdaiEvent{
		Name:    "disk_full.firing",
		HubName: "hub-1",
		Source:  eventing.PrometheusAlertsEventSource,
		Payload: `{}`, // valid JSON
	}
	err := h.ProcessAlertingEvent(context.Background())(ev)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported command type")
}

func TestProcessVariableEvent_BuildCommandError(t *testing.T) {
	h, _, _ := newHubWithAdapter(t)
	ev := eventing.MdaiEvent{
		Name:    "variable.set.add",
		HubName: "hub-1",
		Source:  eventing.ManualVariablesEventSource,
		Payload: `{}`,
	}
	err := h.ProcessVariableEvent(context.Background())(ev)
	require.Error(t, err)
	require.Contains(t, err.Error(), "parse command type from dataType")
}

func TestProcessTriggerEvent_BadPayload(t *testing.T) {
	h, _, client := newHubWithAdapter(t)
	cfg, ok := h.ConfigMapController.(*kubetest.FakeConfigMapStore)
	require.True(t, ok, "expected FakeConfigMapStore")

	// Seed one matching variable trigger rule
	cfg.SeedConfigMap("hub-1", "cm-rules", kube.AutomationConfigMapType, map[string]string{
		"r1": `{
		  "name": "rule-on-var-change",
		  "trigger": { "kind": "variable", "spec": { "name": "my-set", "update_type": "added" } },
		  "commands": []
		}`,
	})
	require.NoError(t, cfg.Run())
	client.EXPECT().Do(context.Background(), XaddMatcher{}).Return(vkmock.Result(vkmock.ValkeyString(""))).AnyTimes()

	ev := eventing.MdaiEvent{
		Name:    "variable.set.add",
		HubName: "hub-1",
		Source:  eventing.ManualVariablesEventSource,
		Payload: `{"bad":`, // invalid JSON ⇒ processEventPayload should not fail in processMdaiEvent
	}
	err := h.ProcessTriggerEvent(context.Background())(ev)
	require.NoError(t, err)
}
