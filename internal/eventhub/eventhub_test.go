package eventhub

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/decisiveai/mdai-data-core/eventing"
	"github.com/decisiveai/mdai-data-core/eventing/rule"
	"github.com/decisiveai/mdai-data-core/eventing/triggers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/valkey-io/valkey-go"
	vkmock "github.com/valkey-io/valkey-go/mock"
	"go.uber.org/zap"
)

func TestProcessAlertingEvent_NoHubName(t *testing.T) {
	h := &EventHub{Logger: zap.NewNop()}

	invoker := h.ProcessAlertingEvent(context.Background())
	err := invoker(eventing.MdaiEvent{
		HubName:   "",                // missing
		Name:      "anything.firing", // present
		Payload:   `{}`,              // present
		Timestamp: time.Now().UTC(),
	})

	require.Error(t, err)
	require.Equal(t, "missing required field: hubName", err.Error())
}

func TestProcessAlertingEvent_UnsupportedSource(t *testing.T) {
	h := &EventHub{Logger: zap.NewNop()}

	invoker := h.ProcessAlertingEvent(context.Background())
	err := invoker(eventing.MdaiEvent{
		HubName:   "test-hub",
		Name:      "x.firing",
		Payload:   `{}`, // must pass Validate()
		Source:    "not-prometheus",
		Timestamp: time.Now().UTC(),
	})

	// Current behavior: warn & skip ⇒ no error
	require.NoError(t, err)
}

func TestProcessRuleForAlertingEvent_UnsupportedCommand(t *testing.T) {
	h := &EventHub{Logger: zap.NewNop()}

	err := h.processCommandsForEvent(
		context.Background(),
		eventing.MdaiEvent{Name: "x.firing", HubName: "t7y"},
		[]rule.Command{{Type: "unknown.cmd"}},
		"ns",
		map[string]any{},
		"alerting",
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "unsupported command type")
}

func TestProcessRuleForAlertingEvent_NoCommands(t *testing.T) {
	h := &EventHub{Logger: zap.NewNop()}

	err := h.processCommandsForEvent(
		context.Background(),
		eventing.MdaiEvent{Name: "x.firing", HubName: "t7y"},
		nil,
		"ns",
		map[string]any{},
		"alerting",
	)
	require.NoError(t, err)
}

type XaddMatcher struct{}

func (XaddMatcher) Matches(x any) bool {
	if cmd, ok := x.(valkey.Completed); ok {
		commands := cmd.Commands()
		return slices.Contains(commands, "XADD") && slices.Contains(commands, "mdai_hub_event_history")
	}
	return false
}

func (XaddMatcher) String() string {
	return "Wanted XADD to mdai_hub_event_history command"
}

func TestProcessRuleForAlertingEvent_Success(t *testing.T) {
	h, ma, client := newHubWithAdapter(t)
	client.EXPECT().Do(mock.MatchedBy(func(arg any) bool {
		_, ok := arg.(context.Context)
		return ok
	}), XaddMatcher{}).Return(vkmock.Result(vkmock.ValkeyString(""))).AnyTimes()

	payload := map[string]any{
		"labels": map[string]any{
			"the_key": "the-value",
		},
	}

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
		ID:      "1",
		Name:    "any",
		HubName: "t7y",
		// Payload is not validated in ProcessVariableEvent path before the source check,
		// but including it keeps the shape consistent.
		Payload:   `{}`,
		Timestamp: time.Now().UTC(),
		Source:    "something_else",
	})

	// Current behavior: warn & skip ⇒ no error
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

	// Verify adapter call
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

func TestWithRecover_PanickingHandler(t *testing.T) {
	logger := zap.NewNop()
	panickingHandler := func(event eventing.MdaiEvent) error {
		panic("something went wrong")
	}
	event := eventing.MdaiEvent{ID: "test-event-1"}

	wrappedHandler := WithRecover(logger, panickingHandler)
	err := wrappedHandler(event)

	require.Error(t, err)
	require.Equal(t, "panic: something went wrong", err.Error())
}

func TestMatchedRules_EmptyRulesMap(t *testing.T) {
	rules := make(map[string]rule.Rule)

	result := matchedRulesByAlertCtx(eventing.MdaiEvent{Name: "any.event"}, rules)

	require.Empty(t, result)
}

func TestMatchedRules_MultipleMatches(t *testing.T) {
	rules := map[string]rule.Rule{
		"rule1": {
			Name: "rule1-match",
			Trigger: &triggers.AlertTrigger{
				Name:   "high_cpu",
				Status: "firing",
			},
		},
		"rule2": {
			Name: "rule2-no-match-status",
			Trigger: &triggers.AlertTrigger{
				Name:   "high_cpu",
				Status: "resolved",
			},
		},
		"rule3": {
			Name: "rule3-match-any-status",
			Trigger: &triggers.AlertTrigger{
				Name:   "high_cpu",
				Status: "", // should match any status
			},
		},
		"rule4": {
			Name: "rule4-no-match-name",
			Trigger: &triggers.AlertTrigger{
				Name:   "high_memory",
				Status: "firing",
			},
		},
		"rule5": {
			Name:    "rule5-non-alert-trigger",
			Trigger: nil,
		},
	}

	result := matchedRulesByAlertCtx(eventing.MdaiEvent{Name: "high_cpu.firing"}, rules)

	require.Len(t, result, 2)

	// Check that the correct rules were returned, regardless of order
	resultNames := []string{result[0].Name, result[1].Name}
	require.Contains(t, resultNames, "rule1-match")
	require.Contains(t, resultNames, "rule3-match-any-status")
}

func TestProcessEventPayload_Success(t *testing.T) {
	validJSON := `{"key1":"value1","key2":42,"nested":{"sub":"val"}}`
	event := eventing.MdaiEvent{
		ID:      "e1",
		Name:    "test",
		HubName: "hub1",
		Payload: validJSON,
	}

	result, err := processEventPayload(event)
	require.NoError(t, err, "expected no error for valid JSON payload")

	assert.Contains(t, result, "key1")
	assert.Contains(t, result, "key2")
	assert.Contains(t, result, "nested")

	assert.Equal(t, "value1", result["key1"])
	assert.InDelta(t, float64(42), result["key2"], 0.001)
	nested, ok := result["nested"].(map[string]any)
	assert.True(t, ok, "expected nested to be a map[string]any")
	assert.Equal(t, "val", nested["sub"])
}

func TestProcessEventPayload_InvalidJSON(t *testing.T) {
	invalidJSON := `{"key1":"value1", "key2":}`
	event := eventing.MdaiEvent{
		ID:      "e2",
		Name:    "test-invalid",
		HubName: "hub2",
		Payload: invalidJSON,
	}

	result, err := processEventPayload(event)
	assert.Nil(t, result, "expected result to be nil on invalid JSON")
	require.Error(t, err, "expected an error for invalid JSON payload")
	assert.Contains(t, err.Error(), "failed to unmarshal payload")
}

func TestProcessEventPayload_EmptyPayload(t *testing.T) {
	t.Parallel()

	ev := eventing.MdaiEvent{HubName: "hub", Name: "x.firing", Payload: ""}
	got, err := processEventPayload(ev)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Empty(t, got)
}

func TestMatchedRules_NoDotInEventName(t *testing.T) {
	t.Parallel()

	// When eventName has no ".", status is empty and name == whole string.
	rules := map[string]rule.Rule{
		"need-any-status": {
			Name: "any-status",
			Trigger: &triggers.AlertTrigger{
				Name:   "cpu_spike",
				Status: "", // match any status
			},
		},
		"need-firing": {
			Name: "need-firing",
			Trigger: &triggers.AlertTrigger{
				Name:   "cpu_spike",
				Status: "firing", // should NOT match (event has empty status)
			},
		},
	}

	result := matchedRulesByAlertCtx(eventing.MdaiEvent{Name: "cpu_spike"}, rules)
	require.Len(t, result, 1)
	require.Equal(t, "any-status", result[0].Name)
}

func TestGetRulesMap_SkipsUnknownTopLevelFields(t *testing.T) {
	t.Parallel()

	hubData := map[string]string{
		"r1": `{
			"name":"ok",
			"trigger":{"kind":"alert","spec":{"name":"n","status":"firing"}},
			"commands":[]
		}`,
		"r2": `{
			"name":"has-extra",
			"extra": 123,
			"trigger":{"kind":"alert","spec":{"name":"n","status":"firing"}},
			"commands":[]
		}`,
	}
	got := getRulesMap(zap.NewNop(), hubData)
	// r2 is skipped, r1 is kept.
	require.Len(t, got, 1)
	_, ok := got["r1"]
	require.True(t, ok)
	_, hasR2 := got["r2"]
	require.False(t, hasR2)
}

func TestWithRecover_PropagatesError_NoPanic(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("boom")
	handler := func(event eventing.MdaiEvent) error { return sentinel }

	wrapped := WithRecover(zap.NewNop(), handler)
	err := wrapped(eventing.MdaiEvent{ID: "e"})
	require.Error(t, err)
	require.Equal(t, sentinel, err)
}

func TestWithRecover_PanicNonStringValue(t *testing.T) {
	t.Parallel()

	handler := func(event eventing.MdaiEvent) error { panic(42) }
	wrapped := WithRecover(zap.NewNop(), handler)

	err := wrapped(eventing.MdaiEvent{ID: "e2"})
	require.Error(t, err)
	require.Equal(t, "panic: 42", err.Error())
}
