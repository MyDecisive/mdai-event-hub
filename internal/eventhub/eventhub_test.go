package eventhub

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/decisiveai/mdai-data-core/eventing"
	"github.com/decisiveai/mdai-data-core/eventing/rule"
	"github.com/decisiveai/mdai-data-core/eventing/triggers"
	"github.com/stretchr/testify/require"
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

	r := rule.Rule{
		Name:     "unsupported",
		Trigger:  &triggers.AlertTrigger{Name: "x", Status: "firing"},
		Commands: []rule.Command{{Type: "unknown.cmd"}},
	}

	err := h.processRuleForAlertingEvent(
		context.Background(),
		eventing.MdaiEvent{Name: "x.firing", HubName: "t7y"},
		r,
		"ns",
		map[string]any{},
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "unsupported command type")
}

func TestProcessRuleForAlertingEvent_NoCommands(t *testing.T) {
	h := &EventHub{Logger: zap.NewNop()}

	r := rule.Rule{
		Name:     "empty",
		Trigger:  &triggers.AlertTrigger{Name: "x", Status: "firing"},
		Commands: nil,
	}

	err := h.processRuleForAlertingEvent(
		context.Background(),
		eventing.MdaiEvent{Name: "x.firing", HubName: "t7y"},
		r,
		"ns",
		map[string]any{},
	)
	require.NoError(t, err)
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
	h, ma := newHubWithAdapter(t)

	p := eventing.ManualVariablesActionPayload{
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
	require.Equal(t, "variable.set.add", r.Commands[0].Type)
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
