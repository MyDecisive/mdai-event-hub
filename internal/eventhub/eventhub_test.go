package eventhub

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/decisiveai/mdai-data-core/eventing"
	"github.com/decisiveai/mdai-data-core/eventing/rule"
	"github.com/decisiveai/mdai-data-core/eventing/triggers"
	"github.com/decisiveai/mdai-event-hub/internal/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestProcessAlertingEvent_NoHubName(t *testing.T) {
	mdai := handlers.MdaiInterface{
		Logger: zap.NewNop(),
	}
	event := eventing.MdaiEvent{
		HubName: "",
	}

	handler := ProcessAlertingEvent(context.Background(), mdai)
	err := handler(event)

	require.Error(t, err)
	assert.EqualError(t, err, "no hub name provided")
}

func TestProcessAlertingEvent_UnsupportedSource(t *testing.T) {
	mdai := handlers.MdaiInterface{
		Logger: zap.NewNop(),
	}
	event := eventing.MdaiEvent{
		HubName: "test-hub",
		Source:  "not-prometheus",
	}

	handler := ProcessAlertingEvent(context.Background(), mdai)
	err := handler(event)

	require.Error(t, err)
	assert.EqualError(t, err, "unsupported Alerts event source")
}

func TestSafePerformAutomationStep_Panic(t *testing.T) {
	mdai := handlers.MdaiInterface{Logger: zap.NewNop()}
	cmd := rule.Command{Type: "webhook.call"}

	panicFn := func(_ handlers.MdaiInterface, _ eventing.MdaiEvent, _ json.RawMessage) error {
		panic("boom")
	}

	err := safePerformAutomationStep(panicFn, mdai, cmd, eventing.MdaiEvent{
		Name:          "test.firing",
		HubName:       "t7y",
		CorrelationID: "cid",
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "panic executing command")
}

func TestSafePerformAutomationStep_Error(t *testing.T) {
	mdai := handlers.MdaiInterface{Logger: zap.NewNop()}
	want := errors.New("nope")
	errFn := func(_ handlers.MdaiInterface, _ eventing.MdaiEvent, _ json.RawMessage) error { return want }

	err := safePerformAutomationStep(errFn, mdai, rule.Command{Type: "variable.set.add"}, eventing.MdaiEvent{})
	require.Error(t, err)
	require.ErrorContains(t, err, "command variable.set.add failed")
}

func TestSafePerformAutomationStep_OK(t *testing.T) {
	mdai := handlers.MdaiInterface{Logger: zap.NewNop()}
	okFn := func(_ handlers.MdaiInterface, _ eventing.MdaiEvent, _ json.RawMessage) error { return nil }

	err := safePerformAutomationStep(okFn, mdai, rule.Command{Type: "variable.set.add"}, eventing.MdaiEvent{})
	require.NoError(t, err)
}

func TestProcessRuleForAlertingEvent_UnsupportedCommand(t *testing.T) {
	mdai := handlers.MdaiInterface{Logger: zap.NewNop()}
	r := rule.Rule{
		Name:     "unsupported",
		Trigger:  &triggers.AlertTrigger{Name: "x", Status: "firing"},
		Commands: []rule.Command{{Type: "unknown.cmd"}},
	}
	err := processRuleForAlertingEvent(context.Background(), eventing.MdaiEvent{
		Name:    "x.firing",
		HubName: "t7y",
	}, mdai, r, nil /* auditAdapter */)
	require.Error(t, err)
	require.ErrorContains(t, err, "unsupported command type")
}

func TestProcessRuleForAlertingEvent_NoCommands(t *testing.T) {
	mdai := handlers.MdaiInterface{Logger: zap.NewNop()}
	r := rule.Rule{
		Name:     "empty",
		Trigger:  &triggers.AlertTrigger{Name: "x", Status: "firing"},
		Commands: nil,
	}
	err := processRuleForAlertingEvent(context.Background(), eventing.MdaiEvent{
		Name:    "x.firing",
		HubName: "t7y",
	}, mdai, r, nil)
	require.NoError(t, err)
}

func TestProcessVariableEvent_UnsupportedSource(t *testing.T) {
	mdai := handlers.MdaiInterface{Logger: zap.NewNop()}
	invoker := ProcessVariableEvent(context.Background(), mdai)

	err := invoker(eventing.MdaiEvent{
		ID:        "1",
		Name:      "any",
		HubName:   "t7y",
		Timestamp: time.Now().UTC(),
		Source:    "something_else",
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "unsupported manual variable update event source")
}

func TestGetRulesMap_BuildsValidRuleAndFallsBackName(t *testing.T) {
	logger := zap.NewNop()

	// "name" intentionally empty to verify fallback to the ConfigMap key.
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
	require.True(t, ok, "rule should be keyed by ConfigMap key")
	require.Equal(t, "ruleA", r.Name)

	_, isAlert := r.Trigger.(*triggers.AlertTrigger)
	require.True(t, isAlert, "trigger must be *triggers.AlertTrigger")

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
