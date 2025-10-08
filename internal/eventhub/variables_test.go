package eventhub

import (
	"context"
	"testing"

	"github.com/decisiveai/mdai-data-core/eventing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type MockHandlerAdapter struct {
	Calls map[string][]map[string]string
}

func (mh *MockHandlerAdapter) AddElementToSet(_ context.Context, variableKey, hubName, value, correlationID string) error {
	mh.Calls["AddElementToSet"] = append(mh.Calls["AddElementToSet"], map[string]string{
		"variableKey":   variableKey,
		"hubName":       hubName,
		"value":         value,
		"correlationID": correlationID,
	})
	return nil
}

func (mh *MockHandlerAdapter) RemoveElementFromSet(_ context.Context, variableKey, hubName, value, correlationID string) error {
	mh.Calls["RemoveElementFromSet"] = append(mh.Calls["RemoveElementFromSet"], map[string]string{
		"variableKey":   variableKey,
		"hubName":       hubName,
		"value":         value,
		"correlationID": correlationID,
	})
	return nil
}

func (mh *MockHandlerAdapter) SetMapEntry(_ context.Context, variableKey, hubName, field, value, correlationID string) error {
	mh.Calls["AddSetMapElement"] = append(mh.Calls["AddSetMapElement"], map[string]string{
		"variableKey":   variableKey,
		"hubName":       hubName,
		"field":         field,
		"value":         value,
		"correlationID": correlationID,
	})
	return nil
}

func (mh *MockHandlerAdapter) RemoveMapEntry(_ context.Context, variableKey, hubName, field, correlationID string) error {
	mh.Calls["RemoveElementFromMap"] = append(mh.Calls["RemoveElementFromMap"], map[string]string{
		"variableKey":   variableKey,
		"hubName":       hubName,
		"field":         field,
		"correlationID": correlationID,
	})
	return nil
}

func (mh *MockHandlerAdapter) SetStringValue(_ context.Context, variableKey, hubName, value, correlationID string) error {
	mh.Calls["SetStringValue"] = append(mh.Calls["SetStringValue"], map[string]string{
		"variableKey":   variableKey,
		"hubName":       hubName,
		"value":         value,
		"correlationID": correlationID,
	})
	return nil
}

func TestHandleManualVariablesActions(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		description string
		event       eventing.MdaiEvent
		handlerName string
		expected    map[string]string
	}{
		{
			description: "add set member operation",
			event: eventing.MdaiEvent{
				ID:            "testId",
				Name:          "testName",
				Payload:       `{"dataType":"set","operation":"add","variableRef":"foobar","data":["bazfoo"]}`,
				Source:        "testSource",
				SourceID:      "testSourceId",
				CorrelationID: "bob",
				HubName:       "barbaz",
			},
			handlerName: "AddElementToSet",
			expected: map[string]string{
				"variableKey":   "foobar",
				"hubName":       "barbaz",
				"value":         "bazfoo",
				"correlationID": "bob",
			},
		},
		{
			description: "remove set member operation",
			event: eventing.MdaiEvent{
				ID:            "testId",
				Name:          "testName",
				Payload:       `{"dataType":"set","operation":"remove","variableRef":"foobar","data":["bazfoo"]}`,
				Source:        "testSource",
				SourceID:      "testSourceId",
				CorrelationID: "bob",
				HubName:       "barbaz",
			},
			handlerName: "RemoveElementFromSet",
			expected: map[string]string{
				"variableKey":   "foobar",
				"hubName":       "barbaz",
				"value":         "bazfoo",
				"correlationID": "bob",
			},
		},
		{
			description: "add map member operation",
			event: eventing.MdaiEvent{
				ID:            "testId",
				Name:          "testName",
				Payload:       `{"dataType":"map","operation":"add","variableRef":"foobar","data":{"argh":"blargh"}}`,
				Source:        "testSource",
				SourceID:      "testSourceId",
				CorrelationID: "bob",
				HubName:       "barbaz",
			},
			handlerName: "AddSetMapElement",
			expected: map[string]string{
				"variableKey":   "foobar",
				"hubName":       "barbaz",
				"field":         "argh",
				"value":         "blargh",
				"correlationID": "bob",
			},
		},
		{
			description: "remove map member operation",
			event: eventing.MdaiEvent{
				ID:            "testId",
				Name:          "testName",
				Payload:       `{"dataType":"map","operation":"remove","variableRef":"foobar","data":["bazfoo"]}`,
				Source:        "testSource",
				SourceID:      "testSourceId",
				CorrelationID: "bob",
				HubName:       "barbaz",
			},
			handlerName: "RemoveElementFromMap",
			expected: map[string]string{
				"variableKey":   "foobar",
				"hubName":       "barbaz",
				"field":         "bazfoo",
				"correlationID": "bob",
			},
		},
		{
			description: "set string operation",
			event: eventing.MdaiEvent{
				ID:            "testId",
				Name:          "testName",
				Payload:       `{"dataType":"string","variableRef":"foobar","data":"bazfoo"}`,
				Source:        "testSource",
				SourceID:      "testSourceId",
				CorrelationID: "bob",
				HubName:       "barbaz",
			},
			handlerName: "SetStringValue",
			expected: map[string]string{
				"variableKey":   "foobar",
				"hubName":       "barbaz",
				"value":         "bazfoo",
				"correlationID": "bob",
			},
		},
		{
			description: "set int operation",
			event: eventing.MdaiEvent{
				ID:            "testId",
				Name:          "testName",
				Payload:       `{"dataType":"int","variableRef":"foobar","data":"3"}`,
				Source:        "testSource",
				SourceID:      "testSourceId",
				CorrelationID: "bob",
				HubName:       "barbaz",
			},
			handlerName: "SetStringValue",
			expected: map[string]string{
				"variableKey":   "foobar",
				"hubName":       "barbaz",
				"value":         "3",
				"correlationID": "bob",
			},
		},
		{
			description: "set bool operation",
			event: eventing.MdaiEvent{
				ID:            "testId",
				Name:          "testName",
				Payload:       `{"dataType":"boolean","variableRef":"foobar","data":"false"}`,
				Source:        "testSource",
				SourceID:      "testSourceId",
				CorrelationID: "bob",
				HubName:       "barbaz",
			},
			handlerName: "SetStringValue",
			expected: map[string]string{
				"variableKey":   "foobar",
				"hubName":       "barbaz",
				"value":         "false",
				"correlationID": "bob",
			},
		},
	}

	mockHandlerAdapter := &MockHandlerAdapter{Calls: make(map[string][]map[string]string)}
	mdai := &VarDeps{Logger: zap.NewNop(), HandlerAdapter: mockHandlerAdapter}
	h := &EventHub{
		Logger:              zap.NewNop(),
		InterpolationEngine: nil,
		VarsAdapter:         *mdai,
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			t.Parallel()

			cmds, err := mdai.BuildCommandFromEvent(tc.event)
			require.NoError(t, err)
			require.NoError(t, h.processCommandsForEvent(t.Context(), tc.event, cmds, "mdai", nil, "vars"))
			assert.Contains(t, mockHandlerAdapter.Calls[tc.handlerName], tc.expected)
		})
	}
}

var _ HandlerAdapter = (*MockHandlerAdapter)(nil)
