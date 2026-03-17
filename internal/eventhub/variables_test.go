package eventhub

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/mydecisive/mdai-data-core/eventing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type MockHandlerAdapter struct {
	mu    sync.Mutex
	calls map[string][]map[string]string // key: op+"_calls"
}

func (mh *MockHandlerAdapter) AddElementToSet(_ context.Context, variableKey, hubName, value, correlationID string, recursionDepth int) error {
	mh.append("AddElementToSet", map[string]string{
		"variableKey":    variableKey,
		"hubName":        hubName,
		"value":          value,
		"correlationID":  correlationID,
		"recursionDepth": strconv.Itoa(recursionDepth),
	})
	return nil
}

func (mh *MockHandlerAdapter) RemoveElementFromSet(_ context.Context, variableKey, hubName, value, correlationID string, recursionDepth int) error {
	mh.append("RemoveElementFromSet", map[string]string{
		"variableKey":    variableKey,
		"hubName":        hubName,
		"value":          value,
		"correlationID":  correlationID,
		"recursionDepth": strconv.Itoa(recursionDepth),
	})
	return nil
}

func (mh *MockHandlerAdapter) SetMapEntry(_ context.Context, variableKey, hubName, field, value, correlationID string, recursionDepth int) error {
	mh.append("SetMapEntry", map[string]string{
		"variableKey":    variableKey,
		"hubName":        hubName,
		"field":          field,
		"value":          value,
		"correlationID":  correlationID,
		"recursionDepth": strconv.Itoa(recursionDepth),
	})
	return nil
}

func (mh *MockHandlerAdapter) RemoveMapEntry(_ context.Context, variableKey, hubName, field, correlationID string, recursionDepth int) error {
	mh.append("RemoveMapEntry", map[string]string{
		"variableKey":    variableKey,
		"hubName":        hubName,
		"field":          field,
		"correlationID":  correlationID,
		"recursionDepth": strconv.Itoa(recursionDepth),
	})
	return nil
}

func (mh *MockHandlerAdapter) SetStringValue(_ context.Context, variableKey, hubName, value, correlationID string, recursionDepth int) error {
	mh.append("SetStringValue", map[string]string{
		"variableKey":    variableKey,
		"hubName":        hubName,
		"value":          value,
		"correlationID":  correlationID,
		"recursionDepth": strconv.Itoa(recursionDepth),
	})
	return nil
}

// --- unexported helpers after exported methods ---

func (mh *MockHandlerAdapter) append(op string, call map[string]string) {
	mh.mu.Lock()
	defer mh.mu.Unlock()
	if mh.calls == nil {
		mh.calls = make(map[string][]map[string]string)
	}
	k := op + "_calls"
	mh.calls[k] = append(mh.calls[k], call)
}

func (mh *MockHandlerAdapter) snapshot(op string) []map[string]string {
	k := op + "_calls"
	mh.mu.Lock()
	defer mh.mu.Unlock()
	out := append([]map[string]string(nil), mh.calls[k]...) // copy
	return out
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
				"variableKey":    "foobar",
				"hubName":        "barbaz",
				"value":          "bazfoo",
				"correlationID":  "bob",
				"recursionDepth": "1",
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
				"variableKey":    "foobar",
				"hubName":        "barbaz",
				"value":          "bazfoo",
				"correlationID":  "bob",
				"recursionDepth": "1",
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
			handlerName: "SetMapEntry",
			expected: map[string]string{
				"variableKey":    "foobar",
				"hubName":        "barbaz",
				"field":          "argh",
				"value":          "blargh",
				"correlationID":  "bob",
				"recursionDepth": "1",
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
			handlerName: "RemoveMapEntry",
			expected: map[string]string{
				"variableKey":    "foobar",
				"hubName":        "barbaz",
				"field":          "bazfoo",
				"correlationID":  "bob",
				"recursionDepth": "1",
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
				"variableKey":    "foobar",
				"hubName":        "barbaz",
				"value":          "bazfoo",
				"correlationID":  "bob",
				"recursionDepth": "1",
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
				"variableKey":    "foobar",
				"hubName":        "barbaz",
				"value":          "3",
				"correlationID":  "bob",
				"recursionDepth": "1",
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
				"variableKey":    "foobar",
				"hubName":        "barbaz",
				"value":          "false",
				"correlationID":  "bob",
				"recursionDepth": "1",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			t.Parallel()

			// fresh mock and hub per subtest
			mock := &MockHandlerAdapter{}
			mdai := &VarDeps{Logger: zap.NewNop(), HandlerAdapter: mock}
			h := &EventHub{
				Logger:              zap.NewNop(),
				InterpolationEngine: nil,
				VarsAdapter:         *mdai,
			}

			cmds, err := mdai.BuildCommandFromEvent(tc.event)
			require.NoError(t, err)
			require.NoError(t, h.processCommandsForEvent(t.Context(), tc.event, cmds, "mdai", nil, "vars"))

			calls := mock.snapshot(tc.handlerName)
			require.NotEmpty(t, calls)
			assert.Equal(t, tc.expected, calls[len(calls)-1])
		})
	}
}

var _ HandlerAdapter = (*MockHandlerAdapter)(nil)
