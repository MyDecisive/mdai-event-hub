package handlers

import (
	"context"
	"testing"
	"time"

	"github.com/decisiveai/mdai-event-hub/pkg/eventing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type MockHandlerAdapter struct {
	Calls map[string][]map[string]string
}

func (mh *MockHandlerAdapter) AddElementToSet(ctx context.Context, variableKey string, hubName string, value string, correlationID string) error {
	mh.Calls["AddElementToSet"] = append(mh.Calls["AddElementToSet"], map[string]string{
		"variableKey":   variableKey,
		"hubName":       hubName,
		"value":         value,
		"correlationID": correlationID,
	})
	return nil
}

func (mh *MockHandlerAdapter) RemoveElementFromSet(ctx context.Context, variableKey string, hubName string, value string, correlationID string) error {
	mh.Calls["RemoveElementFromSet"] = append(mh.Calls["RemoveElementFromSet"], map[string]string{
		"variableKey":   variableKey,
		"hubName":       hubName,
		"value":         value,
		"correlationID": correlationID,
	})
	return nil
}

func (mh *MockHandlerAdapter) AddSetMapElement(ctx context.Context, variableKey string, hubName string, field string, value string, correlationID string) error {
	mh.Calls["AddSetMapElement"] = append(mh.Calls["AddSetMapElement"], map[string]string{
		"variableKey":   variableKey,
		"hubName":       hubName,
		"field":         field,
		"value":         value,
		"correlationID": correlationID,
	})
	return nil
}

func (mh *MockHandlerAdapter) RemoveElementFromMap(ctx context.Context, variableKey string, hubName string, field string, correlationID string) error {
	mh.Calls["RemoveElementFromMap"] = append(mh.Calls["RemoveElementFromMap"], map[string]string{
		"variableKey":   variableKey,
		"hubName":       hubName,
		"field":         field,
		"correlationID": correlationID,
	})
	return nil
}

func (mh *MockHandlerAdapter) SetStringValue(ctx context.Context, variableKey string, hubName string, value string, correlationID string) error {
	mh.Calls["SetStringValue"] = append(mh.Calls["SetStringValue"], map[string]string{
		"variableKey":   variableKey,
		"hubName":       hubName,
		"value":         value,
		"correlationID": correlationID,
	})
	return nil
}

func TestHandleAddNoisyServiceToSet(t *testing.T) {
	mockHandlerAdapter := &MockHandlerAdapter{
		Calls: make(map[string][]map[string]string),
	}
	mdaiInterface := MdaiInterface{
		Logger: zap.NewNop(),
		Data:   mockHandlerAdapter,
	}
	event := eventing.MdaiEvent{
		ID:            "testId",
		Name:          "testName",
		Payload:       `{"key":"bazfoo"}`,
		Source:        "testSource",
		SourceID:      "testSourceId",
		CorrelationID: "bob",
		HubName:       "barbaz",
	}
	args := map[string]string{
		"variable_ref":    "foobar",
		"payload_val_ref": "key",
	}
	require.NoError(t, handleAddNoisyServiceToSet(mdaiInterface, event, args))
	assert.Contains(t, mockHandlerAdapter.Calls["AddElementToSet"], map[string]string{
		"variableKey":   "foobar",
		"hubName":       "barbaz",
		"value":         "bazfoo",
		"correlationID": "bob",
	})
}

func TestHandleRemoveNoisyServiceFromSet(t *testing.T) {
	mockHandlerAdapter := &MockHandlerAdapter{
		Calls: make(map[string][]map[string]string),
	}
	mdaiInterface := MdaiInterface{
		Logger: zap.NewNop(),
		Data:   mockHandlerAdapter,
	}
	event := eventing.MdaiEvent{
		ID:            "testId",
		Name:          "testName",
		Payload:       `{"key":"bazfoo"}`,
		Source:        "testSource",
		SourceID:      "testSourceId",
		CorrelationID: "bob",
		HubName:       "barbaz",
	}
	args := map[string]string{
		"variable_ref":    "foobar",
		"payload_val_ref": "key",
	}
	require.NoError(t, handleRemoveNoisyServiceFromSet(mdaiInterface, event, args))
	assert.Contains(t, mockHandlerAdapter.Calls["RemoveElementFromSet"], map[string]string{
		"variableKey":   "foobar",
		"hubName":       "barbaz",
		"value":         "bazfoo",
		"correlationID": "bob",
	})
}

func TestHandleManualVariablesActions(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		description string
		event       eventing.MdaiEvent
		args        map[string]string
		handlerName string
		expected    map[string]string
	}{
		{
			description: "add set member operation",
			event: eventing.MdaiEvent{
				ID:            "testId",
				Name:          "testName",
				Payload:       `{"dataType":"set","operation":"add","variableRef":"foobar", "data": ["bazfoo"]}`,
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
				Payload:       `{"dataType":"set","operation":"remove","variableRef":"foobar", "data": ["bazfoo"]}`,
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
				Payload:       `{"dataType":"map","operation":"add","variableRef":"foobar", "data": {"argh": "blargh"}}`,
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
				Payload:       `{"dataType":"map","operation":"remove","variableRef":"foobar", "data": ["bazfoo"]}`,
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
				Payload:       `{"dataType":"string","variableRef":"foobar", "data": "bazfoo"}`,
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
				Payload:       `{"dataType":"int","variableRef":"foobar", "data": "3"}`,
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
				Payload:       `{"dataType":"boolean","variableRef":"foobar", "data": "false"}`,
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

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			t.Parallel()
			mockHandlerAdapter := &MockHandlerAdapter{
				Calls: make(map[string][]map[string]string),
			}
			mdaiInterface := MdaiInterface{
				Logger: zap.NewNop(),
				Data:   mockHandlerAdapter,
			}
			require.NoError(t, HandleManualVariablesActions(t.Context(), mdaiInterface, tc.event))
			assert.Contains(t, mockHandlerAdapter.Calls[tc.handlerName], tc.expected)
		})
	}
}

func TestBuildSlackPayload(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		desc     string
		args     map[string]string
		event    eventing.MdaiEvent
		payload  map[string]any
		expected SlackPayload
	}{
		{
			desc: "build minimal slack payload",
			args: map[string]string{},
			event: eventing.MdaiEvent{
				HubName:   "foobar",
				Name:      "barbaz",
				Timestamp: time.Date(2021, time.September, 21, 9, 21, 9, 21, time.UTC),
			},
			payload: map[string]any{},
			expected: SlackPayload{
				Text: "MDAI Hub Event - foobar - barbaz",
				Blocks: []map[string]any{
					{
						"type": "section",
						"text": map[string]string{
							"type": "mrkdwn",
							"text": "*MDAI Hub Event - foobar - barbaz*",
						},
					},
					{
						"type": "section",
						"fields": []map[string]string{
							{
								"type": "mrkdwn",
								"text": "*Alert timestamp* - 2021-09-21 09:21:09.000000021 +0000 UTC",
							},
						},
					},
				},
			},
		}, {
			desc: "build more complex slack payload",
			args: map[string]string{
				"message":                   "SLACKY MCSLACKFACE LOL",
				"payload_val_ref_primary":   "lol",
				"payload_val_ref_secondary": "lmao",
				"payload_val_ref_tertiary":  "even",
				"link_text":                 "CLICK HERE FOR FREE IPAD!",
				"link_url":                  "https://www.example.com",
			},
			event: eventing.MdaiEvent{
				HubName:   "foobaz",
				Name:      "barbar",
				Timestamp: time.Date(2021, time.September, 21, 9, 21, 9, 21, time.UTC),
			},
			payload: map[string]any{
				"lol":  "wut",
				"lmao": "k.",
				"even": "whoa",
			},
			expected: SlackPayload{
				Text: "SLACKY MCSLACKFACE LOL",
				Blocks: []map[string]any{
					{
						"type": "section",
						"text": map[string]string{
							"type": "mrkdwn",
							"text": "*SLACKY MCSLACKFACE LOL*",
						},
					},
					{
						"type": "section",
						"fields": []map[string]string{
							{
								"type": "mrkdwn",
								"text": "*Alert timestamp* - 2021-09-21 09:21:09.000000021 +0000 UTC",
							},
							{
								"type": "mrkdwn",
								"text": "*lol* - wut",
							},
							{
								"type": "mrkdwn",
								"text": "*lmao* - k.",
							},
							{
								"type": "mrkdwn",
								"text": "*even* - whoa",
							},
						},
					},
					{
						"type": "actions",
						"elements": []map[string]any{
							{
								"type": "button",
								"text": map[string]string{
									"type": "plain_text",
									"text": "CLICK HERE FOR FREE IPAD!",
								},
								"style": "primary",
								"url":   "https://www.example.com",
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()
			actual, err := buildSlackPayload(tc.args, tc.event, tc.payload)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, actual)
		})
	}
}
