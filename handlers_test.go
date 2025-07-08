package main

import (
	"github.com/decisiveai/mdai-event-hub/eventing"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

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
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, actual)
		})
	}
}
