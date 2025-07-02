package nats

import (
	"strings"
	"testing"

	"github.com/decisiveai/mdai-event-hub/eventing"
	"github.com/stretchr/testify/assert"
)

func TestSafeToken(t *testing.T) {
	cases := map[string]string{
		"":            "unknown",
		"   ":         "unknown",
		"hello.world": "hello_world",
		"a b c":       "a_b_c",
		". . .":       "_____", // FIXME: replace with a more sophisticated token sanitization
		"valid_token": "valid_token",
	}
	for input, want := range cases {
		got := safeToken(input)
		assert.Equal(t, want, got, "safeToken(%q)", input)
	}
}

func TestSubjectFromEvent(t *testing.T) {
	prefix := "prefix"
	ev := eventing.MdaiEvent{
		HubName: "hub.one",
		Source:  "source two",
		Name:    "event.three",
	}
	want := strings.Join([]string{prefix, "hub_one", "source_two", "event_three"}, ".")
	got := subjectFromEvent(prefix, ev)
	assert.Equal(t, want, got)
}

func TestFirstNonEmpty(t *testing.T) {
	assert.Equal(t, "first", firstNonEmpty("", "first", "second"))
	assert.Equal(t, "second", firstNonEmpty("", "", "second"))
	assert.Equal(t, "", firstNonEmpty("", "", ""))
}

func TestApplyDefaults(t *testing.T) {
	cfg := Config{}
	applyDefaults(&cfg)
	assert.Equal(t, defaultURL, cfg.URL)
	assert.Equal(t, defaultSubject, cfg.Subject)
	assert.Equal(t, defaultStreamName, cfg.StreamName)
	assert.Equal(t, defaultQueueName, cfg.QueueName)
	assert.Equal(t, defaultDurableName, cfg.DurableName)
	assert.Equal(t, defaultClientName, cfg.ClientName)
	assert.NotNil(t, cfg.Logger)
}
