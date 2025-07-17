package eventing

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestApplyDefaults(t *testing.T) {
	event := &MdaiEvent{}
	event.ApplyDefaults()

	assert.NotEmpty(t, event.Id, "expected Id to be set")
	assert.False(t, event.Timestamp.IsZero(), "expected Timestamp to be set")
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		event   MdaiEvent
		wantErr bool
	}{
		{"missing name", MdaiEvent{HubName: "hub", Payload: "data"}, true},
		{"missing hub", MdaiEvent{Name: "test", Payload: "data"}, true},
		{"missing payload", MdaiEvent{Name: "test", HubName: "hub"}, true},
		{"valid event", MdaiEvent{Name: "test", HubName: "hub", Payload: "data"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.event.Validate()
			if tt.wantErr {
				assert.Error(t, err, "expected validation error")
			} else {
				assert.NoError(t, err, "expected no validation error")
			}
		})
	}
}
