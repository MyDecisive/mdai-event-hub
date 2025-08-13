package eventing

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap/zapcore"
)

const (
	AlertConsumerGroupName      = "alert-consumer-group"
	VarsConsumerGroupName       = "vars-consumer-group"
	ManualVariablesEventSource  = "manual_variables_api"
	PrometheusAlertsEventSource = "prometheus"
)

type Publisher interface {
	Publish(ctx context.Context, event MdaiEvent, subject string) error
	Close() error
}

type Subscriber interface {
	Subscribe(ctx context.Context, groupName, dlqSubject string, invoker HandlerInvoker) error
	Close() error
}

// HandlerInvoker is a function type that processes MdaiEvents.
type HandlerInvoker func(event MdaiEvent) error

// MdaiEvent represents an event in the system.
type MdaiEvent struct {
	ID            string    `json:"id,omitempty"`
	Name          string    `json:"type"`    // e.g. "alert_firing"
	Version       int       `json:"version"` // schema version
	Timestamp     time.Time `json:"timestamp,omitempty"`
	Payload       string    `json:"payload"`
	Source        string    `json:"source"`    // used in subject, could not be empty
	SourceID      string    `json:"source_id"` // ex alert fingerprint
	CorrelationID string    `json:"correlation_id,omitempty"`
	HubName       string    `json:"hub_name"`
}

type EventPerSubject struct {
	Event   MdaiEvent
	Subject string
}

// MarshalLogObject signature requires it to return an error, but there's no way the code will generate one.
//
//nolint:unparam
func (mdaiEvent *MdaiEvent) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("type", mdaiEvent.Name)
	enc.AddString("id", mdaiEvent.ID)
	enc.AddString("source", mdaiEvent.Source)
	enc.AddString("source_id", mdaiEvent.SourceID)
	enc.AddString("hub_name", mdaiEvent.HubName)
	enc.AddString("payload", mdaiEvent.Payload)
	enc.AddTime("timestamp", mdaiEvent.Timestamp)
	enc.AddString("correlation_id", mdaiEvent.CorrelationID)
	return nil
}

func (mdaiEvent *MdaiEvent) ApplyDefaults() {
	if mdaiEvent.ID == "" {
		mdaiEvent.ID = createEventUUID()
	}
	if mdaiEvent.Timestamp.IsZero() {
		mdaiEvent.Timestamp = time.Now()
	}

	if mdaiEvent.Version == 0 {
		mdaiEvent.Version = 1
	}
}

var errMissingRequiredFields = errors.New("missing required field")

func (mdaiEvent *MdaiEvent) Validate() error {
	if mdaiEvent.Name == "" {
		return fmt.Errorf("%w: %s", errMissingRequiredFields, "name")
	}

	if mdaiEvent.HubName == "" {
		return fmt.Errorf("%w: %s", errMissingRequiredFields, "hubName")
	}

	if mdaiEvent.Payload == "" {
		return fmt.Errorf("%w: %s", errMissingRequiredFields, "payload")
	}
	return nil
}

func createEventUUID() string {
	id := uuid.New()
	return id.String()
}

// ManualVariablesActionPayload represents a payload for static variables actions.
//
//nolint:tagliatelle
type ManualVariablesActionPayload struct {
	VariableRef string `json:"variableRef"`
	DataType    string `json:"dataType"`
	Operation   string `json:"operation"`
	Data        any    `json:"data"`
}
