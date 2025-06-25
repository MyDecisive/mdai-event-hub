package eventing

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap/zapcore"
)

const ManualVariablesEventSource = "manual_variables_api"

type Publisher interface {
	Publish(MdaiEvent) error
	Close() error
}

type Subscriber interface {
	Subscribe(context.Context, HandlerInvoker) error
	Close() error
}

// HandlerInvoker is a function type that processes MdaiEvents
type HandlerInvoker func(event MdaiEvent) error

// MdaiEvent represents an event in the system
type MdaiEvent struct {
	Id            string    `json:"id,omitempty"`
	Name          string    `json:"name"`
	Timestamp     time.Time `json:"timestamp,omitempty"`
	Payload       string    `json:"payload"`
	Source        string    `json:"source"`
	CorrelationId string    `json:"correlationId,omitempty"`
	HubName       string    `json:"hubName"`
}

func (mdaiEvent *MdaiEvent) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("name", mdaiEvent.Name)
	enc.AddString("id", mdaiEvent.Id)
	enc.AddString("source", mdaiEvent.Source)
	enc.AddString("hub_name", mdaiEvent.HubName)
	enc.AddString("payload", mdaiEvent.Payload)
	enc.AddTime("timestamp", mdaiEvent.Timestamp)
	enc.AddString("correlation_id", mdaiEvent.CorrelationId)
	return nil
}

func (mdaiEvent *MdaiEvent) ApplyDefaults() {
	if mdaiEvent.Id == "" {
		mdaiEvent.Id = createEventUuid()
	}
	if mdaiEvent.Timestamp.IsZero() {
		mdaiEvent.Timestamp = time.Now()
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

func createEventUuid() string {
	id := uuid.New()
	return id.String()
}

// ManualVariablesActionPayload represents a payload for static variables actions
type ManualVariablesActionPayload struct {
	VariableRef string `json:"variableRef"`
	DataType    string `json:"dataType"`
	Operation   string `json:"operation"`
	Data        any    `json:"data"`
}
