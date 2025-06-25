package nats

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/decisiveai/mdai-event-hub/eventing"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
	"go.uber.org/zap"
)

type EventPublisher struct {
	cfg    Config
	logger *zap.Logger
	conn   *nats.Conn
	js     nats.JetStreamContext
}

func NewPublisher(cfg Config) (*EventPublisher, error) {
	applyDefaults(&cfg)

	conn, js, err := connect(cfg)
	if err != nil {
		return nil, err
	}

	cfg.Logger.Info("Publisher connected to NATS", zap.String("nats_url", cfg.URL))

	if err := ensureStream(js, cfg); err != nil {
		_ = conn.Drain()
		return nil, fmt.Errorf("ensure stream: %w", err)
	}

	return &EventPublisher{cfg: cfg, logger: cfg.Logger, conn: conn, js: js}, nil
}

func (p *EventPublisher) Publish(event eventing.MdaiEvent) error {
	if event.Id == "" {
		event.Id = nuid.Next()
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}

	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	msg := &nats.Msg{
		Subject: subjectFromEvent(p.cfg.Subject, event),
		Data:    data,
		Header: nats.Header{
			"name":        []string{event.Name},
			"source":      []string{event.Source},
			"hubName":     []string{event.HubName},
			nats.MsgIdHdr: []string{event.Id},
		},
	}
	if event.CorrelationId != "" {
		msg.Header.Set("correlationId", event.CorrelationId)
	}

	_, err = p.js.PublishMsg(msg)
	return err
}

func (p *EventPublisher) Close() error {
	if p.conn != nil && !p.conn.IsClosed() {
		return p.conn.Drain()
	}
	return nil
}

func ensureStream(js nats.JetStreamContext, cfg Config) error {
	_, err := js.StreamInfo(cfg.StreamName)
	if errors.Is(err, nats.ErrStreamNotFound) {
		_, err = js.AddStream(&nats.StreamConfig{
			Name:       cfg.StreamName,
			Subjects:   []string{cfg.Subject + ".>"},
			Storage:    nats.FileStorage,
			Retention:  nats.WorkQueuePolicy, // assume no replay needed
			MaxMsgs:    -1,
			MaxBytes:   -1,
			Discard:    nats.DiscardNew,
			Duplicates: defaultDuplicates,
		})
	}
	return err
}
