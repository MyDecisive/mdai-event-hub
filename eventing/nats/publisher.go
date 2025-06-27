package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/decisiveai/mdai-event-hub/eventing"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
	"go.uber.org/zap"
)

type EventPublisher struct {
	cfg    Config
	logger *zap.Logger
	conn   *nats.Conn
	js     jetstream.JetStream
}

func NewPublisher(cfg Config) (*EventPublisher, error) {
	applyDefaults(&cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, js, err := connect(ctx, cfg)
	if err != nil {
		return nil, err
	}

	cfg.Logger.Info("Publisher connected to NATS", zap.String("nats_url", cfg.URL))

	if err := ensureStream(ctx, js, cfg); err != nil {
		_ = conn.Drain()
		return nil, fmt.Errorf("ensure stream: %w", err)
	}

	return &EventPublisher{cfg: cfg, logger: cfg.Logger, conn: conn, js: js}, nil
}

func (p *EventPublisher) Publish(ctx context.Context, event eventing.MdaiEvent) error {
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

	_, err = p.js.PublishMsg(ctx, msg)
	return err
}

func (p *EventPublisher) Close() error {
	if p.conn != nil && !p.conn.IsClosed() {
		return p.conn.Drain()
	}
	return nil
}

func ensureStream(ctx context.Context, js jetstream.JetStream, cfg Config) error {
	_, err := js.Stream(ctx, cfg.StreamName)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		_, err = js.CreateStream(ctx,
			jetstream.StreamConfig{
				Name:       cfg.StreamName,
				Subjects:   []string{cfg.Subject + ".>"},
				Storage:    jetstream.FileStorage,
				Retention:  jetstream.WorkQueuePolicy, // assume no replay needed
				MaxMsgs:    -1,
				MaxBytes:   -1,
				Discard:    jetstream.DiscardNew,
				Duplicates: defaultDuplicates,
			})
	}
	return err
}

func hashKey(hub, metric string, P uint32) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(hub))
	_, _ = h.Write([]byte(metric))
	return h.Sum32() % P
}
