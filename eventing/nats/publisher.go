package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/decisiveai/mdai-event-hub/eventing"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
	"github.com/synadia-io/orbit.go/pcgroups"
	"go.uber.org/zap"
)

type EventPublisher struct {
	cfg    Config
	logger *zap.Logger
	conn   *nats.Conn
	js     jetstream.JetStream
}

func NewPublisher(logger *zap.Logger, clientName string) (*EventPublisher, error) {
	cfg, err := LoadConfig()
	if err != nil {
		return nil, err
	}

	cfg.Logger = logger
	cfg.ClientName = clientName

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
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

	subject := subjectFromEvent(p.cfg.Subject, event)
	p.logger.Info("Publishing event to subject", zap.String("subject", subject), zap.Any("event", event))

	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	msg := &nats.Msg{
		Subject: subject,
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

	pubAck, err := p.js.PublishMsg(ctx, msg)
	p.logger.Info("Published message", zap.Any("pubAck", pubAck))
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
				Discard:    jetstream.DiscardOld,
				Duplicates: defaultDuplicates,
			})
	}

	if err != nil && !errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
		return err // otherwise someone else just created it
	}

	ec, _ := pcgroups.GetElasticConsumerGroupConfig(ctx, js, cfg.StreamName, eventing.ConsumerGroupName)
	if ec == nil {
		_, err = pcgroups.CreateElastic(
			ctx,
			js,
			cfg.StreamName,
			eventing.ConsumerGroupName,
			10, // works for 1-3 replicas, TODO make it configurable: partitions = replicas * 3  (rounded to something tidy, e.g. 10, 12, 16)
			"events.*.*.*",
			[]int{1, 3}, // TODO should we change to []int{1,2,3}?
			-1,
			-1,
		)
		if err != nil {
			cfg.Logger.Error("NATS Elastic Consumer Group creation failed", zap.Error(err))
			return err
		}
		cfg.Logger.Info("NATS Elastic Consumer Group created")
	}

	return nil
}

var _ eventing.Publisher = (*EventPublisher)(nil)
