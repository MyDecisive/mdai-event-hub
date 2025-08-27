package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/decisiveai/mdai-event-hub/pkg/eventing"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
	"github.com/synadia-io/orbit.go/pcgroups"
	"go.uber.org/zap"
)

const (
	maxPCGroupMembers = 5
)

type EventPublisher struct {
	cfg    Config
	logger *zap.Logger
	conn   *nats.Conn
	js     jetstream.JetStream
}

func NewPublisher(logger *zap.Logger, clientName string) (*EventPublisher, error) {
	logger.Info("Initializing NATS publisher", zap.String("client_name", clientName))
	cfg, err := LoadConfig()
	if err != nil {
		return nil, err
	}

	cfg.Logger = logger
	cfg.ClientName = clientName

	ctx, cancel := context.WithTimeout(context.Background(), newSubscriberContextTimeout)
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

	if err := ensurePCGroup(ctx, js, cfg); err != nil {
		_ = conn.Drain()
		return nil, fmt.Errorf("ensure pcgroup: %w", err)
	}

	return &EventPublisher{cfg: cfg, logger: cfg.Logger, conn: conn, js: js}, nil
}

func (p *EventPublisher) Publish(ctx context.Context, event eventing.MdaiEvent, subject string) error {
	if event.ID == "" {
		event.ID = nuid.Next()
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}

	if subject == "" {
		return errors.New("subject is required")
	}

	fullSubject := addPrefixToSubject(p.cfg.Subject, subject)
	p.logger.Info("Publishing event to subject", zap.String("subject", fullSubject), zap.Object("event", &event))

	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	msg := &nats.Msg{
		Subject: fullSubject,
		Data:    data,
		Header: nats.Header{
			"name":        []string{event.Name}, // TODO review headers = identity, tracing, safety, and schema hints
			"source":      []string{event.Source},
			"hubName":     []string{event.HubName},
			nats.MsgIdHdr: []string{event.ID},
		},
	}
	if event.CorrelationID != "" {
		msg.Header.Set("correlationId", event.CorrelationID)
	}

	pubAck, err := p.js.PublishMsg(ctx, msg)
	p.logger.Info("Published message", zap.Reflect("pubAck", pubAck))
	return err
}

func (p *EventPublisher) Close() error {
	if p.conn != nil && !p.conn.IsClosed() {
		return p.conn.Drain()
	}
	return nil
}

func ensurePCGroup(ctx context.Context, js jetstream.JetStream, cfg Config) error {
	if err := ensureElasticGroup(ctx, js, cfg.StreamName, eventing.AlertConsumerGroupName, "events.alert.*.*", []int{1, 2}, cfg); err != nil {
		return err
	}
	return ensureElasticGroup(ctx, js, cfg.StreamName, eventing.VarsConsumerGroupName, "events.var.*.*", []int{1, 2}, cfg)
}

func ensureStream(ctx context.Context, js jetstream.JetStream, cfg Config) error {
	_, err := js.Stream(ctx, cfg.StreamName)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		cfg.Logger.Info("Creating new NATS JetStream stream", zap.String("stream_name", cfg.StreamName))
		_, err = js.CreateStream(ctx,
			jetstream.StreamConfig{
				Name: cfg.StreamName,
				// TODO create a separate stream for DLQ since it could have different retention settings
				Subjects:   []string{"events.alert.*.*", "events.alert.dlq", "events.var.*.*", "events.var.dlq"},
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

	return nil
}

func ensureElasticGroup(ctx context.Context, js jetstream.JetStream, streamName, groupName, pattern string, hashWildcards []int, cfg Config) error {
	ec, _ := pcgroups.GetElasticConsumerGroupConfig(ctx, js, streamName, groupName)
	if ec == nil {
		cfg.Logger.Info("NATS Elastic Consumer Group does not exist, creating", zap.String("group_name", groupName), zap.String("pattern", pattern))
		_, err := pcgroups.CreateElastic(
			ctx,
			js,
			streamName,
			groupName,
			maxPCGroupMembers, // works for 1-3 replicas, TODO make it configurable: partitions = replicas * 3  (rounded to something tidy, e.g. 10, 12, 16)
			pattern,
			hashWildcards,
			-1,
			-1,
		)
		if err != nil {
			cfg.Logger.Error("NATS Elastic Consumer Group creation failed", zap.Error(err))
			return err
		}
		cfg.Logger.Info("NATS Elastic Consumer Group created", zap.String("group_name", groupName), zap.String("pattern", pattern))
	}
	return nil
}

var _ eventing.Publisher = (*EventPublisher)(nil)
