package eventing

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	wmnats "github.com/ThreeDotsLabs/watermill-nats/v2/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/garsue/watermillzap"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

const (
	natsUrlEnvVarKey           = "NATS_URL"
	natsPasswordEnvVar         = "NATS_PASS"
	ManualVariablesEventSource = "manual_variables_api"
)

type Config struct {
	URL        string
	Subject    string
	StreamName string
	Logger     *zap.Logger
}

func defaults() Config {
	l, _ := zap.NewProduction()
	return Config{
		URL:        getEnvVariableWithDefault(natsUrlEnvVarKey, "nats://nats:4222"),
		Subject:    "events",
		StreamName: "EVENTS_STREAM",
		Logger:     l,
	}
}

// EventBus is a minimal wrapper around Watermill’s NATS publisher/subscriber.
// Close() *must* be called to flush/close network resources.
type EventBus struct {
	cfg        Config
	logger     *zap.Logger
	wmLogger   watermill.LoggerAdapter
	publisher  *wmnats.Publisher
	subscriber *wmnats.Subscriber
}

func New(cfg Config) (*EventBus, error) {
	d := defaults()
	if cfg.URL == "" {
		cfg.URL = d.URL
	}
	if cfg.Subject == "" {
		cfg.Subject = d.Subject
	}
	if cfg.StreamName == "" {
		cfg.StreamName = d.StreamName
	}
	if cfg.Logger == nil {
		cfg.Logger = d.Logger
	}

	wmLogger := watermillzap.NewLogger(cfg.Logger)

	natsOpts := []nats.Option{
		//nats.UserInfo("mdai", os.Getenv(natsPasswordEnvVar)),
		nats.RetryOnFailedConnect(true),
		nats.Timeout(30 * time.Second),
		nats.ReconnectWait(1 * time.Second),
	}

	jsCfg := wmnats.JetStreamConfig{
		Disabled:      false,
		AutoProvision: true, // create stream if missing
		SubscribeOptions: []nats.SubOpt{
			nats.DeliverAll(),
			nats.AckExplicit(),
			nats.MaxAckPending(1), // preserve order, will slow the performance
		},
		PublishOptions: nil,
		TrackMsgId:     false,
		AckAsync:       false,
		DurablePrefix:  "",
	}

	// ---- Publisher ----
	pub, err := wmnats.NewPublisher(wmnats.PublisherConfig{
		URL:         cfg.URL,
		NatsOptions: natsOpts,
		Marshaler:   &wmnats.GobMarshaler{},
		JetStream:   jsCfg,
	}, wmLogger)
	if err != nil {
		return nil, err
	}

	// ---- Subscriber ----
	sub, err := wmnats.NewSubscriber(wmnats.SubscriberConfig{
		URL:              cfg.URL,
		QueueGroupPrefix: "events",
		CloseTimeout:     30 * time.Second,
		AckWaitTimeout:   30 * time.Second,
		NatsOptions:      natsOpts,
		Unmarshaler:      &wmnats.GobMarshaler{},
		JetStream:        jsCfg,
	}, wmLogger)
	if err != nil {
		_ = pub.Close()
		return nil, err
	}

	return &EventBus{
		cfg:        cfg,
		logger:     cfg.Logger,
		wmLogger:   wmLogger,
		publisher:  pub,
		subscriber: sub,
	}, nil
}

// Publish sends a message with optional metadata.
func (b *EventBus) Publish(event MdaiEvent) error {
	if event.Id == "" {
		b.logger.Warn("event ID not provided, generating new UUID")
		event.Id = watermill.NewUUID()
	}
	if event.Timestamp.IsZero() {
		b.logger.Warn("event timestamp not provided, setting current time")
		event.Timestamp = time.Now().UTC()
	}

	payload, err := json.Marshal(event)
	if err != nil {
		b.logger.Error("failed to marshal MdaiEvent", zap.Error(err))
		return err
	}
	msg := message.NewMessage(event.Id, payload)
	msg.Metadata.Set("name", event.Name)
	msg.Metadata.Set("source", event.Source)
	msg.Metadata.Set("hubName", event.HubName)
	if event.CorrelationId != "" {
		msg.Metadata.Set("correlationId", event.CorrelationId)
	}
	return b.publisher.Publish(b.cfg.Subject, msg)
}

// Subscribe starts a goroutine that passes each message to handler and handles ack/nack.
func (b *EventBus) Subscribe(ctx context.Context, invoker HandlerInvoker) error {
	ch, err := b.subscriber.Subscribe(ctx, b.cfg.Subject)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				b.logger.Info("event bus context cancelled")
				return
			case m, ok := <-ch:
				if !ok {
					b.logger.Warn("event bus channel closed")
					return
				}

				meta := m.Metadata
				b.logger.Info("received event", zap.Any("Metadata", meta), zap.String("Payload", string(m.Payload)))

				consumerSeqStr := m.Metadata.Get(jetstream.SequenceHeader)
				if consumerSeqStr != "" { // FIXME why this is not working?
					if cseq, _ := strconv.ParseUint(consumerSeqStr, 10, 64); cseq > 0 {
						b.logger.Debug("delivery attempt", zap.Uint64("consumer_seq", cseq))
					}
				}

				var event MdaiEvent
				if err := json.Unmarshal(m.Payload, &event); err != nil {
					b.logger.Error("failed to unmarshal MdaiEvent", zap.Error(err))
					m.Nack()
					continue
				}

				if err := invoker(event); err != nil {
					b.logger.Error("handler error", zap.Error(err))
					m.Nack()
				} else {
					m.Ack()
				}
			}
		}
	}()
	return nil
}

// Close flushes and releases underlying resources.
func (b *EventBus) Close() error {
	var errPub, errSub error
	if b.publisher != nil {
		errPub = b.publisher.Close()
	}
	if b.subscriber != nil {
		errSub = b.subscriber.Close()
	}
	if errPub != nil {
		return errPub
	}
	return errSub
}

func getEnvVariableWithDefault(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
