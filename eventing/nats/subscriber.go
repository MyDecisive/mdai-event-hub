package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/decisiveai/mdai-event-hub/eventing"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

const dlqSuffix = ".dlq"

type EventSubscriber struct {
	cfg           Config
	logger        *zap.Logger
	conn          *nats.Conn
	streamContext nats.JetStreamContext
	subscription  *nats.Subscription
	waitGroup     sync.WaitGroup
	closeCh       chan struct{}
	closeOnce     sync.Once
}

func NewSubscriber(cfg Config) (*EventSubscriber, error) {
	applyDefaults(&cfg)

	conn, js, err := connect(cfg)
	if err != nil {
		return nil, err
	}

	// Fail fast if stream is absent.
	if _, err := js.StreamInfo(cfg.StreamName); err != nil {
		_ = conn.Drain()
		return nil, fmt.Errorf("stream %q not found: %w", cfg.StreamName, err)
	}

	return &EventSubscriber{
		cfg:           cfg,
		logger:        cfg.Logger,
		conn:          conn,
		streamContext: js,
		closeCh:       make(chan struct{}),
	}, nil
}

func (s *EventSubscriber) Subscribe(ctx context.Context, invoker eventing.HandlerInvoker) error {
	pattern := s.cfg.Subject + ".>"
	dlqSubject := s.cfg.Subject + dlqSuffix

	subscriber, err := s.streamContext.QueueSubscribe(
		pattern,
		s.cfg.QueueName,
		func(msg *nats.Msg) {
			s.waitGroup.Add(1)
			defer s.waitGroup.Done()

			metadata, _ := msg.Metadata() // temporary to debug order
			if metadata != nil {
				s.logger.Debug("delivery attempt",
					zap.Uint64("consumer_seq", metadata.Sequence.Consumer),
					zap.Uint64("stream_seq", metadata.Sequence.Stream))
			}

			forwardToDLQ := func(reason string, err error) bool {
				// copy headers so we don’t mutate the in-flight message
				header := nats.Header{}
				for k, vv := range msg.Header {
					header[k] = append([]string(nil), vv...)
				}
				header.Set("dlq_reason", reason)
				header.Set("dlq_error", err.Error())

				dlq := &nats.Msg{
					Subject: dlqSubject,
					Data:    msg.Data,
					Header:  header,
				}

				if _, pubErr := s.streamContext.PublishMsg(dlq); pubErr != nil {
					s.logger.Error("publish DLQ failed", zap.Error(pubErr))
					_ = msg.Nak() // retry the original later
					return false
				}

				s.logger.Warn("sent message to DLQ",
					zap.String("dlq_subject", dlqSubject),
					zap.String("reason", reason),
					zap.Int("bytes", len(msg.Data)))
				_ = msg.Ack()
				return true
			}

			var event eventing.MdaiEvent
			if err := json.Unmarshal(msg.Data, &event); err != nil {
				s.logger.Error("unmarshal", zap.Error(err))
				forwardToDLQ("json_unmarshal_error", err)
				return
			}

			if err := invoker(event); err != nil {
				s.logger.Error("handler", zap.Error(err))
				forwardToDLQ("handler_error", err)
				return
			}
			_ = msg.Ack()
		},
		nats.ManualAck(),
		nats.Durable(s.cfg.DurableName),
		nats.AckWait(defaultAckWait),
		nats.MaxAckPending(defaultMaxAckPending),
	)
	if err != nil {
		return err
	}
	s.subscription = subscriber

	// Shutdown listener
	go func() {
		select {
		case <-ctx.Done():
		case <-s.closeCh:
		}
		_ = s.subscription.Drain()
		s.waitGroup.Wait()
	}()

	return nil
}

func (s *EventSubscriber) Close() error {
	var err error
	s.closeOnce.Do(func() {
		close(s.closeCh)
		if s.conn != nil && !s.conn.IsClosed() {
			err = s.conn.Drain()
		}
	})
	return err
}
