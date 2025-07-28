package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/decisiveai/mdai-event-hub/pkg/eventing"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/pcgroups"
	"go.uber.org/zap"
)

const (
	dlqSuffix                   = ".dlq"
	newSubscriberContextTimeout = 5 * time.Minute
	subscribeContextTimeout     = 5 * time.Second
)

type EventSubscriber struct {
	cfg       Config
	logger    *zap.Logger
	conn      *nats.Conn
	jetStream jetstream.JetStream
	waitGroup sync.WaitGroup
	closeCh   chan struct{}
	closeOnce sync.Once
	memberID  string
}

func NewSubscriber(logger *zap.Logger, clientName string) (*EventSubscriber, error) {
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
		return nil, fmt.Errorf("connect to NATS: %w", err)
	}

	if err := ensureStream(ctx, js, cfg); err != nil {
		_ = conn.Drain()
		return nil, fmt.Errorf("ensure stream: %w", err)
	}

	return &EventSubscriber{
		cfg:       cfg,
		logger:    cfg.Logger,
		conn:      conn,
		jetStream: js,
		closeCh:   make(chan struct{}),
		memberID:  getMemberIDs(),
	}, nil
}

func (s *EventSubscriber) Subscribe(ctx context.Context, invoker eventing.HandlerInvoker) error {
	dlqSubject := s.cfg.Subject + dlqSuffix
	handler := func(msg jetstream.Msg) {
		s.handleMessage(ctx, msg, dlqSubject, invoker)
	}

	consumerConfig := jetstream.ConsumerConfig{
		AckWait:       defaultAckWait,
		MaxAckPending: defaultMaxAckPending,
		// Durable:       s.cfg.DurableName,
		AckPolicy:         jetstream.AckExplicitPolicy,
		InactiveThreshold: s.cfg.InactiveThreshold,
	}

	ec, err := pcgroups.GetElasticConsumerGroupConfig(ctx, s.jetStream, s.cfg.StreamName, eventing.ConsumerGroupName)
	if err != nil {
		return fmt.Errorf("get Elastic Consumer Group config: %w", err)
	}

	memberID := s.memberID
	if !ec.IsInMembership(memberID) {
		members, err := pcgroups.AddMembers(
			ctx,
			s.jetStream,
			s.cfg.StreamName,
			eventing.ConsumerGroupName,
			[]string{memberID},
		)
		if err != nil {
			return err
		}
		s.logger.Info("Subscribed with member ID", zap.String("memberID", memberID), zap.Reflect("members", members))
	}

	if _, err := pcgroups.ElasticConsume(
		ctx,
		s.jetStream,
		s.cfg.StreamName,
		eventing.ConsumerGroupName,
		memberID,
		handler,
		consumerConfig,
	); err != nil {
		return fmt.Errorf("consume: %w", err)
	}
	s.logger.Info("Consumer started", zap.String("subject", s.cfg.Subject))

	go func() {
		select {
		case <-ctx.Done():
		case <-s.closeCh:
		}

		s.logger.Info("shutting down subscriber")
		s.waitGroup.Wait()
	}()

	return nil
}

func (s *EventSubscriber) Close() error {
	var err error
	s.closeOnce.Do(func() {
		members, dropErr := pcgroups.DeleteMembers(
			context.Background(),
			s.jetStream,
			s.cfg.StreamName,
			eventing.ConsumerGroupName,
			[]string{s.memberID},
		)
		if dropErr != nil {
			s.logger.Error("failed to deregister from elastic group", zap.Error(dropErr), zap.String("memberID", s.memberID), zap.Strings("members", members))
		} else {
			s.logger.Info("deregistered from elastic group", zap.String("memberID", s.memberID), zap.Strings("members", members))
		}

		close(s.closeCh)
		if s.conn != nil && !s.conn.IsClosed() {
			err = s.conn.Drain()
		}
	})
	return err
}

func (s *EventSubscriber) handleMessage(ctx context.Context, msg jetstream.Msg, dlqSubject string, invoker eventing.HandlerInvoker) {
	s.waitGroup.Add(1)
	defer s.waitGroup.Done()

	if metadata, _ := msg.Metadata(); metadata != nil {
		s.logger.Info("delivery attempt",
			zap.Uint64("consumer_seq", metadata.Sequence.Consumer),
			zap.Uint64("stream_seq", metadata.Sequence.Stream))
	}

	forwardToDLQ := func(reason string, err error) bool {
		// copy headers so we don't mutate the in-flight message
		header := nats.Header{}
		for k, vv := range msg.Headers() {
			header[k] = append([]string(nil), vv...)
		}
		header.Set("dlq_reason", reason)
		header.Set("dlq_error", err.Error())

		dlq := &nats.Msg{
			Subject: dlqSubject,
			Data:    msg.Data(),
			Header:  header,
		}

		ctxDLQ, cancel := context.WithTimeout(ctx, subscribeContextTimeout)
		defer cancel()
		if _, pubErr := s.jetStream.PublishMsg(ctxDLQ, dlq); pubErr != nil {
			s.logger.Error("publish DLQ failed", zap.Error(pubErr))
			return false
		}

		s.logger.Warn("sent message to DLQ",
			zap.String("dlq_subject", dlqSubject),
			zap.String("reason", reason),
			zap.Int("bytes", len(msg.Data())))
		return true
	}

	var event eventing.MdaiEvent
	if err := json.Unmarshal(msg.Data(), &event); err != nil {
		s.logger.Error("unmarshal", zap.Error(err))
		if forwardToDLQ("json_unmarshal_error", err) {
			_ = msg.Ack()
		} else {
			_ = msg.Nak()
		}
		return
	}

	if err := invoker(event); err != nil {
		s.logger.Error("handler", zap.Error(err))
		if forwardToDLQ("handler_error", err) {
			_ = msg.Ack()
		} else {
			_ = msg.Nak()
		}
		return
	}
	_ = msg.Ack()
}

var _ eventing.Subscriber = (*EventSubscriber)(nil)
