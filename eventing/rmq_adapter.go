package eventing

import (
	"context"
	"encoding/json"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var _ EventHub = (*RmqBackend)(nil)

func NewRmqBackend(connectionString string, queueName string, logger *zap.Logger) EventHub {
	return &RmqBackend{
		connectionString: connectionString,
		queueName:        queueName,
		logger:           logger,
	}
}

// RmqBackend represents a connection to RabbitMQ
type RmqBackend struct {
	conn             *amqp.Connection
	ch               *amqp.Channel
	queueName        string
	mu               sync.Mutex
	isListening      bool
	shutdown         chan struct{}
	processingWg     sync.WaitGroup
	logger           *zap.Logger
	connCloseChan    chan *amqp.Error
	connectionString string
}

func (h *RmqBackend) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("queue_name", h.queueName)
	enc.AddBool("is_listening", h.isListening)
	return nil
}

// Connect connects to amqp service
func (h *RmqBackend) Connect() error {
	conn, err := amqp.Dial(h.connectionString)
	if err != nil {
		return fmt.Errorf("failed to dial: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		if closeErr := conn.Close(); closeErr != nil {
			h.logger.Error("Failed to close connection", zap.Error(closeErr))
		}
		return fmt.Errorf("failed to open channel: %w", err)
	}

	q, err := ch.QueueDeclare(
		h.queueName, // name
		true,        // durable - make queue persistent
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		if closeErr := ch.Close(); closeErr != nil {
			h.logger.Error("Failed to close channel", zap.Error(closeErr))
		}
		if closeErr := conn.Close(); closeErr != nil {
			h.logger.Error("Failed to close connection", zap.Error(closeErr))
		}
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	h.conn = conn
	h.ch = ch
	h.queueName = q.Name
	h.shutdown = make(chan struct{})
	h.connCloseChan = make(chan *amqp.Error, 1)

	return nil
}

// Close closes connection & waits for processing to complete
func (h *RmqBackend) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.ch != nil {
		if err := h.ch.Close(); err != nil {
			h.logger.Error("Failed to close channel", zap.Error(err))
		}
		h.ch = nil
	}
	if h.conn != nil {
		if err := h.conn.Close(); err != nil {
			h.logger.Error("Failed to close connection", zap.Error(err))
		}
		h.conn = nil
	}

	h.processingWg.Wait()
	h.logger.Info("All message processing completed")
}

// PublishMessage publishes an MdaiEvent to the queue
func (h *RmqBackend) PublishMessage(event MdaiEvent) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.ch == nil || h.conn == nil {
		return fmt.Errorf("connection is closed")
	}

	jsonData, err := json.Marshal(event)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = h.ch.PublishWithContext(ctx,
		"",          // exchange
		h.queueName, // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         jsonData,
			DeliveryMode: amqp.Persistent,
		})

	if err != nil {
		return err
	}

	h.logger.Info("Published event",
		zap.String("event", event.Name),
		zap.String("id", event.Id))
	return nil
}

// StartListening listens for messages & processes them using provided handler
func (h *RmqBackend) StartListening(invoker HandlerInvoker) error {
	h.mu.Lock()
	if h.isListening {
		h.mu.Unlock()
		return nil
	}

	if h.ch == nil || h.conn == nil {
		h.mu.Unlock()
		return fmt.Errorf("connection is closed")
	}

	h.isListening = true
	h.mu.Unlock()

	err := h.ch.Qos(
		5, // this number has to be tuned after load testing
		0,
		false,
	)
	if err != nil {
		h.mu.Lock()
		h.isListening = false
		h.mu.Unlock()
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	msgs, err := h.ch.Consume(
		h.queueName,
		"",
		false, // auto-ack - changed to false for manual acks
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		h.mu.Lock()
		h.isListening = false
		h.mu.Unlock()
		return fmt.Errorf("failed to consume: %w", err)
	}

	h.connCloseChan = h.conn.NotifyClose(make(chan *amqp.Error, 1))

	go h.runMessageLoop(msgs, invoker)

	h.logger.Info("Listening for events", zap.String("queue", h.queueName))
	return nil
}

// processDelivery handles a single message delivery
func (h *RmqBackend) processDelivery(delivery amqp.Delivery, invoker HandlerInvoker) {
	defer h.processingWg.Done()

	var event MdaiEvent
	if err := json.Unmarshal(delivery.Body, &event); err != nil {
		h.logger.Error("Failed to parse event",
			zap.Error(err),
			zap.String("body", string(delivery.Body)))

		// Acknowledge to avoid redelivery
		if err := delivery.Ack(false); err != nil {
			h.logger.Error("Failed to acknowledge message", zap.Error(err))
		}
		return
	}

	err := invoker(event)
	if err != nil {
		h.logger.Error("Failed to process event",
			zap.Error(err),
			zap.String("eventId", event.Id),
			zap.String("eventName", event.Name))

		// Reject the message but don't requeue if it's a permanent error
		// TODO: Retry logic?
		if err := delivery.Reject(false); err != nil {
			h.logger.Error("Failed to reject message", zap.Error(err))
		}
	} else {
		if err := delivery.Ack(false); err != nil {
			h.logger.Error("Failed to acknowledge message", zap.Error(err))
		}

		h.logger.Info("Successfully processed event",
			zap.String("eventId", event.Id),
			zap.String("eventName", event.Name),
			zap.String("hubName", event.HubName))
	}
}

// runMessageLoop handles the main message processing loop
func (h *RmqBackend) runMessageLoop(msgs <-chan amqp.Delivery, invoker HandlerInvoker) {
	for {
		select {
		case <-h.shutdown:
			h.logger.Info("Shutting down consumer")
			return

		case err := <-h.connCloseChan:
			if err != nil {
				h.logger.Error("Event queue connection closed", zap.Error(err))
			} else {
				h.logger.Info("Event queue connection closed gracefully")
			}
			return

		case delivery, ok := <-msgs:
			if !ok {
				h.logger.Warn("Message channel closed")
				return
			}

			h.logger.Info("Received message", zap.Int("size", len(delivery.Body)))
			h.processingWg.Add(1)

			// Process in a separate goroutine for parallel processing
			go h.processDelivery(delivery, invoker)
		}
	}
}

func (h *RmqBackend) ListenUntilSignal(invoker HandlerInvoker) error {
	err := h.StartListening(invoker)
	if err != nil {
		return err
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Block until a termination signal is received
	sig := <-sigChan
	h.logger.Info("Received signal, shutting down gracefully", zap.String("signal", sig.String()))

	close(h.shutdown)

	shutdownTimeout := 30 * time.Second
	shutdownComplete := make(chan struct{})

	go func() {
		// this will wait for processing to complete
		h.Close()
		close(shutdownComplete)
	}()

	select {
	case <-shutdownComplete:
		h.logger.Info("Graceful shutdown completed")
	case <-time.After(shutdownTimeout):
		h.logger.Warn("Graceful shutdown timed out, forcing exit", zap.Duration("timeout", shutdownTimeout))
	}

	return nil
}
