package nats

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/decisiveai/mdai-event-hub/pkg/eventing"
	"github.com/kelseyhightower/envconfig"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
	"go.uber.org/zap"
)

const (
	connectTimeout = 2 * time.Second
	reconnectWait  = 2 * time.Second
)

type Config struct {
	URL               string        `default:"nats://mdai-hub-nats.mdai.svc.cluster.local:4222" envconfig:"NATS_URL"`
	Subject           string        `default:"events"                                           envconfig:"NATS_SUBJECT"`
	StreamName        string        `default:"EVENTS_STREAM"                                    envconfig:"NATS_STREAM_NAME"`
	QueueName         string        `default:"events"                                           envconfig:"NATS_QUEUE_NAME"`
	ClientName        string        `envconfig:"-"`
	InactiveThreshold time.Duration `default:"1m"                                               envconfig:"NATS_INACTIVE_THRESHOLD"`
	NatsPassword      string        `envconfig:"NATS_PASSWORD"`
	Logger            *zap.Logger   `envconfig:"-"`
}

const (
	defaultAckWait       = 30 * time.Second
	defaultMaxAckPending = 1
	defaultDuplicates    = 2 * time.Minute
)

func LoadConfig() (Config, error) {
	var cfg Config
	if err := envconfig.Process("", &cfg); err != nil {
		return cfg, fmt.Errorf("processing envconfig: %w", err)
	}
	return cfg, nil
}

func safeToken(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "unknown"
	}
	return strings.NewReplacer(".", "_", " ", "_").Replace(s)
}

func subjectFromEvent(prefix string, event eventing.MdaiEvent) string {
	return strings.Join([]string{
		prefix,
		safeToken(event.HubName),
		safeToken(event.Source),
		safeToken(event.Name),
	}, ".")
}

func connect(ctx context.Context, cfg Config) (*nats.Conn, jetstream.JetStream, error) {
	natsOpts := []nats.Option{
		nats.UserInfo("mdai", cfg.NatsPassword),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.Timeout(connectTimeout),
		nats.ReconnectWait(reconnectWait),
		nats.Name(cfg.ClientName),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			cfg.Logger.Error("NATS disconnect", zap.Error(err))
		}),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			cfg.Logger.Error("NATS async error", zap.Error(err))
		}),
		nats.ClosedHandler(func(_ *nats.Conn) {
			cfg.Logger.Warn("NATS connection closed")
		}),
	}

	var conn *nats.Conn
	operation := func() (*nats.Conn, error) {
		return nats.Connect(cfg.URL, natsOpts...)
	}

	conn, err := backoff.Retry(ctx, operation)
	if err != nil {
		return nil, nil, err
	}

	// block here until we have completed an INFO/CONNECT/PONG round-trip
	waitForNATSConnection(ctx, conn, cfg)

	js, err := jetstream.New(conn) // implements pcgroups’ JetStream interface
	if err != nil {
		cfg.Logger.Error("NATS JetStream setup failed", zap.Error(err))
		_ = conn.Drain()
		return nil, nil, err
	}

	cfg.Logger.Info("NATS setup completed")
	return conn, js, nil
}

func waitForNATSConnection(ctx context.Context, conn *nats.Conn, cfg Config) {
	exp := backoff.NewExponentialBackOff()
	exp.InitialInterval = 250 * time.Millisecond
	exp.MaxInterval = 60 * time.Second
	exp.Multiplier = 2.0

	notify := func(err error, next time.Duration) {
		cfg.Logger.Error(
			"NATS connection not ready, backing off",
			zap.Error(err),
			zap.Duration("next_retry_in", next),
			zap.String("nats_url", cfg.URL),
		)
	}

	operation := func() (bool, error) {
		// RetryFlush returns nil as soon as FlushTimeout succeeds.
		if err := conn.FlushTimeout(250 * time.Millisecond); err != nil {
			return false, err
		}
		cfg.Logger.Info("NATS connection verified")
		return true, nil
	}

	_, err := backoff.Retry(
		ctx,
		operation,
		backoff.WithBackOff(exp),
		backoff.WithNotify(notify),
	)
	if err != nil {
		cfg.Logger.Fatal("Unable to establish NATS connection", zap.Error(err))
	}
	cfg.Logger.Info("NATS connection ready")
}

func getMemberIDs() string {
	raw := firstNonEmpty(
		os.Getenv("POD_NAME"),
		os.Getenv("HOSTNAME"),
		nuid.Next(), // fallback for local testing
	)
	// Valid priority group name must match A-Z, a-z, 0-9, -_/=)+ and may not exceed 16 characters
	clean := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '-', r == '_', r == '/', r == '=':
			return r
		default:
			return '_'
		}
	}, raw)

	const maxLen = 16
	if len(clean) > maxLen {
		clean = clean[len(clean)-maxLen:]
	}
	return clean
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
