package nats

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/decisiveai/mdai-event-hub/eventing"
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
	URL               string        `envconfig:"NATS_URL" default:"nats://mdai-hub-nats.mdai.svc.cluster.local:4222"`
	Subject           string        `envconfig:"NATS_SUBJECT" default:"events"`
	StreamName        string        `envconfig:"NATS_STREAM_NAME" default:"EVENTS_STREAM"`
	QueueName         string        `envconfig:"NATS_QUEUE_NAME" default:"events"`
	ClientName        string        `envconfig:"-"`
	InactiveThreshold time.Duration `envconfig:"NATS_INACTIVE_THRESHOLD" default:"1m"`
	NatsPassword      string        `envconfig:"NATS_PASSWORD"`
	Logger            *zap.Logger   `envconfig:"-"`
}

const (
	natsPasswordEnvVar   = "NATS_PASS" // TODO add user and password to connect opts
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
	for {
		if err := conn.FlushTimeout(250 * time.Millisecond); err == nil {
			cfg.Logger.Info("NATS connection ready")
			break
		}
		cfg.Logger.Error("NATS connection not ready yet, retrying", zap.Error(err), zap.String("nats_url", cfg.URL))
		time.Sleep(5 * time.Second)
	}

	js, err := jetstream.New(conn) // implements pcgroups’ JetStream interface
	if err != nil {
		cfg.Logger.Error("NATS JetStream setup failed", zap.Error(err))
		_ = conn.Drain()
		return nil, nil, err
	}

	cfg.Logger.Info("NATS setup completed")
	return conn, js, nil
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
