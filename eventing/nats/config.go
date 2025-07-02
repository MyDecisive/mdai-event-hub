package nats

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/decisiveai/mdai-event-hub/eventing"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
	"go.uber.org/zap"
)

const (
	natsPasswordEnvVar = "NATS_PASS" // TODO add user and password to connect opts

	defaultSubject           = "events"
	defaultStreamName        = "EVENTS_STREAM"
	defaultQueueName         = "events"
	defaultDurableName       = "events_durable"
	defaultClientName        = "mdai-event"
	defaultURL               = "nats://nats.default.svc.cluster.local:4222"
	defaultAckWait           = 30 * time.Second
	defaultMaxAckPending     = 1
	defaultDuplicates        = 2 * time.Minute
	connectTimeout           = 2 * time.Second
	reconnectWait            = 2 * time.Second
	defaultInactiveThreshold = 1 * time.Minute
)

type Config struct {
	URL               string
	Subject           string
	StreamName        string
	QueueName         string
	DurableName       string
	ClientName        string
	InactiveThreshold time.Duration
	Logger            *zap.Logger
}

func applyDefaults(c *Config) {
	if c.URL == "" {
		c.URL = getenv("NATS_URL", defaultURL)
	}
	if c.Subject == "" {
		c.Subject = defaultSubject
	}
	if c.StreamName == "" {
		c.StreamName = defaultStreamName
	}
	if c.QueueName == "" {
		c.QueueName = defaultQueueName
	}
	if c.DurableName == "" {
		c.DurableName = defaultDurableName
	}
	if c.ClientName == "" {
		c.ClientName = defaultClientName
	}
	if c.InactiveThreshold == 0 {
		c.InactiveThreshold = defaultInactiveThreshold
	}
	if c.Logger == nil {
		l, _ := zap.NewProduction()
		c.Logger = l
	}
}

func getenv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
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
