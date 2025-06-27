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
	"github.com/synadia-io/orbit.go/pcgroups"
	"go.uber.org/zap"
)

const (
	natsPasswordEnvVar = "NATS_PASS" // TODO add user and password to connect opts

	defaultSubject       = "events"
	defaultStreamName    = "EVENTS_STREAM"
	defaultQueueName     = "events"
	defaultDurableName   = "events_durable"
	defaultClientName    = "mdai-event"
	defaultURL           = "nats://nats:4222"
	defaultAckWait       = 30 * time.Second
	defaultMaxAckPending = 1
	defaultDuplicates    = 2 * time.Minute
	connectTimeout       = 2 * time.Second
	reconnectWait        = 2 * time.Second
)

type Config struct {
	URL         string
	Subject     string
	StreamName  string
	QueueName   string
	DurableName string
	ClientName  string
	Logger      *zap.Logger
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
	//part := hashKey(event.HubName, event.Name /*metric*/, 16) // 16 partitions FIXME
	return strings.Join([]string{
		prefix,
		safeToken(event.HubName),
		safeToken(event.Source),
		safeToken(event.Name),
		//strconv.FormatUint(uint64(part), 10),
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

	ec, err := pcgroups.GetElasticConsumerGroupConfig(ctx, js, cfg.StreamName, "hubMetricCG")
	if err != nil {
		cfg.Logger.Error("NATS Elastic Consumer Group setup failed", zap.Error(err))
	}
	if ec == nil {
		_, err = pcgroups.CreateElastic(
			ctx,
			js,
			cfg.StreamName,
			"hubMetricCG",
			10,
			"events.*.*.*",
			[]int{1, 3},
			1024,
			1024,
		)
		if err != nil {
			cfg.Logger.Error("NATS Elastic Consumer Group creation failed", zap.Error(err))
		}
	}

	_, err = pcgroups.AddMembers(
		ctx,
		js,
		cfg.StreamName,
		"hubMetricCG",
		[]string{"m1", "m2", "m3"},
	)
	if err != nil {
		cfg.Logger.Error("NATS Elastic Consumer Group member addition failed", zap.Error(err))
	}

	cfg.Logger.Info("NATS setup completed")
	return conn, js, nil
}
