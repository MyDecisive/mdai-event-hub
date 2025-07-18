package nats

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/decisiveai/mdai-event-hub/pkg/eventing"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestSafeToken(t *testing.T) {
	cases := map[string]string{
		"":            "unknown",
		"   ":         "unknown",
		"hello.world": "hello_world",
		"a b c":       "a_b_c",
		". . .":       "_____", // FIXME: replace with a more sophisticated token sanitization
		"valid_token": "valid_token",
	}
	for input, want := range cases {
		got := safeToken(input)
		assert.Equal(t, want, got, "safeToken(%q)", input)
	}
}

func TestSubjectFromEvent(t *testing.T) {
	prefix := "prefix"
	ev := eventing.MdaiEvent{
		HubName: "hub.one",
		Source:  "source two",
		Name:    "event.three",
	}
	want := strings.Join([]string{prefix, "hub_one", "source_two", "event_three"}, ".")
	got := subjectFromEvent(prefix, ev)
	assert.Equal(t, want, got)
}

func TestFirstNonEmpty(t *testing.T) {
	assert.Equal(t, "first", firstNonEmpty("", "first", "second"))
	assert.Equal(t, "second", firstNonEmpty("", "", "second"))
	assert.Empty(t, firstNonEmpty("", "", ""))
}

// Delay server startup to force initial connect failures.
func TestConnectRetriesUntilServerAvailable(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "failed to pick a free port")
	addr, ok := l.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("expected TCP address, got %T", l.Addr())
	}
	port := addr.Port
	_ = l.Close()

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	url := fmt.Sprintf("nats://127.0.0.1:%d", port)
	cfg := Config{
		URL:        url,
		ClientName: "test-retry",
		Logger:     logger,
	}

	// Delay server startup to force initial connect failures
	go func() {
		time.Sleep(1 * time.Second)
		opts := &server.Options{JetStream: true, Port: port}
		srv, createServerErr := server.NewServer(opts)
		assert.NoError(t, createServerErr, "failed to create embedded NATS server")
		go srv.Start()
		assert.True(t, srv.ReadyForConnections(5*time.Second), "embedded server did not start in time")
	}()

	// Attempt to connect with retries
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	conn, js, err := connect(ctx, cfg)
	require.NoError(t, err, "Connect should succeed after retries")
	assert.NotNil(t, conn, "nats.Conn should not be nil")
	assert.NotNil(t, js, "JetStream context should not be nil")

	// Cleanup
	_ = conn.Drain()
}
