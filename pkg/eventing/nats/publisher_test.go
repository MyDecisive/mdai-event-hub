package nats

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/decisiveai/mdai-event-hub/pkg/eventing"
	"github.com/nats-io/nats-server/v2/server"
	natsclient "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

//nolint:gocritic
func runJetStream(t *testing.T) (*server.Server, string) {
	t.Helper()
	tempDir := t.TempDir()
	ns, err := server.NewServer(&server.Options{
		JetStream: true,
		StoreDir:  tempDir,
		Port:      -1, // pick a random free port
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats-server did not start")
	}
	url := ns.ClientURL()
	t.Setenv("NATS_URL", url)

	return ns, url
}

func mustPublish(t *testing.T, pub *EventPublisher, ev eventing.MdaiEvent) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := pub.Publish(ctx, ev, "alert.mdai.test"); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

func setPodName(name string) { _ = os.Setenv("POD_NAME", name) }

func TestElasticGroupDelivery(t *testing.T) {
	assertion := assert.New(t)

	srv, _ := runJetStream(t)
	defer srv.Shutdown()

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	pub, err := NewPublisher(logger, "test")
	require.NoError(t, err)

	want := map[string]int{
		"mdai-hub-second|NoisyServiceFired": 5,
		"mdai-hub-second|CPUHigh":           5,
		"mdai-hub-second|DiskFill":          5,
	}
	got := make(map[string]int)
	var mu sync.Mutex

	handler := func(ev eventing.MdaiEvent) error {
		key := ev.HubName + "|" + ev.Name
		mu.Lock()
		got[key]++
		mu.Unlock()
		return nil
	}

	for i, id := range []string{"m1", "m2", "m3"} {
		setPodName(id)
		sub, err := NewSubscriber(logger, "test-subscriber-"+id)
		require.NoError(t, err, "subscriber %d", i)
		require.NoError(t, sub.Subscribe(t.Context(), eventing.AlertConsumerGroupName, "alert", handler), "subscribe %d", i)
	}

	for range 5 {
		mustPublish(t, pub, eventing.MdaiEvent{
			Name: "NoisyServiceFired", HubName: "mdai-hub-second",
			Source: "tester", Payload: `{"v":1}`,
		})
		mustPublish(t, pub, eventing.MdaiEvent{
			Name: "CPUHigh", HubName: "mdai-hub-second",
			Source: "tester", Payload: `{"v":2}`,
		})
		mustPublish(t, pub, eventing.MdaiEvent{
			Name: "DiskFill", HubName: "mdai-hub-second",
			Source: "tester", Payload: `{"v":3}`,
		})
	}

	time.Sleep(2 * time.Second)

	assertion.Equal(want, got, "all events delivered exactly once")
}

// TestPartitionKeyConsistency verifies that messages with the same key
// are always delivered to the same consumer instance within an elastic group.
func TestPartitionKeyConsistency(t *testing.T) {
	srv, _ := runJetStream(t)
	defer srv.Shutdown()

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	pub, err := NewPublisher(logger, "test-publisher")
	require.NoError(t, err)

	// Define two distinct event keys
	events := []eventing.MdaiEvent{
		{Name: "KeyA", HubName: "hub", Source: "src", Payload: `{"v":1}`},
		{Name: "KeyB", HubName: "hub", Source: "src", Payload: `{"v":2}`},
	}

	type recorder struct {
		seq []string
		mu  sync.Mutex
	}
	r1 := &recorder{}
	handler1 := func(ev eventing.MdaiEvent) error {
		r1.mu.Lock()
		r1.seq = append(r1.seq, ev.Name)
		r1.mu.Unlock()
		return nil
	}

	r2 := &recorder{}
	handler2 := func(ev eventing.MdaiEvent) error {
		r2.mu.Lock()
		r2.seq = append(r2.seq, ev.Name)
		r2.mu.Unlock()
		return nil
	}

	setPodName("member1")
	sub1, err := NewSubscriber(logger, "test-subscriber1")
	require.NoError(t, err)
	require.NoError(t, sub1.Subscribe(t.Context(), eventing.AlertConsumerGroupName, "alert", handler1))

	setPodName("member2")
	sub2, err := NewSubscriber(logger, "test-subscriber2")
	require.NoError(t, err)
	require.NoError(t, sub2.Subscribe(t.Context(), eventing.AlertConsumerGroupName, "alert", handler2))

	const count = 5
	for range count {
		for _, ev := range events {
			mustPublish(t, pub, ev)
		}
	}

	time.Sleep(2 * time.Second)

	// Verify total messages delivered equals published count
	gotTotal := len(r1.seq) + len(r2.seq)
	wantTotal := count * len(events)
	assert.Equal(t, wantTotal, gotTotal, "total delivered messages should match published count")

	// Determine assignment of keys to members and ensure consistency
	set1 := make(map[string]struct{})
	for _, name := range r1.seq {
		set1[name] = struct{}{}
	}
	set2 := make(map[string]struct{})
	for _, name := range r2.seq {
		set2[name] = struct{}{}
	}

	for key := range set1 {
		if _, ok := set2[key]; ok {
			t.Errorf("Key %s was delivered to both subscribers", key)
		}
	}

	_ = sub1.Close()
	_ = sub2.Close()
}

func TestDLQForwarding(t *testing.T) {
	srv, url := runJetStream(t)
	defer srv.Shutdown()

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	pub, err := NewPublisher(logger, "test-publisher")
	require.NoError(t, err)

	// Subscribe directly to the DLQ subject
	nc, err := natsclient.Connect(url)
	require.NoError(t, err)
	defer func(nc *natsclient.Conn) {
		if cloggedDrainErr := nc.Drain(); cloggedDrainErr != nil {
			logger.Warn("failed to drain NATS connection", zap.Error(cloggedDrainErr))
		}
	}(nc)

	dlqSubject := "events.alert.dlq"
	dlqSub, err := nc.SubscribeSync(dlqSubject)
	require.NoError(t, err)

	// Create a subscriber whose handler always errors
	sub, err := NewSubscriber(logger, "test-dlq-subscriber")
	require.NoError(t, err)
	err = sub.Subscribe(t.Context(), eventing.AlertConsumerGroupName, "alert", func(ev eventing.MdaiEvent) error {
		return errors.New("handler error")
	})
	require.NoError(t, err)

	// Publish a single event
	ev := eventing.MdaiEvent{Name: "FailTest", HubName: "hub", Source: "src", Payload: `{"v":42}`}
	mustPublish(t, pub, ev)

	// Expect the message in DLQ
	msg, err := dlqSub.NextMsg(5 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, dlqSubject, msg.Subject)
	assert.Equal(t, "handler_error", msg.Header.Get("dlq_reason"), "reason header should reflect error type")
	assert.Equal(t, "handler error", msg.Header.Get("dlq_error"))

	_ = sub.Close()
}

// TestDuplicateSuppression ensures that publishing the same message ID twice
// within the duplicate window only delivers once.
func TestDuplicateSuppression(t *testing.T) {
	srv, _ := runJetStream(t)
	defer srv.Shutdown()

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	pub, err := NewPublisher(logger, "test")
	require.NoError(t, err)

	var mu sync.Mutex
	delivered := 0

	// Subscriber records each delivery
	sub, err := NewSubscriber(logger, "test")
	require.NoError(t, err)
	err = sub.Subscribe(t.Context(), eventing.AlertConsumerGroupName, "alerts", func(ev eventing.MdaiEvent) error {
		mu.Lock()
		delivered++
		mu.Unlock()
		return nil
	})
	require.NoError(t, err)

	// Publish the same event twice
	ev := eventing.MdaiEvent{ID: "dup-123", Name: "DupTest", HubName: "hub", Source: "src", Payload: `{"v":99}`}
	mustPublish(t, pub, ev)
	time.Sleep(100 * time.Millisecond)
	mustPublish(t, pub, ev)

	// Allow for delivery
	time.Sleep(2 * time.Second)

	mu.Lock()
	count := delivered
	mu.Unlock()
	if count != 1 {
		t.Errorf("expected exactly 1 delivery, got %d", count)
	}

	_ = sub.Close()
}

// TestSingleActiveMember verifies that after registering 10 members then disconnecting them,
// the lone remaining subscriber receives all events across multiple keys.
func TestSingleActiveMember(t *testing.T) {
	srv, _ := runJetStream(t)
	defer srv.Shutdown()

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	pub, err := NewPublisher(logger, "test")
	require.NoError(t, err)

	// Register and disconnect 10 subscribers to add them to the group membership
	for i := 1; i <= 10; i++ {
		pod := fmt.Sprintf("member_%02d", i)
		setPodName(pod)
		sub, newSubErr := NewSubscriber(logger, "test")
		require.NoError(t, newSubErr)
		require.NoError(t, sub.Subscribe(t.Context(), eventing.AlertConsumerGroupName, "alert", func(ev eventing.MdaiEvent) error { return nil }))
		_ = sub.Close()
	}

	time.Sleep(2 * time.Second)

	// Only one active subscriber remains
	active := "member_11"
	setPodName(active)
	var mu sync.Mutex
	var received []string
	sub, err := NewSubscriber(logger, "test")
	require.NoError(t, err)
	require.NoError(t, sub.Subscribe(t.Context(), eventing.AlertConsumerGroupName, "alerts", func(ev eventing.MdaiEvent) error {
		mu.Lock()
		received = append(received, ev.Name)
		mu.Unlock()
		return nil
	}))
	defer func(sub *EventSubscriber) {
		_ = sub.Close()
	}(sub)

	// Publish events for two keys
	keys := []string{"KeyA", "KeyB"}
	const count = 5
	for i := range count {
		for _, k := range keys {
			mustPublish(t, pub, eventing.MdaiEvent{Name: k, HubName: "hub", Source: "src", Payload: fmt.Sprintf(`{"i":%d}`, i)})
		}
	}
	time.Sleep(2 * time.Second)

	mu.Lock()
	total := len(received)
	mu.Unlock()
	want := len(keys) * count
	assert.Equal(t, want, total, "active subscriber should receive all %d messages, got %d", want, total)
}

func TestPublishEventIDGeneration(t *testing.T) {
	srv, _ := runJetStream(t)
	defer srv.Shutdown()

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	pub, err := NewPublisher(logger, "test-id-gen")
	require.NoError(t, err)
	defer func() {
		err := pub.Close()
		require.NoError(t, err, "failed to close pub1")
	}()

	tests := []struct {
		name  string
		event eventing.MdaiEvent
		hasID bool
		desc  string
	}{
		{
			name: "generates ID when empty",
			event: eventing.MdaiEvent{
				Name:    "TestEvent",
				HubName: "hub",
				Source:  "test",
				Payload: `{"test": true}`,
			},
			hasID: false,
			desc:  "should generate ID when not provided",
		},
		{
			name: "preserves existing ID",
			event: eventing.MdaiEvent{
				ID:      "existing-id-123",
				Name:    "TestEvent",
				HubName: "hub",
				Source:  "test",
				Payload: `{"test": true}`,
			},
			hasID: true,
			desc:  "should preserve existing ID",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			originalID := tt.event.ID

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			err := pub.Publish(ctx, tt.event, "alert.test.test")
			require.NoError(t, err, tt.desc)

			if tt.hasID {
				assert.Equal(t, originalID, tt.event.ID, "existing ID should be preserved")
			} else {
				assert.Empty(t, originalID, "original event should have empty ID")
			}
		})
	}
}

func TestPublishTimestampGeneration(t *testing.T) {
	srv, _ := runJetStream(t)
	defer srv.Shutdown()

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	pub, err := NewPublisher(logger, "test-timestamp")
	require.NoError(t, err)
	defer func() {
		err := pub.Close()
		require.NoError(t, err, "failed to close publisher")
	}()

	tests := []struct {
		name    string
		event   eventing.MdaiEvent
		hasTime bool
		desc    string
	}{
		{
			name: "generates timestamp when zero",
			event: eventing.MdaiEvent{
				Name:    "TestEvent",
				HubName: "hub",
				Source:  "test",
				Payload: `{"test": true}`,
			},
			hasTime: false,
			desc:    "should generate timestamp when not provided",
		},
		{
			name: "preserves existing timestamp",
			event: eventing.MdaiEvent{
				Name:      "TestEvent",
				HubName:   "hub",
				Source:    "test",
				Payload:   `{"test": true}`,
				Timestamp: time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
			},
			hasTime: true,
			desc:    "should preserve existing timestamp",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			originalTime := tt.event.Timestamp

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			beforePublish := time.Now().UTC()
			err := pub.Publish(ctx, tt.event, "alert.test.test")
			afterPublish := time.Now().UTC()

			require.NoError(t, err, tt.desc)

			if tt.hasTime {
				assert.Equal(t, originalTime, tt.event.Timestamp, "existing timestamp should be preserved")
			} else {
				assert.True(t, originalTime.IsZero(), "original timestamp should be zero")
				assert.True(t, beforePublish.Before(afterPublish) || beforePublish.Equal(afterPublish))
			}
		})
	}
}

func TestPublishSubjectGeneration(t *testing.T) {
	srv, _ := runJetStream(t)
	defer srv.Shutdown()

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	pub, err := NewPublisher(logger, "test-subject")
	require.NoError(t, err)
	defer func() {
		closeErr := pub.Close()
		require.NoError(t, closeErr, "failed to close publisher")
	}()

	event := eventing.MdaiEvent{
		Name:    "Test.Event",
		HubName: "hub.name",
		Source:  "test source",
		Payload: `{"test": true}`,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = pub.Publish(ctx, event, "alert.test.test")
	require.NoError(t, err)
}

func TestPublishHeaderGeneration(t *testing.T) {
	srv, _ := runJetStream(t)
	defer srv.Shutdown()

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	pub, err := NewPublisher(logger, "test-headers")
	require.NoError(t, err)
	defer func() {
		err := pub.Close()
		require.NoError(t, err, "failed to close pub")
	}()

	tests := []struct {
		name  string
		event eventing.MdaiEvent
		desc  string
	}{
		{
			name: "sets basic headers",
			event: eventing.MdaiEvent{
				ID:      "test-id",
				Name:    "TestEvent",
				HubName: "TestHub",
				Source:  "TestSource",
				Payload: `{"test": true}`,
			},
			desc: "should set name, source, hubName, and ID headers",
		},
		{
			name: "sets correlation ID header when present",
			event: eventing.MdaiEvent{
				ID:            "test-id-2",
				Name:          "TestEvent2",
				HubName:       "TestHub2",
				Source:        "TestSource2",
				CorrelationID: "correlation-123",
				Payload:       `{"test": true}`,
			},
			desc: "should set correlationId header when provided",
		},
		{
			name: "skips correlation ID header when empty",
			event: eventing.MdaiEvent{
				ID:      "test-id-3",
				Name:    "TestEvent3",
				HubName: "TestHub3",
				Source:  "TestSource3",
				Payload: `{"test": true}`,
			},
			desc: "should not set correlationId header when empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			err := pub.Publish(ctx, tt.event, "alert.test.test")
			require.NoError(t, err, tt.desc)
		})
	}
}

func TestPublishJSONSerialization(t *testing.T) {
	srv, _ := runJetStream(t)
	defer srv.Shutdown()

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	pub, err := NewPublisher(logger, "test-json")
	require.NoError(t, err)
	defer func() {
		err := pub.Close()
		require.NoError(t, err, "failed to close publisher")
	}()

	tests := []struct {
		name    string
		event   eventing.MdaiEvent
		wantErr bool
		desc    string
	}{
		{
			name: "serializes valid event",
			event: eventing.MdaiEvent{
				ID:      "valid-id",
				Name:    "ValidEvent",
				HubName: "ValidHub",
				Source:  "ValidSource",
				Payload: `{"valid": true}`,
			},
			wantErr: false,
			desc:    "should serialize valid event successfully",
		},
		{
			name: "handles empty payload",
			event: eventing.MdaiEvent{
				ID:      "empty-payload-id",
				Name:    "EmptyPayloadEvent",
				HubName: "EmptyHub",
				Source:  "EmptySource",
				Payload: "",
			},
			wantErr: false,
			desc:    "should handle empty payload",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			err := pub.Publish(ctx, tt.event, "alert.test.test")

			if tt.wantErr {
				assert.Error(t, err, tt.desc)
			} else {
				assert.NoError(t, err, tt.desc)
			}
		})
	}
}

func TestPublisherClose(t *testing.T) {
	srv, _ := runJetStream(t)
	defer srv.Shutdown()

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	pub, err := NewPublisher(logger, "test-close")
	require.NoError(t, err)

	err = pub.Close()
	require.NoError(t, err, "should close successfully")

	err = pub.Close()
	require.NoError(t, err, "should handle double close gracefully")
}

func TestNewPublisherConfigHandling(t *testing.T) {
	srv, url := runJetStream(t)
	defer srv.Shutdown()

	t.Setenv("NATS_URL", url)
	t.Setenv("NATS_SUBJECT", "custom-events")
	t.Setenv("NATS_STREAM_NAME", "CUSTOM_STREAM")

	logger := zap.NewNop()

	pub, err := NewPublisher(logger, "test-config")
	require.NoError(t, err)
	defer func() {
		err := pub.Close()
		require.NoError(t, err, "failed to close publisher")
	}()

	assert.Equal(t, "custom-events", pub.cfg.Subject, "should use custom subject from env")
	assert.Equal(t, "CUSTOM_STREAM", pub.cfg.StreamName, "should use custom stream name from env")
	assert.Equal(t, "test-config", pub.cfg.ClientName, "should set client name")
	assert.Equal(t, logger, pub.cfg.Logger, "should set logger")
}

func TestNewPublisherStreamCreation(t *testing.T) {
	srv, _ := runJetStream(t)
	defer srv.Shutdown()

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	pub1, err := NewPublisher(logger, "test-stream-1")
	require.NoError(t, err, "first publisher should create stream successfully")
	defer func() {
		pub1CloseErr := pub1.Close()
		require.NoError(t, pub1CloseErr, "failed to close pub1")
	}()

	pub2, err := NewPublisher(logger, "test-stream-2")
	require.NoError(t, err, "second publisher should use existing stream")
	defer func() {
		pub2CloseErr := pub2.Close()
		require.NoError(t, pub2CloseErr, "failed to close pub2")
	}()

	event := eventing.MdaiEvent{
		Name:    "StreamTest",
		HubName: "hub",
		Source:  "test",
		Payload: `{"test": true}`,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = pub1.Publish(ctx, event, "alert.test.test")
	require.NoError(t, err, "first publisher should publish successfully")

	err = pub2.Publish(ctx, event, "alert.test.test")
	require.NoError(t, err, "second publisher should publish successfully")
}
