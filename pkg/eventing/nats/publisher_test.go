package nats

import (
	"context"
	"errors"
	"fmt"
	"github.com/decisiveai/mdai-event-hub/pkg/eventing"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsclient "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

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
	assert.NoError(t, os.Setenv("NATS_URL", ns.ClientURL()))

	return ns, ns.ClientURL()
}

func mustPublish(t *testing.T, pub *EventPublisher, ev eventing.MdaiEvent) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pub.Publish(ctx, ev); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

func setPodName(name string) { _ = os.Setenv("POD_NAME", name) }

func TestElasticGroupDelivery(t *testing.T) {
	assert := assert.New(t)

	srv, _ := runJetStream(t)
	defer srv.Shutdown()

	logger, err := zap.NewDevelopment()
	assert.NoError(err)

	pub, err := NewPublisher(logger, "test")
	assert.NoError(err)

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
		assert.NoError(err, "subscriber %d", i)
		assert.NoError(sub.Subscribe(context.Background(), handler), "subscribe %d", i)
	}

	for n := 0; n < 5; n++ {
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

	assert.Equal(want, got, "all events delivered exactly once")
}

// TestPartitionKeyConsistency verifies that messages with the same key
// are always delivered to the same consumer instance within an elastic group.
func TestPartitionKeyConsistency(t *testing.T) {
	srv, _ := runJetStream(t)
	defer srv.Shutdown()

	logger, err := zap.NewDevelopment()
	assert.NoError(t, err)

	pub, err := NewPublisher(logger, "test-publisher")
	assert.NoError(t, err)

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
	assert.NoError(t, err)
	assert.NoError(t, sub1.Subscribe(context.Background(), handler1))

	setPodName("member2")
	sub2, err := NewSubscriber(logger, "test-subscriber2")
	assert.NoError(t, err)
	assert.NoError(t, sub2.Subscribe(context.Background(), handler2))

	const count = 5
	for i := 0; i < count; i++ {
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
	assert.NoError(t, err)

	pub, err := NewPublisher(logger, "test-publisher")
	assert.NoError(t, err)

	// Subscribe directly to the DLQ subject
	nc, err := natsclient.Connect(url)
	assert.NoError(t, err)
	defer func(nc *natsclient.Conn) {
		if err := nc.Drain(); err != nil {
			logger.Warn("failed to drain NATS connection", zap.Error(err))
		}
	}(nc)

	dlqSubject := pub.cfg.Subject + ".dlq"
	dlqSub, err := nc.SubscribeSync(dlqSubject)
	assert.NoError(t, err)

	// Create a subscriber whose handler always errors
	sub, err := NewSubscriber(logger, "test-dlq-subscriber")
	assert.NoError(t, err)
	err = sub.Subscribe(context.Background(), func(ev eventing.MdaiEvent) error {
		return errors.New("handler error")
	})
	assert.NoError(t, err)

	// Publish a single event
	ev := eventing.MdaiEvent{Name: "FailTest", HubName: "hub", Source: "src", Payload: `{"v":42}`}
	mustPublish(t, pub, ev)

	// Expect the message in DLQ
	msg, err := dlqSub.NextMsg(5 * time.Second)
	assert.NoError(t, err)
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
	assert.NoError(t, err)

	pub, err := NewPublisher(logger, "test")
	assert.NoError(t, err)

	var mu sync.Mutex
	delivered := 0

	// Subscriber records each delivery
	sub, err := NewSubscriber(logger, "test")
	assert.NoError(t, err)
	err = sub.Subscribe(context.Background(), func(ev eventing.MdaiEvent) error {
		mu.Lock()
		delivered++
		mu.Unlock()
		return nil
	})
	assert.NoError(t, err)

	// Publish the same event twice
	ev := eventing.MdaiEvent{Id: "dup-123", Name: "DupTest", HubName: "hub", Source: "src", Payload: `{"v":99}`}
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
	assert.NoError(t, err)

	pub, err := NewPublisher(logger, "test")
	assert.NoError(t, err)

	// Register and disconnect 10 subscribers to add them to the group membership
	for i := 1; i <= 10; i++ {
		pod := fmt.Sprintf("member_%02d", i)
		setPodName(pod)
		sub, err := NewSubscriber(logger, "test")
		assert.NoError(t, err)
		assert.NoError(t, sub.Subscribe(context.Background(), func(ev eventing.MdaiEvent) error { return nil }))
		_ = sub.Close()
	}

	time.Sleep(2 * time.Second)

	// Only one active subscriber remains
	active := "member_11"
	setPodName(active)
	var mu sync.Mutex
	var received []string
	sub, err := NewSubscriber(logger, "test")
	assert.NoError(t, err)
	assert.NoError(t, sub.Subscribe(context.Background(), func(ev eventing.MdaiEvent) error {
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
	for i := 0; i < count; i++ {
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
