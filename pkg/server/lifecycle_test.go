package server

import (
	"encoding/json"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

// A retained member is also carried by queued join/leave notifications. Its
// event queue must remain open even after the client's cleanup has finished.
func TestDisconnectedClientEventQueueRemainsOpen(t *testing.T) {
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	log := logrus.New()
	log.SetOutput(io.Discard)
	hook := &disconnectedHook{done: make(chan struct{})}
	log.AddHook(hook)
	srv := &Server{Log: log, registry: registry{
		clients: make(map[uint64]channelMember), channels: make(map[string]*channel),
	}}
	srv.serveClient(conn, 7, "test peer")
	peer.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.WriteString(peer, "{\"type\":\"join\",\"channel\":\"test\",\"connection_type\":\"master\"}\n"); err != nil {
		t.Fatal(err)
	}
	var response ClientChannelJoinedResponse
	if err := json.NewDecoder(peer).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if response.Type != "channel_joined" {
		t.Fatalf("response = %#v", response)
	}
	srv.registry.lock.RLock()
	member := srv.registry.clients[7]
	srv.registry.lock.RUnlock()
	peer.Close()
	awaitLivenessSignal(t, hook.done)
	// No receiver remains and the queue is empty. A stale send must not panic.
	select {
	case member.events <- pingMessage{}:
	default:
		t.Fatal("disconnected client's event queue is not empty")
	}
}

func newLifecycleRegistry() *registry {
	return &registry{clients: make(map[uint64]channelMember), channels: make(map[string]*channel)}
}

func joinLifecycleClient(t *testing.T, reg *registry, name string, id uint64) *client {
	t.Helper()
	c := &client{id: id, events: make(chan Message, 1)}
	ch, _, err := joinChannel(name, channelMember{id: id, connectionType: "master", events: c.events}, reg)
	if err != nil {
		t.Fatal(err)
	}
	c.channel = ch
	return c
}

func receiveLifecycleEvent(t *testing.T, events <-chan Message) Message {
	t.Helper()
	select {
	case msg := <-events:
		return msg
	case <-time.After(5 * time.Second):
		t.Fatal("event delivery did not finish")
		return nil
	}
}

func assertLifecycleRegistryEmpty(t *testing.T, reg *registry) {
	t.Helper()
	reg.lock.RLock()
	defer reg.lock.RUnlock()
	if len(reg.clients) != 0 || len(reg.channels) != 0 || reg.numE2eChannels != 0 {
		t.Fatalf("registry after leave: clients=%d channels=%d e2e=%d", len(reg.clients), len(reg.channels), reg.numE2eChannels)
	}
}

func TestLeaveAcknowledgesRegistryCleanup(t *testing.T) {
	reg := newLifecycleRegistry()
	c := joinLifecycleClient(t, reg, "E2E_"+strings.Repeat("a", 64), 7)
	c.channel.leave(c.id)
	// Deliberately do not take reg.lock: leave's acknowledgement must itself
	// synchronize with all registry mutations. A subsequent RLock would hide
	// premature acknowledgement by waiting for asynchronous deletion. There
	// are no other members, pending joins, or registry writers in this test.
	if len(reg.clients) != 0 || len(reg.channels) != 0 || reg.numE2eChannels != 0 {
		t.Fatalf("leave acknowledged before registry cleanup: clients=%d channels=%d e2e=%d", len(reg.clients), len(reg.channels), reg.numE2eChannels)
	}
	if len(c.channel.members) != 0 {
		t.Fatal("leave acknowledged before channel membership cleanup")
	}
}

func TestClientLeaveUnblocksInFlightPingDispatch(t *testing.T) {
	reg := newLifecycleRegistry()
	c := joinLifecycleClient(t, reg, "test", 7)
	// The handler has exited with a full queue. Pin the dispatcher to this
	// member under the same read lock used by Server.Serve, before cleanup.
	c.events <- pingMessage{}
	dispatchStarted := make(chan struct{})
	dispatchDone := make(chan struct{})
	go func() {
		reg.lock.RLock()
		member := reg.clients[c.id]
		close(dispatchStarted)
		member.events <- pingMessage{}
		reg.lock.RUnlock()
		close(dispatchDone)
	}()
	awaitLivenessSignal(t, dispatchStarted)
	left := make(chan struct{})
	go func() {
		c.leaveChannel()
		close(left)
	}()
	awaitLivenessSignal(t, left)
	awaitLivenessSignal(t, dispatchDone)
	assertLifecycleRegistryEmpty(t, reg)
	// The channel goroutine has also completed all member mutations by ack.
	if len(c.channel.members) != 0 {
		t.Fatal("departed client remains a channel member")
	}
}

func TestClientLeaveUnblocksChannelDelivery(t *testing.T) {
	reg := newLifecycleRegistry()
	departing := joinLifecycleClient(t, reg, "test", 7)
	remaining := joinLifecycleClient(t, reg, "test", 8)
	if msg, ok := receiveLifecycleEvent(t, departing.events).(joinedChannelMSG); !ok || msg.id != remaining.id {
		t.Fatalf("join notification = %#v", msg)
	}
	// Stop consuming the departing member's queue. Delivery of the next
	// message cannot complete until cleanup starts draining it.
	departing.events <- pingMessage{}
	msg := channelMessage{origin: 99, msg: map[string]interface{}{"type": "test"}}
	departing.channel.messages <- msg
	left := make(chan struct{})
	go func() {
		departing.leaveChannel()
		close(left)
	}()
	if delivered, ok := receiveLifecycleEvent(t, remaining.events).(channelMessage); !ok || delivered.origin != msg.origin {
		t.Fatalf("message delivery = %#v", delivered)
	}
	if notification, ok := receiveLifecycleEvent(t, remaining.events).(leftChannelMSG); !ok || notification.id != departing.id {
		t.Fatalf("leave notification = %#v", notification)
	}
	awaitLivenessSignal(t, left)
	reg.lock.RLock()
	_, present := reg.clients[departing.id]
	remainingRegistered := reg.clients[remaining.id].id == remaining.id
	reg.lock.RUnlock()
	if present || !remainingRegistered {
		t.Fatal("registry membership is incorrect after leave")
	}
	// Delivery and ping dispatch after leave still reach the remaining member.
	remaining.channel.messages <- msg
	if delivered, ok := receiveLifecycleEvent(t, remaining.events).(channelMessage); !ok || delivered.origin != msg.origin {
		t.Fatalf("delivery after leave = %#v", delivered)
	}
	reg.lock.RLock()
	for _, member := range reg.clients {
		member.events <- pingMessage{}
	}
	reg.lock.RUnlock()
	if _, ok := receiveLifecycleEvent(t, remaining.events).(pingMessage); !ok {
		t.Fatal("remaining member did not receive ping")
	}
	remaining.leaveChannel()
	assertLifecycleRegistryEmpty(t, reg)
}

func TestConcurrentClientLeavesWithPingDispatch(t *testing.T) {
	reg := newLifecycleRegistry()
	first := joinLifecycleClient(t, reg, "test", 7)
	second := joinLifecycleClient(t, reg, "test", 8)
	receiveLifecycleEvent(t, first.events)
	third := joinLifecycleClient(t, reg, "E2E_"+strings.Repeat("a", 64), 9)
	clients := []*client{first, second, third}
	for _, c := range clients {
		c.events <- pingMessage{}
	}
	dispatchStarted := make(chan struct{})
	dispatchDone := make(chan struct{})
	go func() {
		reg.lock.RLock()
		close(dispatchStarted)
		for _, member := range reg.clients {
			member.events <- pingMessage{}
		}
		reg.lock.RUnlock()
		close(dispatchDone)
	}()
	awaitLivenessSignal(t, dispatchStarted)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for _, c := range clients {
		wg.Add(1)
		go func(c *client) {
			defer wg.Done()
			<-start
			c.leaveChannel()
		}(c)
	}
	left := make(chan struct{})
	go func() {
		wg.Wait()
		close(left)
	}()
	close(start)
	awaitLivenessSignal(t, left)
	awaitLivenessSignal(t, dispatchDone)
	assertLifecycleRegistryEmpty(t, reg)
}
