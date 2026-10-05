//go:build performance

package server

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/n0ot/nvremoted/internal/perf"
	"github.com/sirupsen/logrus"
)

// These benchmarks deliberately call the same decoder, channel worker and
// recipient handler used by production. The local payloads are protocol-shaped
// examples; benchmark results are ns/op plus the standard Go allocation data.
var perfSink any
var perfStringSink string
var perfBytesSink []byte

var perfTypedMessage = []byte(`{"type":"join","channel":"benchmark","connection_type":"master"}`)

func TestPerformanceComponentAdapters(t *testing.T) {
	raw, err := perf.Payload("arbitrary-nested")
	if err != nil {
		t.Fatal(err)
	}
	message, err := unmarshalClientMessage(41, raw)
	if err != nil {
		t.Fatal(err)
	}
	channelMsg, ok := message.(*channelMessage)
	if !ok || channelMsg.origin != 41 {
		t.Fatalf("decoded message = %#v", message)
	}
	buffer := &perfBufferConn{}
	recipient := &client{conn: buffer, encoder: json.NewEncoder(buffer)}
	handleClientChannelEvent(recipient, *channelMsg)
	var response map[string]json.RawMessage
	if err := json.Unmarshal(buffer.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if string(response["origin"]) != "41" || len(response["tree"]) == 0 {
		t.Fatalf("recipient response omitted origin or message body: %s", buffer.Bytes())
	}

	typed, err := unmarshalClientMessage(1, perfTypedMessage)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := typed.(*ClientJoinMessage); !ok {
		t.Fatalf("typed decode = %T", typed)
	}

	// Exercise the actual channel worker and recipient event handler together.
	reg := &registry{clients: make(map[uint64]channelMember), channels: make(map[string]*channel)}
	ch := &channel{done: make(chan struct{}), name: "performance-smoke", messages: make(chan channelMessage), joins: make(chan joinChannelRequest), parts: make(chan leaveChannelRequest)}
	events := make(chan Message, defaultEventQueueSize)
	ch.members = []channelMember{{id: 2, events: events, stop: func(string) { t.Error("unexpected event queue overflow") }}}
	go func() { defer close(ch.done); ch.start(reg) }()
	defer func() {
		ch.leave(2)
		close(events)
		<-ch.done
	}()
	ch.messages <- *channelMsg
	forwarded := <-events
	forwardBuffer := &perfBufferConn{}
	forwardClient := &client{conn: forwardBuffer, encoder: json.NewEncoder(forwardBuffer)}
	handleClientChannelEvent(forwardClient, forwarded)
	response = nil
	if err := json.Unmarshal(forwardBuffer.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if string(response["origin"]) != "41" || len(response["tree"]) == 0 {
		t.Fatalf("forwarded response omitted origin or payload: %s", forwardBuffer.Bytes())
	}
}

func perfComponentPayload(b *testing.B, name string) []byte {
	b.Helper()
	payload, err := perf.Payload(name)
	if err != nil {
		b.Fatal(err)
	}
	return payload
}

func BenchmarkPerformanceMessageDecode(b *testing.B) {
	b.Run("typed-second-unmarshal", func(b *testing.B) {
		b.SetBytes(int64(len(perfTypedMessage)))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var msg Message = &ClientJoinMessage{}
			if err := json.Unmarshal(perfTypedMessage, &msg); err != nil {
				b.Fatal(err)
			}
			perfSink = msg
		}
	})
	b.Run("arbitrary-map-unmarshal", func(b *testing.B) {
		raw := perfComponentPayload(b, "arbitrary-nested")
		b.SetBytes(int64(len(raw)))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			message := make(map[string]interface{})
			if err := json.Unmarshal(raw, &message); err != nil {
				b.Fatal(err)
			}
			perfSink = message
		}
	})
	for _, name := range perf.PayloadNames() {
		raw := perfComponentPayload(b, name)
		b.Run(name+"/reader", func(b *testing.B) {
			source := bytes.NewReader(nil)
			reader := newMessageReader(source, 0)
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				source.Reset(raw)
				reader.input.Reset(source)
				reader.pending = nil
				got, err := reader.read()
				if err != nil {
					b.Fatal(err)
				}
				perfBytesSink = got
			}
		})
		b.Run(name+"/type", func(b *testing.B) {
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var generic GenericClientMessage
				if err := json.Unmarshal(raw, &generic); err != nil {
					b.Fatal(err)
				}
				perfStringSink = generic.Type
			}
		})
		b.Run(name+"/protocol-unmarshal", func(b *testing.B) {
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				msg, err := unmarshalClientMessage(7, raw)
				if err != nil {
					b.Fatal(err)
				}
				perfSink = msg
			}
		})
	}
}

func BenchmarkPerformanceRecipientPreparation(b *testing.B) {
	for _, name := range perf.PayloadNames() {
		raw := perfComponentPayload(b, name)
		msg, err := unmarshalClientMessage(19, raw)
		if err != nil {
			b.Fatal(err)
		}
		channelMsg, ok := msg.(*channelMessage)
		if !ok {
			continue
		}
		b.Run(name+"/copy-map-and-origin", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				response := make(ClientResponse)
				for key, value := range channelMsg.msg {
					response[key] = value
				}
				response["origin"] = channelMsg.origin
				perfSink = response
			}
		})
		b.Run(name+"/copy-map", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				response := make(ClientResponse)
				for key, value := range channelMsg.msg {
					response[key] = value
				}
				perfSink = response
			}
		})
		b.Run(name+"/add-origin", func(b *testing.B) {
			response := make(ClientResponse, 1)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				response["origin"] = channelMsg.origin
				perfSink = response
			}
		})
		b.Run(name+"/json-encode", func(b *testing.B) {
			response := make(ClientResponse)
			for key, value := range channelMsg.msg {
				response[key] = value
			}
			response["origin"] = channelMsg.origin
			var output bytes.Buffer
			encoder := json.NewEncoder(&output)
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				output.Reset()
				if err := encoder.Encode(response); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkPerformanceChannelHandoff(b *testing.B) {
	for _, fanout := range []int{1, 2, 4, 10, 32} {
		b.Run(strconv.Itoa(fanout)+"-recipients", func(b *testing.B) {
			benchmarkChannelPipeline(b, fanout, perfComponentPayload(b, "arbitrary-nested"), false, false)
		})
	}
}

func BenchmarkPerformanceChannelMessagePipeline(b *testing.B) {
	for _, fanout := range []int{1, 2, 4, 10, 32} {
		b.Run(strconv.Itoa(fanout)+"-recipients/handoff-recipient-encode", func(b *testing.B) {
			benchmarkChannelPipeline(b, fanout, perfComponentPayload(b, "arbitrary-nested"), false, true)
		})
		b.Run(strconv.Itoa(fanout)+"-recipients/full-reader-decode-dispatch-encode", func(b *testing.B) {
			benchmarkChannelPipeline(b, fanout, perfComponentPayload(b, "arbitrary-nested"), true, true)
		})
	}
}

func benchmarkChannelPipeline(b *testing.B, fanout int, raw []byte, decode, encode bool) {
	registry := &registry{clients: make(map[uint64]channelMember), channels: make(map[string]*channel)}
	ch := &channel{
		done: make(chan struct{}), name: "benchmark",
		messages: make(chan channelMessage), joins: make(chan joinChannelRequest), parts: make(chan leaveChannelRequest),
	}
	consumers := make([]chan Message, fanout)
	clients := make([]*client, fanout)
	acks := make([]chan struct{}, fanout)
	var overflow atomic.Uint64
	for i := range consumers {
		id := uint64(i + 2)
		consumers[i] = make(chan Message, defaultEventQueueSize)
		acks[i] = make(chan struct{}, 1)
		conn := perfDiscardConn{}
		clients[i] = &client{conn: conn, encoder: json.NewEncoder(conn), log: logrus.New()}
		ch.members = append(ch.members, channelMember{id: id, events: consumers[i], stop: func(string) { overflow.Add(1) }})
	}
	go func() { defer close(ch.done); ch.start(registry) }()
	var consumersDone sync.WaitGroup
	for i, events := range consumers {
		consumersDone.Add(1)
		go func(i int, events <-chan Message) {
			defer consumersDone.Done()
			for msg := range events {
				if _, ok := msg.(channelMessage); !ok {
					continue
				}
				if encode {
					handleClientChannelEvent(clients[i], msg)
				}
				acks[i] <- struct{}{}
			}
		}(i, events)
	}
	baseMessage, err := unmarshalClientMessage(1, raw)
	if err != nil {
		b.Fatal(err)
	}
	baseChannelMsg := *(baseMessage.(*channelMessage))
	defer func() {
		b.StopTimer()
		for _, member := range append([]channelMember(nil), ch.members...) {
			ch.leave(member.id)
		}
		<-ch.done
		for _, events := range consumers {
			close(events)
		}
		consumersDone.Wait()
	}()

	b.SetBytes(int64(len(raw)))
	b.ReportAllocs()
	b.ResetTimer()
	source := bytes.NewReader(nil)
	reader := newMessageReader(source, 0)
	framedInput := append(append([]byte(nil), raw...), '\n')
	for i := 0; i < b.N; i++ {
		channelMsg := baseChannelMsg
		if decode {
			source.Reset(framedInput)
			reader.input.Reset(source)
			reader.pending = nil
			framed, err := reader.read()
			if err != nil {
				b.Fatal(err)
			}
			message, err := unmarshalClientMessage(1, framed)
			if err != nil {
				b.Fatal(err)
			}
			channelMsg = *(message.(*channelMessage))
		}
		ch.messages <- channelMsg
		for _, ack := range acks {
			<-ack
		}
	}
	b.StopTimer()
	if got := overflow.Load(); got != 0 {
		b.Fatalf("event queues overflowed %d times", got)
	}
}

type perfDiscardConn struct{}

func (perfDiscardConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (perfDiscardConn) Write(p []byte) (int, error)      { return len(p), nil }
func (perfDiscardConn) Close() error                     { return nil }
func (perfDiscardConn) LocalAddr() net.Addr              { return perfAddr("local") }
func (perfDiscardConn) RemoteAddr() net.Addr             { return perfAddr("remote") }
func (perfDiscardConn) SetDeadline(time.Time) error      { return nil }
func (perfDiscardConn) SetReadDeadline(time.Time) error  { return nil }
func (perfDiscardConn) SetWriteDeadline(time.Time) error { return nil }

var _ net.Conn = perfDiscardConn{}

type perfAddr string

func (a perfAddr) Network() string { return "perf" }
func (a perfAddr) String() string  { return string(a) }

type perfBufferConn struct{ bytes.Buffer }

func (*perfBufferConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (*perfBufferConn) Close() error                     { return nil }
func (*perfBufferConn) LocalAddr() net.Addr              { return perfAddr("local") }
func (*perfBufferConn) RemoteAddr() net.Addr             { return perfAddr("remote") }
func (*perfBufferConn) SetDeadline(time.Time) error      { return nil }
func (*perfBufferConn) SetReadDeadline(time.Time) error  { return nil }
func (*perfBufferConn) SetWriteDeadline(time.Time) error { return nil }

// Keep these compile-time assertions close to the helper rather than relying
// on a test-only alternate send implementation.
var _ net.Conn = (*perfBufferConn)(nil)
