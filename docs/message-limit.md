# Incoming message size

## Wire evidence and compatibility

Investigated on 2026-10-04:

Links below identify the inspected repository branches, not release tags or
verified current HEAD hashes; the web sources may be cached snapshots.

* [Legacy NVDA Remote transport](https://github.com/NVDARemote/NVDARemote/blob/master/addon/globalPlugins/remoteClient/transport.py)
  accumulates socket chunks, splits at LF, processes each completed nonempty
  record and preserves the unfinished suffix.
  [Its serializer](https://github.com/NVDARemote/NVDARemote/blob/master/addon/globalPlugins/remoteClient/serializer.py)
  emits `json.dumps` as UTF-8 followed by LF.
* [NVDA built-in Remote Access transport](https://github.com/nvaccess/nvda/blob/master/source/_remoteClient/transport.py)
  and [serializer](https://github.com/nvaccess/nvda/blob/master/source/_remoteClient/serializer.py)
  use the same record contract.
* [NVDA Remote Companion NetworkClient.cpp](https://github.com/gozaltech/NVDARemoteCompanion/blob/main/src/NetworkClient.cpp)
  appends LF when sending, accumulates received fragments, processes all
  LF-delimited records, strips a trailing CR and retains the incomplete suffix.

For these clients, the wire contract is newline-delimited JSON: LF is a record
boundary, not an interchangeable arbitrary-whitespace separator. JSON whitespace
inside a single physical line is allowed; pretty-printed multiline JSON is not
a single record. CRLF works (JSON accepts the trailing CR in Python; Companion
explicitly strips it). Multiple records in one socket read and one record split
across many reads are normal. No examined client depends on Go `json.Decoder`
value framing or sends multiline JSON. These source observations establish the
examined implementations, not a claim about every third-party client.

Historical local nvremoted commits `4258e5a` and `556965d` already use a streaming
Go JSON decoder, accepting arbitrary JSON whitespace, multiline values,
adjacent objects without LF and a complete object at EOF without LF. Thus the
server has a broader receive contract than the examined client senders.
This fix preserves that behavior rather than enforcing new line framing.
Ordinary outbound responses still use `json.Encoder` and end in LF.

## Baseline memory path

`readFromClient` used one connection-long `json.Decoder`, whose `Decode` reads
the whole value before unmarshaling. Its growing buffer retains unfinished
values, including huge strings or nested objects/arrays, before application
validation. Whitespace before a value can also accumulate in that buffer.
The buffer grows geometrically and can retain the connection's high-water
capacity after a large successful message. The standard JSON depth check
(10,000 levels) limits nesting depth, not total string size, element count or
object width.

`RawMessage.UnmarshalJSON` copies the full encoded value. The first
`json.Unmarshal` extracts `GenericClientMessage.Type`; another unmarshals the
same raw bytes to a known control struct or arbitrary `map[string]interface{}`.
Strings and keys allocate decoded storage; arrays, maps, interface values and
numbers add allocation overhead, especially for many tiny nested containers.
Unknown channel message types deliberately remain valid and keep their fields.
The generic type string may also be duplicated by the map decode.

Therefore the peak is not merely the wire length: decoder buffer, raw copy,
decoded payload and temporary allocations can coexist, including old buffers
during growth/GC. There is no useful constant “three copies” guarantee for
arbitrary maps. Before this fix, neither a finished value nor a never-finished
value had a byte bound. Sequential large values could retain a large decoder
buffer and accumulate live maps in event queues; simultaneous clients multiply
these costs.

Relevant primary Go sources:
[stream.go at Go 1.23.0](https://github.com/golang/go/blob/go1.23.0/src/encoding/json/stream.go),
[decode.go](https://github.com/golang/go/blob/go1.23.0/src/encoding/json/decode.go),
[scanner.go](https://github.com/golang/go/blob/go1.23.0/src/encoding/json/scanner.go).
`Decoder.Buffered` explicitly exposes bytes read beyond the requested value.

## Enforcement choice

| Candidate | Assessment |
| --- | --- |
| Bounded LF reader or separate line framing | Matches examined clients but rejects historical multiline/adjacent values. |
| One `LimitReader` for the connection | Incorrectly makes the limit cumulative across messages. |
| Reset a counting/limiting reader around a persistent decoder | Decoder read-ahead can charge next-message bytes to the wrong value; buffered bytes can bypass a reset budget. A post-Decode size check is too late. |
| Custom incremental JSON validator/framer | Can enforce before decode but duplicates JSON grammar and creates compatibility/maintenance risk. |
| Fresh bounded decoder with explicit read-ahead ownership | Uses the existing JSON grammar, preserves value framing and bounds every assembly attempt. Chosen. |

`messageReader` streams away inter-value JSON whitespace using a fixed buffered
reader. From the first non-whitespace byte it creates a fresh decoder over a
budget of L bytes plus one probe byte. The extra byte distinguishes an exact-L
value from an oversized or still-growing value. Two `LimitedReader`s avoid
integer overflow in L+1. No decoder can ingest more than L+1 value-attempt
bytes, even for an unfinished string. Reader calls are capped at 4096 bytes;
transport buffering/read-ahead uses fixed additional storage.

On success, the raw value length, not the bytes fetched, determines its size.
`Decoder.Buffered()` is copied back in front of any remaining unread prefix,
then the decoder is discarded. The copy is at most 4096 bytes, so a large
decoder is not kept for the next message. No recursive reader chain accumulates.
The next decoder starts from those saved bytes and gets an independent budget.
Internal whitespace counts; leading/trailing inter-value whitespace and LF do
not. Escapes and UTF-8 count as their encoded byte lengths, not character count.

A syntax error within the budget remains malformed even if data was prefetched.
A value needing byte L+1 is oversize, including incomplete input at that size.
EOF during an incomplete in-budget value remains `io.ErrUnexpectedEOF`; a
complete object at EOF remains accepted. Numeric scalar concatenation still
follows JSON grammar (two adjacent digit sequences are one number).

The encoded-storage cost is now O(L) plus fixed read-ahead and scanner storage.
The decoder's geometric capacity, RawMessage copy and GC transients still exist;
the setting is not an exact heap-byte budget. Accepted arbitrary maps may use
many times L in heap storage. Rejecting oversize happens before both protocol
Unmarshals and before any event submission. Sequential reads discard large
decoder buffers; accepted queued payloads and concurrent clients remain separate
sources of aggregate memory use.

## API and default

`Server.MaxMessageSize int` follows the existing Server resource settings.
Positive values specify encoded JSON value bytes. Zero and negative values use
4 MiB (4,194,304 bytes); there is no implicit unlimited mode. Configure before
serving, like other Server fields. It applies equally to control messages and
arbitrary channel types. No CLI/TOML option is added: safe zero-value behavior
solves the defect; library embedders can adjust unusually large workloads.

The actual clients exchange small key/control/tone messages, serialized speech
sequences, braille cell lists, clipboard text and wave notifications. See
[legacy protocol types](https://github.com/NVDARemote/NVDARemote/blob/master/addon/globalPlugins/remoteClient/protocol.py),
[current NVDA session](https://github.com/nvaccess/nvda/blob/master/source/_remoteClient/session.py)
and [clipboard producer](https://github.com/nvaccess/nvda/blob/master/source/_remoteClient/client.py).
Wave notifications are not bulk audio transfer. No protocol maximum for speech
or clipboard text was found. A finite limit cannot preserve arbitrarily large
valid clipboard content.

4 MiB is therefore an explicit relay policy, not a measured real-user maximum
or an NV Access requirement. It leaves room for more than 512 Ki BMP clipboard
characters even with six encoded bytes per character (3 MiB plus envelope),
or nearly four million plain ASCII characters, as well as ordinary speech
sequences and braille/control traffic. Supplementary characters may use twelve
escaped bytes per character. This provides substantial text headroom while
bounding allocation before validation. Larger legitimate text needs an explicit
API override. The bound counts the representation actually sent, regardless of
a client's serializer escaping policy.

Both inspected Python serializers call `json.dumps` without changing
[`ensure_ascii`, which defaults to true](https://docs.python.org/3.12/library/json.html#json.dump).
Thus the six-byte BMP estimate covers their non-ASCII clipboard representation,
rather than assuming all clients send unescaped UTF-8 text.

## Oversize lifecycle and independent limits

Oversize calls `c.stop("Incoming message too large")` immediately. The first
stop reason wins. Stop closes the transport (the underlying TLS transport first,
as already required by the lifecycle), wakes blocked I/O and allows reader and
handler completion, channel leave, registry removal and active-session removal.
No response is sent: an unread malicious peer could occupy the serialized write
path until its timeout and delay cleanup. The existing disconnect log records
the distinct reason without payload content. The stream is never resumed after
an oversize error, never silently truncated and never partially forwarded.

A client sending a never-completed value slowly while staying within L can
still retain its connection/goroutines. Size bounds memory, not connection
lifetime. A message-assembly timeout could start only after the first
non-whitespace value byte and end after a complete value; it would need a
separate compatibility policy for slow legitimate clipboard transfers. No
general read timeout or idle/application timeout is introduced here.

For outbound channel delivery, the map is shared across recipient queues;
each recipient creates a shallow response map and sets `origin`. The encoder
allocates a full wire buffer. JSON HTML escaping can expand `<`, `>` and `&`
from one byte to six; floating-point normalization, invalid UTF-8 replacement
and origin insertion can also change size. Therefore inbound L is not an
identical outbound wire limit, though payload-derived output and allocations
are bounded functions of L. A write deadline limits socket waiting, not encoder
allocation. Server-generated MOTD/stats/member lists have independent sources
and may exceed L. A separate outbound byte mechanism is unnecessary for this
single-input defect and is not added for symmetry.

Follow-ups: total active-client admission/resource budgets, event queue byte
budgets and fan-out memory/CPU amplification, independent server-generated
response sizes, optional message-assembly lifetime policy, and the examined
clients' own unbounded unterminated LF receive buffers. None is changed here.

## Regression evidence

Before enforcement, small configured-limit tests failed for complete arbitrary
messages, complete known controls and an unfinished growing string; historical
stream compatibility passed. No hundreds-of-megabytes allocation was needed.

Tests cover below/exact/one-byte-over limits; strings, nested arrays/objects,
UTF-8/escapes and multiline internal whitespace; one-byte/half/whole input
reads; multiple values per input read, adjacent objects and CRLF/blank
separators; EOF and malformed JSON; sequential allowed messages with a total
larger than L; following oversize and read-ahead recovery after a 9 KB value;
bounded underlying reads; long streamed whitespace; zero/negative/max-int
configuration; in-budget unfinished assembly without deadlines; arbitrary and
known control decoding; unread offenders, concurrent offenders, first reason,
no partial/oversize forwarding, healthy relay after disconnect, last-member
channel destruction and unjoined active-session cleanup.

Full normal and race suites, 30 repetitions of message-limit regressions under
race detection, `go vet ./...` and `mage build` passed on Windows. The application
binary was never run. Server tests also cross-compile for Linux/386,
Darwin/arm64 and Plan9/amd64.
