# Availability-preserving admission

Admission is relay policy, not an NVDA Remote protocol limit. Configure the
single-run Server before use. Existing TOML files keep decoding; finite limits
are enabled by default. Large existing deployments must size them explicitly.
There is no unlimited mode. The relay never authenticates endpoint identity.

## Capacity arithmetic

`P` counts every owned connection before successful admission, including TLS,
startup, admission processing, rejection, and their cleanup. `A` counts admitted
connections through their full cleanup. Transferring P to A and issuing channel
credits happens under one capacity mutex before channel membership changes:

```
P <= pendingLimit
A <= admittedLimit
P + A <= hardLimit
TLS <= tlsLimit
unjoined startup <= startupLimit
hardLimit >= pendingLimit + admittedLimit
```

The serial acceptor waits for pending capacity before accepting another socket.
Kernel backlog connections are outside server ownership. A socket returned by
Accept occupies the available pending slot before client queues or workers are
allocated. TLS/startup sublimit refusals close it before protocol processing.
There is no socket write or cleanup under the capacity, lifecycle or registry
locks. Physical slots release after readers/handlers/watchdogs, channel leave,
transport close and disconnect logging finish, never when stop is requested.

Let `G` be ordinary channel obligations (active and unexpired offline), `K`
configured channels, and `E` best-effort extra members. Committed admitted
capacity is `C = 2G + 3K + E <= admittedLimit`. Each generic obligation backs
one master and one slave; configured obligations also back one temporary
overlap. For each channel extras initially count `max(masters-1,0) +
max(slaves-1,0) + unknownRoles`; a currently used configured overlap subtracts
one duplicate-role member from extras. Credits are channel obligations, not
individual client identities. Counts include provisional admissions and cleanup
until physical release. Complementary roles consume already committed credits.
When an incumbent leaves, a same-role extra can occupy its base credit.

Every new ordinary channel, even one first entered by an unknown role, needs
the pair obligation. An unknown member additionally needs an ordinary credit;
it never consumes a master/slave credit. Existing multi-master/multi-slave
channels remain supported within best-effort headroom. There is no per-channel
member cap apart from these global bounds. New channels and ordinary extras
cannot borrow offline/configured/complementary credits.

## Recovery and protected configuration

After the last successfully admitted member completes cleanup, its channel
worker, sockets and clients are destroyed. Only a digest, counts and expiration
metadata remain. Both pair credits stay committed for `recoveryGrace` (default
60s). One shared expiration timer releases expired obligations exactly once;
admission also expires overdue entries under the same mutex. Equality at the
deadline means expired. Recovery inside grace preserves the obligation; refused
attempts never renew it. Reservations have no LRU eviction and their count is
bounded by issued credits (at most admittedLimit/2). Repeated successfully
admitted reconnects may legitimately create another grace period on disconnect.
Generic reservations are in memory and disappear on restart.

Configured channels reserve three credits at startup and keep them while
offline without expiry. Startup rejects inconsistent limits, malformed or
duplicate digests, negative API settings, or configured credits exceeding
admitted capacity. The CLI also rejects zero/non-integer limits, non-duration
strings and unknown admission options. API zeros mean defaults.

`protectedChannels` contains hex SHA-256 of the exact UTF-8 channel string,
not plaintext keys. Hex letter case is immaterial; the channel string is never
trimmed, folded, or Unicode normalized. Use high-entropy channel keys: a digest
of a weak password enables offline guessing. Digests are sensitive verifier
material; protect configuration access. They are not logged or stats labels.
For example, locally compute a digest in PowerShell 7 (avoid recording the key
in shell history; Read-Host -AsSecureString is useful for interactive entry):

```powershell
$channelSecret = Read-Host -AsSecureString 'Exact channel key'
$channelBytes = [Text.Encoding]::UTF8.GetBytes(
    [Net.NetworkCredential]::new('', $channelSecret).Password)
[Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($channelBytes)).ToLowerInvariant()
[Array]::Clear($channelBytes)
```

The third configured credit is a single duplicate-role overlap across both
roles. Ordinary headroom is used first for permanent extra members; otherwise
one reconnect can borrow overlap for `overlapTimeout` (10s), measured from
admission. Inputs/repeated joins do not renew it. If the incumbent completes
cleanup and the overlap holder fits the pair, that holder becomes a base member
and overlap becomes available again. If a duplicate remains at expiry, the
later admitted overlap holder is stopped; the credit stays occupied through
cleanup. The server cannot safely decide which device is the ghost. An attacker
knowing the same key can impersonate a role, occupy pair/overlap, or race the
legitimate reconnect. No IP-based identity inference is used.

## Startup and refusal

TLS establishment retains the existing independent 10s handshake timeout.
After TLS (or immediately for plain TCP), an absolute `startupTimeout` (10s)
allows join with or without protocol_version, or the existing `stat` service
request. Whitespace, fragmented bytes, repeated protocol_version and arbitrary
input do not renew it. Joined idle clients have no admission/idle deadline.
There is no supported key-generation/service request beyond `stat`; unknown
requests preserve the existing protocol-error behavior before rejection.

A channel-aware capacity refusal enters a bounded rejection state in P. It
never adds a member, sends channel_joined, or causes fake join/leave events.
A master receives immediately, separately from the unchanged MOTD path:

```json
{"type":"speak","sequence":["Server is at capacity. Existing remote sessions are protected. Please try again later."]}
```

No origin is required. An existing error record with `error: "server at capacity"`
also supports protocol tools; it is not the speech mechanism. Controlled/slave
clients do not register inbound speech playback, so they get the error and
server diagnostics without a false successful join.

The absolute rejection window defaults to 5s; periodic speech defaults to 1s.
Configuration requires interval >= 4ns and interval <= window, preserving a
nonzero derived minimum even for unusually small operator-selected intervals.
Any incoming bytes, including malformed JSON or whitespace, coalesce into one
pending input signal. Input can trigger another speech once interval/4 has
elapsed since the last announcement (250ms by default). Periodic eligibility
stays on its original schedule; when it falls within minimum spacing, it waits
until eligible. Input never moves the deadline. There are at most
`ceil(window/(interval/4))` speech attempts before expiry (20 defaults), plus
one error attempt. The input mailbox and raw drain buffer are bounded. Writes
use the existing serialized encoder and per-write timeout clipped to the
absolute rejection deadline. Write failure ends the connection immediately.

MOTD is sent exactly as before at handler startup, before requests, with the
same text and force_display=false. Admission never modifies MOTD/config/cache;
accepted and rejected peers receive the same independent MOTD. Speech refusal
does not invoke, replace or force-display it.

## Limits of the guarantee

Protection applies after a connection reaches channel-aware admission. Until
join, the key is unknown: pending/TLS exhaustion, full hard physical capacity,
OS backlog/descriptor limits or the network may prevent even a configured
channel from reaching join and speech refusal. This is explicitly not a
volumetric/L4 DDoS guarantee, traffic fairness, endpoint authentication or
survival across server restart. Successfully admitted attacker channels are
allowed their own backed pair and finite grace; they can exhaust capacity for
new obligations but cannot cancel older obligations. Unbounded attackers can
keep pending capacity busy; finite timeouts bound each connection, not their
aggregate rate. Existing healthy channels continue relay subject to existing
queue/backpressure, message size, transport and operating-system limits.

## Observability and synchronization

Existing stats fields retain their semantics. Additive `stats.admission` contains
physical/current admitted counts, TLS/startup/admission/rejection/cleanup stages,
active entitlement channels, ordinary extras, total guaranteed credits, offline
generic/configured reservations, overlap usage, fixed-reason reject counters,
startup timeouts, recovery admissions, and physical/pending/admitted high-water
marks. Counters contain no key, digest, IP or arbitrary reason labels.

Lock order is lifecycle -> registry -> capacity during initialization, and
registry -> capacity during join/stats. Capacity never acquires other locks or
does I/O, logs, stops clients or waits. Channel workers keep existing registry
and pendingJoins ordering. Timeout, provisional admission, expiration/recovery,
release and shutdown fencing serialize under capacity.mu. Watchdogs are owned
by the client supervisor and waited before final cleanup; the shared expiration
timer belongs to Serve and stops on exit. Failed admission rolls back credits;
successful membership owns its credits through cleanup. Shutdown fences new
admission, closes transports and drains clients/workers before clearing offline
reservations. Generic reservations are not persisted.

## Protocol evidence and verification

[Legacy MasterSession](https://github.com/NVDARemote/NVDARemote/blob/master/addon/globalPlugins/remoteClient/session.py)
and [built-in LeaderSession](https://github.com/nvaccess/nvda/blob/master/source/_remoteClient/session.py)
dispatch `speak(sequence, priority)` without membership/origin prerequisites;
[LocalMachine](https://github.com/nvaccess/nvda/blob/master/source/_remoteClient/localMachine.py)
queues playback. Strings are valid sequence elements. The corresponding
SlaveSession/FollowerSession does not register inbound speak.
[Companion ConnectionManager::HandleIncomingMessage](https://github.com/gozaltech/NVDARemoteCompanion/blob/main/src/ConnectionManager.cpp)
registers its receiver before sending startup requests and dispatches `speak`
by type without consulting joined/handshake state or origin. Its handler
extracts nonempty string elements from the sequence array and calls Speech::Speak
when local speech/profile settings permit playback. Thus the minimal direct
wire shape also reaches Companion's controller speech path before channel_joined.
Muted clients, disabled speech engines, or a client closing its own startup
timeout early cannot be made audible or kept connected by the relay.
[Legacy transport](https://github.com/NVDARemote/NVDARemote/blob/master/addon/globalPlugins/remoteClient/transport.py)
and [built-in transport](https://github.com/nvaccess/nvda/blob/master/source/_remoteClient/transport.py)
retry about every 5s. Thus 60s grace spans roughly 12 retries; it is a conservative,
configurable policy, not an outage-recovery guarantee. See the existing
[framing evidence](message-limit.md) for LF JSON and Companion serialization.

Tests cover deterministic credit accounting, provisional rollback, concurrent
last-credit joins, complement/extra races, recovery/expiry, configured overlap,
exact hashing, validation/overflow, deadlines, stage/physical limits and
idempotent cleanup. Wire tests cover direct speech, repeat/coalescing/flood
bounds, forced close, malformed input, no membership side effects, unchanged
MOTD, startup paths, unread peers/write failure and healthy relay under overload.
An end-to-end Serve/Shutdown test drains a blocked rejection writer, stalled TLS,
an admitted channel and offline grace together.
Real screen-reader audible playback, keyboard cancellation and host speech
interruption require manual verification on legacy NVDA, built-in Remote and
Companion; wire fixtures verify JSON framing and response shape, not actual
client dispatcher execution or audio/UI timing.
