NVRemoted is an implementation of the [NVDA Remote][] server in Go.

Local performance tooling is available through `mage bench`, `mage benchLong`
and dedicated profiling/comparison targets. It measures component costs and
real localhost TCP/TLS relay latency, delivery, saturation and lifecycle resources.
Performance runs are excluded from CI. See the [benchmark guide](docs/benchmarks.md).

Admission control is enabled with finite server policy defaults: 1024 admitted
credits, 128 pending connections and a 1152 hard physical ceiling, including
cleanup. Every accepted ordinary channel has backed capacity for one master
and one slave; extra members are best effort. Offline ordinary channels retain
their pair for 60 seconds after cleanup. Operators can configure permanent
protected channels by SHA-256 digest, including one bounded reconnect overlap.
Channel-aware capacity refusals speak a separate explanation to controllers
for up to 5 seconds; existing MOTD semantics stay unchanged. Protection cannot
guarantee passage through an exhausted TCP/TLS/pre-join path or prevent L4 DDoS.
Old TOML files keep working with these finite defaults; larger installations
should size the new `[admission]` section explicitly. See
[configuration, arithmetic and security boundaries](docs/admission-control.md)
and [the example](examples/nvremoted.toml).

Incoming JSON values are limited to 4 MiB before protocol decoding or relay.
Oversize immediately closes the connection with the logged reason
`Incoming message too large`, without a protocol response or partial forwarding.
Go API users can set `server.Server.MaxMessageSize` before serving; zero and
negative values select the safe default, never unlimited input. The count
includes all encoded bytes inside a value, including internal whitespace and
JSON escapes, but excludes whitespace between values and the terminating LF.
The CLI uses the default without new flags or TOML keys. This is a relay policy,
not an NVDA clipboard limit; exceptionally large clipboard/speech payloads may
need a larger API setting. Fragmented messages and historical JSON stream
framing remain supported. See [the message limit design](docs/message-limit.md)
for protocol evidence, buffering, memory implications and remaining limits.

`server.timeBetweenPings` controls server ping delivery only (seconds; 0 disables
pings). NVDA Remote clients do not acknowledge these pings, so valid idle
connections are allowed. `server.pingsUntilTimeout` and `--pings-until-timeout`
are deprecated and ignored, including existing nonzero values. They no longer
impose an inactivity timeout.

TCP keepalive is enabled for both TCP and TLS connections independently of
these settings, with a 15-second idle period. Probe intervals, retry counts,
and the time to detect a lost peer depend on the listener and operating system;
this is not a 15-second disconnect timeout. Transport errors and disconnects
stop the client and remove its channel membership. Keepalive cannot detect a
stalled client application while its operating system still responds to TCP.

Each client has a bounded FIFO queue of 64 pending server events, in addition
to the event being handled. A full queue disconnects that client rather than
dropping or coalescing messages on a continuing connection. Each response gets
a 10-second socket write deadline; a timeout or write error stops the connection
and prevents further protocol writes. These limits are independent of ping and
TCP keepalive settings. The write deadline does not bound reads during an
unfinished TLS handshake. TLS session establishment has its own 10-second
timeout for the entire handshake, before any protocol reads or writes. Each
handshake runs independently of the accept loop and other clients. A failed or
timed out handshake closes the connection; a successful handshake cancels its
timer without leaving a socket deadline or limiting application idle time.

Go API users can set `server.Server.EventQueueSize` and
`server.Server.WriteTimeout` before serving clients; nonpositive values use
64 and 10 seconds, respectively. The CLI uses these defaults; there are no
CLI flags or TOML keys for these limits. `server.Server.TLSHandshakeTimeout`
can independently override the handshake timeout; nonpositive values use
10 seconds. It applies only to TLS connections, including a TLS listener passed
to `Serve`, and does not affect plain TCP connections.

Connection logs use the peer IP address in `remote_host`. Admission performs no
reverse DNS queries, so slow or unavailable DNS cannot delay other clients or
the start of the TLS handshake timeout.

When admission ends outside explicit shutdown, `Serve` returns the accept
error; `ListenAndServe` and `ListenAndServeTLS` propagate that error and close
their owned listener.
Closure is detectable with `errors.Is(err, net.ErrClosed)` and is logged only
at debug level. Known descriptor/buffer/memory exhaustion errors are retried
with exponential delays from 5 milliseconds to 1 second, reset after a success.
Only the first failure in each consecutive resource exhaustion series is logged
as a warning. Closing the listener during a retry wait is observed on the next
accept, after at most 1 second. Other accept failures, including listener
timeouts, are logged once and returned. Retries do not use the deprecated
`net.Error.Temporary` classification. Standard TCP listeners handle interrupted
and aborted accepts internally.

`Server` is a single-run object: configure it before use and create another
instance for a restart. A second or concurrent call to any serving method
returns `server.ErrServerUsed`, including after a failed start. Calling
`Shutdown` before serving also permanently closes the unused instance.

`Shutdown(ctx)` stops admission and periodic pings, closes the listener and all
accepted transports (including idle connections and unfinished TLS handshakes),
and waits for client/channel cleanup and disconnect logs. It closes sessions
immediately rather than waiting for peers to leave or flushing event queues.
The context limits only the caller's wait: cancellation returns `ctx.Err()`
while teardown continues. Call `Shutdown` again to wait for completion.
Completed calls return the same listener-close error, or nil; client close
errors retain their existing handling and are not aggregated. Custom transports
and logging hooks must cooperate with teardown.

`Serve` also closes and waits for all accepted sessions when acceptance ends,
and stops its ping ticker before returning. Explicit shutdown returns nil;
external listener closure returns a wrapped `net.ErrClosed`; terminal accept
failures retain their wrapped error, even when racing with shutdown. In-flight
ping snapshots check cancellation between recipients; a delivery already in
progress can finish during shutdown, but none remains after completion.

A caller of `Serve` owns its listener and must close it after an accept failure.
`Serve` does not close it merely because `Accept` failed. Calling `Shutdown`
explicitly authorizes closing the active listener to unblock `Accept`.
`ListenAndServe` and `ListenAndServeTLS` close their internally created listener
on every exit. See [the lifecycle design](docs/server-lifecycle.md) for ownership,
synchronization, and error guarantees.

To use:

* `go install github.com/n0ot/nvremoted/cmd/nvremoted`
* Create a directory, $HOME/.config/nvremoted, and copy examples/nvremoted.toml there.
* Open $HOME/.config/nvremoted/nvremoted.toml, and follow the instructions in the file.
* As NVDA Remote only uses TLS, you need to point NVRemoted at a certificate and private key.
    Self signed certificates can be generated with openssl,
    and signed certificates can be gotten from [Let's Encrypt][].
* Run `nvremoted start`

#### Development and builds

This repository is the independently maintained `cyrmax/nvremoted` fork.
Use Go 1.23 or newer; CI's pinned stable toolchain is in `.go-version`.
Install [Mage][] at the version pinned in `go.mod`:

```sh
go install github.com/magefile/mage@v1.17.2
```

Mage is the shared local/CI development contract:

| Command | Purpose |
| --- | --- |
| `mage build` (or `mage`) | Build for the current host OS and architecture |
| `mage test` | Run application and Mage contract tests 100 times |
| `mage testRace` | Run the same tests 100 times with the race detector |
| `mage check` | Check Go formatting, module tidiness and `go vet`, without edits |
| `mage buildAll` | Build the full six-platform distribution matrix |
| `mage clean` | Remove generated `dist` and legacy `bin` output |

Platform targets are `mage buildLinuxAmd64`, `mage buildLinuxArm64`,
`mage buildWindowsAmd64`, `mage buildWindowsArm64`, `mage buildDarwinAmd64`
and `mage buildDarwinArm64`. Output is
`dist/<linux|windows|darwin>-<amd64|arm64>/nvremoted`, with `.exe` for Windows
and a sibling `.sha256` checksum. Builds preserve Git-derived version embedding.
Host builds always select the host, even if `GOOS`/`GOARCH` are set externally.

Builds and ordinary tests disable CGO; race tests enable it. Supply any
machine-specific compiler environment outside Mage. The separate
[Windows race setup](docs/build-ci.md#windows-race-environment) explains LLVM and
the `PATH`/`Path` normalization caveat. For a short diagnostic run, set
`NVREMOTED_TEST_COUNT=1`; the default remains 100 for both test targets.
Fix formatting and module drift with standard `gofmt` and `go mod tidy` commands.

CI invokes these same Mage targets. Download platform artifacts from a
[successful GitHub Actions CI run](https://github.com/cyrmax/nvremoted/actions/workflows/ci.yml).
Build/upload jobs require successful check, test and race jobs. Artifacts contain
the binary and its SHA-256 checksum and are retained for 21 days. Choose a run
whose overall status is **success**, so all six builds and uploads have completed.
After extracting a Unix artifact, set its executable bit (`chmod +x nvremoted`);
GitHub's ZIP artifact format does not preserve that permission.
See [build and CI design](docs/build-ci.md) for the contract and verification details.

[NVDA Remote]: https://www.nvdaremote.com
[Let's Encrypt]: https://letsencrypt.org
[Mage]: https://github.com/magefile/mage
