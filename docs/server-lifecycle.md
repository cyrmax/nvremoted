# Server lifecycle

## Baseline investigation

Before this change, `Serve` initialized the registry, launched an accept
goroutine, and ran periodic ping dispatch in its own select loop. Closing the
listener or a terminal accept error ended admission, returned a wrapped accept
error, and stopped the ping ticker. There was no separate ping goroutine.
Known resource failures retried with capped backoff.

Accepted clients were independent of the serving loop: each had a lifecycle
goroutine, then (after TLS establishment) a reader and a handler. Only those
three goroutines arranged their cleanup. No server-level mechanism stopped
them or waited for them. Idle clients and unfinished TLS handshakes were absent
from the protocol registry, which counted only channel members. Channel
goroutines ended when their last member left without pending joins.

Consequently, returning from `Serve` meant that admission and periodic pings
had ended, but sessions could still join channels, relay messages, and request
stats. The server instance still owned their state without exposing a way for
the caller to end their lifetime. Neither wrapper added session cleanup:
`ListenAndServe` and `ListenAndServeTLS` only closed their internally created
listener on return. `Serve` left its caller-owned listener alone.

A second `Serve` replaced the entire registry, including its mutex, maps,
creation time, historical maxima, and counters. Old clients retained a pointer
to that same registry field. They could access the new maps through the replaced
mutex, race with initialization, delete new clients with reused numeric IDs,
or remove a new channel with the same name while leaving an old channel. Old
channel goroutines also retained that pointer. Sequential calls were therefore
unsafe while sessions survived the first run; concurrent calls additionally
left two accept and ping loops sharing state. The first ping ticker did stop
when its `Serve` returned, but its sessions did not.

## Chosen model and API

`Server` is a single-run object. Configuration is immutable during use; the
instance must not be copied. A serving method reserves the run before creating
listeners or loading TLS configuration. Failed starts consume the instance.
Second or concurrent calls to `Serve`, `ListenAndServe`, or `ListenAndServeTLS`
return `ErrServerUsed` before touching another listener, configuration, or
registry. Shutdown of an unused instance also prevents future serving.

Restart means constructing another instance after the old one has completed
cleanup. There is no registry reset between runs. The first run initializes
creation time and statistics under the registry lock; shutdown preserves
historical maxima and creation time while removing current memberships and
the E2E channel count.

The sole new operation is `Shutdown(ctx context.Context) error`. It:

1. Marks the instance as stopping, rejects new admission, and cancels accept
   retry waits and future periodic delivery.
2. Closes the active listener to unblock `Accept` and waits for admission to end.
3. Stops every accepted client in parallel, closing its transport immediately.
4. Waits for all client cleanup, channel workers, and disconnect logging.
5. Publishes completion to every shutdown caller.

Relay sessions may be idle indefinitely. There is no request boundary or
finite transaction to drain; shutdown disconnects peers instead of waiting
for voluntary departure or flushing queued messages. This is graceful server
cleanup, not a protocol drain or a new backpressure policy. Socket close already
unblocks pending reads and writes, and TLS stop closes the underlying transport
before TLS close to avoid close-notify stalls.

Internal cancellation belongs to the server rather than to a particular caller.
The shutdown context limits only that caller's wait for completion. Deadline or
cancellation returns `ctx.Err()` without reversing shutdown or abandoning its
cleanup. Transports are closed as part of shutdown without waiting for the
deadline; expiry does not introduce a second force-close phase. A later call
can wait again. Completed calls are idempotent and prefer the completed result
even if their context is already canceled.

A separate `Close` would duplicate the same immediate transport-stop action
with a different wait contract. A context on `Serve` would change three serving
entry points without adding a necessary guarantee. Safe reuse would require
per-run state separation and reset rules for stats, sessions, and configuration;
explicit rejection is the smaller useful model.

Applications need stopped admission and complete client cleanup before normal
exit. Restart and tests additionally need completion of all server-owned work
before replacing resources or asserting final state. The API gives that same
completion guarantee to all three. Reuse of the same instance is rejected.

## Ownership and completion

`Serve` callers own their listener and close it after an accept failure. Merely
returning an accept error does not close that listener. Explicit `Shutdown`
authorizes closing the active listener, as necessary to unblock `Accept`.
Both listen wrappers own their listener and defer its close on every exit.

The server owns every successfully admitted session, including pre-join idle
clients and unfinished TLS handshakes. An accepted connection arriving after
the stop flag is set is closed without launching session goroutines. A separate
lifecycle map tracks admitted `*client` objects; the protocol registry keeps its
existing membership/stats meaning.

The completion path is:

`stop -> transport close -> read/write unblock -> reader and handler finish ->
leave -> channel and registry removal -> disconnect log -> lifecycle removal`.

The existing stop operation remains idempotent and preserves the first reason.
There is one client cleanup owner. Join may already be in progress when stop
arrives; its handler still completes membership bookkeeping and then leaves.
Existing bounded enqueue prevents join/leave notifications from stalling on
unread event queues. No event queues are closed and no new queue policy is added.

The server waits for stop workers, client lifecycle workers (which wait for
reader, handler, and leave acknowledgement), and channel workers. The latter
wait closes the small gap between leave acknowledgement and channel worker
return. Completion is signaled only after disconnect logs have finished.
Ticker stop runs before session cleanup. Neither `Serve` nor a successful
`Shutdown` leaves server-owned work running. An unsuccessful second serving
call makes no such guarantee about the already active first run.

Custom listener/connection implementations must make `Close` unblock their
operations, and logging hooks must finish. A context bounds shutdown caller
waiting even when custom operations or hooks delay cleanup; it cannot forcibly
terminate arbitrary Go code. Independent client stop workers ensure that a
slow close on one transport does not delay closing the others.

## Synchronization

The lifecycle mutex protects run reservation, stop state, completion channel
initialization, and the active map. Client `WaitGroup.Add` and admission's stop
check occur together under this mutex, before any client worker is launched.
The accept loop ends before final cleanup begins, and stopping prevents further
admission. There can be no new client additions during `Wait`.

Channel worker counts are incremented before launch, under the registry lock.
The server waits for all clients before waiting for channels, so no handler
can start another channel while the channel worker count is being waited on.
Shutdown uses no polling or sleeps. Resource retry retains the original delay
policy, with a timer that can now be interrupted by internal cancellation.

Run initialization takes lifecycle then registry locks before any run workers
exist. Normal active-map operations hold only the lifecycle lock. Shutdown
snapshots the map and releases that lock before stopping or waiting. Client
cleanup releases channel/registry synchronization before lifecycle removal.
Existing registry -> pending-join lock ordering is preserved. Stop releases its
own stop lock before closing sockets. No new path holds a registry, lifecycle,
or pending-join lock while invoking client stop or waiting for workers.
The stop reason is read under its lock for disconnect logging. The completion
channel publishes the final listener-close result to shutdown callers.

Ping dispatch still snapshots under the registry read lock and enqueues outside
it. The serving loop checks cancellation before dispatch; snapshot delivery
also checks it between recipients. An enqueue already in progress can finish
after cancellation, using the baseline bounded queue/stop guarantees. There is
no new periodic work after the serving loop exits and no retained ticker or
dispatch after completion. Accept failure takes the same final cleanup path.

## Errors

| Event | Serving method | Shutdown |
| --- | --- | --- |
| Explicit shutdown | `nil` after cleanup | `nil` after completion |
| Caller closes listener | Wrapped `net.ErrClosed` after cleanup | `nil` once completed |
| Terminal accept failure | Original wrapped accept error after cleanup | `nil` once completed |
| Accept failure races with shutdown | Terminal errors are preserved; expected close during shutdown returns `nil` | Completion result |
| Shutdown context expires/cancels | Serving/cleanup continues | `ctx.Err()`; may wait again |
| Shutdown listener close fails | Expected accept close still returns `nil` | Wrapped close error after cleanup, retained for later calls |
| Client close fails | Existing handling; not aggregated | Existing handling; not aggregated |
| Repeated/concurrent serving method | `ErrServerUsed` immediately | First run is unaffected |
| Listen or TLS setup fails | Wrapped setup error; instance consumed | Completion result |

As before, a client send interrupted by transport close can emit the existing
send warning. It cannot replace an already recorded shutdown reason. Expected
listener closure is debug-level; graceful server shutdown does not manufacture
a fatal serving error.

## Regression coverage and follow-ups

Tests first reproduced early `Serve` return with an idle client and unsafe
second serving admission. Lifecycle regressions cover empty shutdown,
pre-start shutdown, idle TCP, established TLS, silent TLS handshake, unread
peer writes, multiple clients across ordinary/E2E channels, concurrent callers,
context timeout/cancellation with cleanup/log barriers, in-progress join and
leave, snapshot dispatch and actual periodic ping, late accepts and terminal
errors, registry cleanup, listener ownership/close errors, client close errors,
first stop reason, independent closes, failed starts, and reuse rejection.
Completion barriers and worker synchronization verify finished owned work
without global goroutine-count guesses or sleep-based polling.

The CLI now owns startup cancellation, its listener and the signal-driven
`Shutdown` caller; see [CLI termination lifecycle](cli-lifecycle.md) for the
process budget, error precedence and exit policy. Existing send warnings
during intentional stop may still deserve a separate logging change. Neither
the CLI integration nor that follow-up changes these API guarantees.
