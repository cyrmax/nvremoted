# CLI termination lifecycle

## Investigation and policy (before implementation)

The baseline at `fe1b86e` is `main -> commands.Execute -> Cobra.Execute ->
Cobra.OnInitialize(initConfig) -> runServer -> logging/MOTD -> Server
construction -> ListenAndServe[ TLS ] -> Serve -> log.Fatal -> os.Exit(1)`.
Config/home errors also call `os.Exit(1)`. The TLS wrapper loads the key pair,
then listens; TCP listens directly. Both wrappers own their listener. `Serve`
owns admission, pings, sessions and cleanup, and preserves terminal accept
errors. `Shutdown(ctx)` stops admission, closes transports and waits for that
cleanup; expiration bounds the caller, not the teardown workers. There is no
CLI signal handling. Default signal termination bypasses deferred cleanup,
and even a nil serving result is logged fatal.

Sources checked on 2026-10-04:

- [Go os/signal](https://pkg.go.dev/os/signal): SIGINT normally comes from the
  terminal interrupt character (usually Ctrl+C); Go defaults SIGINT/SIGTERM to
  process termination. `Notify` diverts them; `NotifyContext` cancels once and
  continues diverting subsequent signals until stop. SIGKILL cannot be caught.
  Buffered signal delivery is required with an explicit channel; rapid Unix
  signals need not be counted reliably. A first interactive interrupt should
  promptly stop admission and unblock connections, then finish cleanup.
- [POSIX shell, section 2.8](https://pubs.opengroup.org/onlinepubs/9799919799/utilities/V3_chap02.html)
  distinguishes signal death (shell status greater than 128, encoding unspecified)
  from ordinary exit. `128+signal` is a common shell convention, not a portable
  obligation for an application that catches the request and exits normally.
- [Go os.Exit](https://pkg.go.dev/os#Exit): zero denotes success, nonzero failure;
  portable codes are 0..125, and Exit does not execute defers.
- Upstream [systemd.kill](https://github.com/systemd/systemd/blob/main/man/systemd.kill.xml),
  [systemd.service](https://github.com/systemd/systemd/blob/main/man/systemd.service.xml)
  and [manager defaults](https://github.com/systemd/systemd/blob/main/man/systemd-system.conf.xml):
  absent overriding configuration, stop uses SIGTERM (and SIGCONT), then
  SIGKILL for remaining processes after TimeoutStopSec. KillMode defaults to
  control-group. The manager's DefaultTimeoutStopSec defaults to 90 seconds;
  unit/manager settings and stop commands affect the actual budget. This is
  not an nvremoted guarantee. Ordinary exit 0 is clean. A numeric exit 143 is
  a nonzero exit unless SuccessExitStatus says otherwise; a process actually
  killed by SIGTERM is treated differently. An explicitly requested stop does
  not cause automatic restart. No systemd unit or notification protocol changes.
- Go represents Windows Ctrl+C/Ctrl+Break as `os.Interrupt`. Console close,
  logoff and shutdown events may be delivered as `syscall.SIGTERM`, but Go
  cannot prevent their subsequent OS termination. [Microsoft HandlerRoutine](https://learn.microsoft.com/en-us/windows/console/handlerroutine)
  documents event-dependent OS budgets (e.g. console close usually 5 seconds,
  some events much shorter), unlike Ctrl+C/Break. Unix signal sending/death
  statuses are not a Windows contract. This CLI is not a Windows service.
- [Go ListenConfig.Listen](https://pkg.go.dev/net#ListenConfig.Listen): the
  context controls address resolution, not the lifetime of the resulting listener.

## Explicit invariants

1. Register supported signals as the first operation in `main`, before Cobra,
   config or logging, and retain interception until process exit. Before main
   (runtime/package initialization), Go's defaults apply; no server resources
   exist. The first delivered request latches cancellation. Repeated requests
   are idempotent, cannot restart startup or extend the deadline, and create no
   log spam. No self-kill or second-signal `os.Exit` path.
2. Check cancellation before/after config, MOTD preparation, construction,
   TLS loading and listen. A successful step cannot authorize the next step
   after observed cancellation. A listener returned concurrently with stop is
   still owned and closed by the CLI, including the gap before Serve starts.
3. A newly constructed Server belongs to this invocation only. If shutdown
   wins its reservation race with Serve, its ErrServerUsed is the expected
   pre-start stop result, not a reuse attempt. Other startup/serve errors remain
   failures. No change to library lifecycle or wrapper semantics.
4. While serving, use Shutdown with a fresh context, independent of the already
   cancelled startup context. Library admission stops when Shutdown latches
   stop; existing sessions, stalled TLS and blocked writes are closed by the
   existing lifecycle. Successful CLI return waits for Serve and owned cleanup.
5. The process coordinator allows at most 10 seconds after observing cancellation
   for the entire command, including blocking config/TLS filesystem reads,
   logging, listen and shutdown. Normal returns run command defers first. At
   expiration it reports failure and returns to the sole main exit boundary;
   still-blocked Go work is abandoned by process exit. Go cannot forcibly cancel
   arbitrary filesystem calls or logging hooks. A worker that later resumes
   checks cancellation and cannot continue startup. This budget is application
   policy, independent of systemd/Windows budgets, which may kill it sooner.
   Final stderr error reporting has a separate maximum 100ms best-effort budget
   before main exits: even a full stderr pipe cannot hold the process alive.
   A blocked diagnostic may be incomplete; exit status remains authoritative.
6. Successful completion, successful controlled SIGINT/SIGTERM shutdown, and
   cancellation before server creation all exit 0 on every platform. Real
   startup/serve/close/shutdown errors and incomplete cleanup exit 1. We choose
   ordinary success for a fulfilled stop request, including interactive SIGINT;
   callers needing actual Unix signal death must not infer it from this status.
7. Priority is real startup/serve error, then cleanup error/timeout, then clean
   completion/stop. Preserve multiple known errors together, with the serving
   error first. Never suppress an error merely because the context is cancelled;
   only explicit startup stop and causal listen cancellation are expected.
   When result and signal are both ready, always consume the result. At the
   deadline, an already published complete result wins; otherwise timeout wins.
   Errors hidden inside a still-blocked operation cannot be reported before it
   returns; timeout must not falsely report successful cleanup.
8. Config errors propagate through Cobra's error-capable pre-run hook; start
   uses RunE. Expected listener closure from Shutdown is handled by Server.
   Controlled server stop logs once at info; startup failures reach stderr as
   errors, never Fatal. Library diagnostics remain unchanged.

`NotifyContext` is sufficient specifically because requests deliberately share
one path/status and repeats are idempotent. An explicit signal channel would
only be necessary to distinguish signal identities or implement escalation.
The library already immediately closes all transports; cancelling its wait
does not accelerate cleanup. Restoring defaults on the second signal would
bypass cleanup and has different Unix/Windows behavior, so that policy is not
introduced. A distinct supervision/escalation design remains a follow-up if
users need a second-key emergency exit before the bounded deadline.

## Stage/race behavior

| Stage/event | Outcome |
| --- | --- |
| Before command/config/Server/listen | Stop check ends startup, no serving |
| During config/MOTD/TLS read | Observe stop on return; preserve real required-read error; outer budget bounds process waiting |
| Signal plus bind error | Real bind error wins; causal context cancellation alone is clean |
| Bind succeeds with stop | Close returned listener; no later startup stage |
| Listener ready, Serve not reserved | Stop check or Shutdown-before-Serve, release CLI listener |
| Running / TLS-client startup | Shutdown stops admission, closes active transports, waits for cleanup |
| Signal plus terminal accept error | Serve's retained error wins; cleanup still runs |
| Serving goroutine finished, process alive | Interception remains active; completed result determines status |
| Repeated signal / signal after timeout | Same latched request; no additional timer or exit path |
| Shutdown cannot finish | Failure after budget, main exits; no claim of completed cleanup |

## Follow-ups and scope

No lifecycle redesign, queue/handshake/size-limit changes, logging backend,
systemd configuration, dependencies or build changes. Existing per-client send
warnings during intentional closure remain a library logging follow-up.
Windows service control requires a separate service integration. Arbitrary
blocked startup calls cannot guarantee in-process resource cleanup at deadline;
the documented process boundary is required. External SIGKILL/Windows forced
termination always precludes cleanup guarantees.

## Regression verification

The CLI tests inject cancellation instead of signalling the test process. They
cover pre-creation stop; prepare/listen barriers; post-listen ownership; causal
listen cancellation versus bind errors; configuration and key-pair loading
errors concurrent with stop; successful/failed normal completion; the
Shutdown-before-Serve reservation race; repeated requests; global and shutdown
timeouts; and resumed blocked config/TLS reads that must not proceed to listen.
Real Server orchestration tests cover idle TCP, established TLS, stalled TLS
handshake and an unread peer whose MOTD write is blocked. Barriers and transport
observations verify completion; deliberately stalled workers are released and
joined by tests. The existing library suite further covers client/channel
cleanup, listener-close errors, multi-client teardown and accept/shutdown races.
The main-package tests verify successful diagnostics and bounded final error
reporting with a blocked stderr writer.

Validation: native Windows `go test ./...`, `go test -race ./...`, 30 race
repetitions of the CLI package, `go vet ./...`, and `mage build`. Linux and
Darwin CLI builds and command-test binaries cross-compile. Plan 9 CLI
cross-compilation is blocked by the unchanged afero v1.11.0 dependency referring
to unavailable `syscall.EBADFD`; its library compatibility is outside this fix.
No built nvremoted executable or OS-signal integration test is run.
