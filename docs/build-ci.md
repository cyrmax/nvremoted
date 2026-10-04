# Build and CI contract

Mage owns project-specific build, test and static verification behavior. The
GitHub workflow only prepares tools, restores caches, invokes public targets and
uploads their distribution output. This fork is maintained independently.

## Existing behavior and decisions

The original Magefile exposed `Build`, `BuildRace`, `Install` and `Clean`.
It used `git describe --always --long --dirty` (annotated tags when present,
otherwise commit SHA), falling back to `unset` outside a usable Git checkout.
`-X github.com/n0ot/nvremoted/cmd/nvremoted/commands.Version=...` remains unchanged.
There was no test target, repeat implementation or explicit CGO policy.
The agreed 100-repeat policy is now encoded in Mage with Go's `-count=100`;
there is no shell loop or test result caching. `NVREMOTED_TEST_COUNT` accepts a
positive integer override for diagnosis. Both test targets include the `mage`
build tag so the Mage contract tests run alongside application tests.

The old output was `bin/nvremoted`, with `.exe` selected only by the environment
variable `GOOS`. That missed ordinary Windows host builds. `Build` now uses
`runtime.GOOS/GOARCH`, and explicit targets always supply their own platform.
All builds share one helper and use `dist/<goos>-<goarch>/nvremoted[.exe]`.
Each binary has a sibling `nvremoted[.exe].sha256`, in standard SHA-256 checksum
format with a basename so it can be verified after extracting the artifact.

Distribution builds and ordinary tests explicitly use `CGO_ENABLED=0`.
Race tests use `CGO_ENABLED=1`. Compiler selection and PATH belong to the caller;
Mage never selects LLVM or edits PATH. Target environment overrides apply only
to child Go processes. `GOWORK=off` keeps repository checks/builds independent of
a surrounding workspace; build, test and vet use `-mod=readonly`.
Linux, Windows and Darwin on amd64 and arm64 all cross-compile without CGO;
native macOS/Windows CI runners are unnecessary for distribution builds.

`BuildRace` and `Install` remain for compatibility. `BuildRace` uses the same
host helper and replaces that host's distribution binary with a race build.
Neither is part of CI or the recommended development workflow.

## Static checks and cleanup

`mage check` runs all three checks even if one fails:

* Go formatting for regular `.go` files, including Mage and tests, excluding
  `.git`, generated `dist`/legacy `bin`, and vendored dependencies. It compares
  `go/format` output and prints `gofmt -d` for drift; it never writes formatting.
* `go mod tidy -diff`: prints drift and fails without modifying `go.mod` or
  `go.sum`. This option is available with the project's Go 1.23 minimum. No
  temporary worktree, stash, reset or restoration is needed, even on dirty trees.
* `go vet -mod=readonly -tags=mage ./...` on the host with CGO disabled.

`mage clean` removes only `dist` and the legacy generated `bin` directory.
It checks both trees before deleting and rejects symbolic links/junctions in
output trees. It preserves source, configuration and shared module/build caches.

## Windows race environment

Prepare LLVM only in the shell/process running tests, following the project's
`AGENTS.md`: prepend `C:\Program Files\LLVM\bin` to PATH and set `CC=clang`.
Mage supplies CGO itself. Do not change global environment variables.
If both `PATH` and `Path` are inherited, create a child process environment with
all case variants removed and a single PATH containing LLVM. A simple shell PATH
assignment can leave a second variant that Go child tools select instead.
The linker warning `LNK4217` is not a test failure.

## Local contract verification

Before adding CI, Windows/amd64 verification exercised the host build, every
explicit platform target and `clean` followed by `buildAll`. ELF, PE and Mach-O
headers and Go build metadata confirmed the six target architectures, extension
rules, CGO disabled and the unchanged embedded `git describe` value. All SHA-256
sidecars matched the binaries. The built application was never executed.

Isolated module fixtures confirmed that a failing test and a real data race
make the public targets fail, that the default executes exactly 100 repeats and
the override exactly one, and that format/vet/tidy drift makes `check` fail while
preserving every file byte, including untracked user content. A real Windows
junction fixture confirmed cleanup refuses redirected output and preserves its
external contents.

The first 100-repeat ordinary suite exposed a pre-existing TLS shutdown test
synchronization gap: MOTD decode completed before the send's deadline reset.
The test now waits on its existing `writeCleared` barrier before shutdown;
1000 focused repetitions and the subsequent full 100-repeat suite passed.
Server behavior was not changed.
