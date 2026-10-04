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

## GitHub Actions

The workflow triggers on push, pull requests and manual dispatch. `check`,
`test` and `race` run independently on Ubuntu 24.04. The six-entry `build`
matrix depends on all three succeeding, then calls the corresponding public
Mage target and uploads only `dist/<platform>/`. No build flags, CGO setting,
version logic, repetitions or static checks are repeated in YAML.
GCC is already supplied by the standard Ubuntu runner image; race needs no
Windows-specific PATH workaround or extra compiler install.

The Go version is pinned in `.go-version` to 1.27.1, the latest stable release
verified on 2026-10-05. The minimum `go 1.23.0` directive is unchanged.
Mage 1.17.2, the latest official stable release, is pinned by the existing
dependency in `go.mod`. The local composite setup action reads that version and
installs it into a fresh runner-temporary GOBIN, then adds it to GITHUB_PATH.
No floating `@latest` or cached binary can accidentally select another version.

`setup-go` caches both GOMODCACHE and GOCACHE. The explicit dependency files are
`go.mod` and `go.sum`; its cache key also includes runner OS/architecture, Ubuntu
image and resolved Go version. Mage's source/compiler cache participates in the
same cache. Local installation measurements on Go 1.27.0 were 9.02 seconds for
the first install (including module download) and 0.76 seconds for a warm
install. A separate Mage binary cache adds configuration without demonstrated
benefit, so it is omitted. Cache misses install automatically; cache hits still
run the pinned install and cheaply relink/copy the executable.

Official stable Actions checked before writing the workflow:

| Action | Latest stable release checked | Major used |
| --- | --- | --- |
| [checkout](https://github.com/actions/checkout/releases/tag/v7.0.1) | v7.0.1 | v7 |
| [setup-go](https://github.com/actions/setup-go/releases/tag/v7.0.0) | v7.0.0 | v7 |
| [upload-artifact](https://github.com/actions/upload-artifact/releases/tag/v7.0.1) | v7.0.1 | v7 |

No separate cache or download-artifact action is required. Build checkouts use
`fetch-depth: 0` to retain tags/history for the original `git describe` semantics;
PR builds describe GitHub's tested merge revision. Verification jobs can use
shallow checkouts because they do not calculate a distribution version.

Artifacts are named `nvremoted-<platform>` and contain `nvremoted[.exe]` plus
`nvremoted[.exe].sha256`. Retention is 21 days (within GitHub's documented 1–90
day range). No artifact is uploaded when check/test/race fails or is cancelled.
Consumers must select a run with overall **success**: individual matrix entries
may finish before another build or upload fails. This keeps the first CI simple
without intermediate artifacts or a second publication/rebuild stage.
ZIP uploads lose Unix executable permissions; use `chmod +x` after extraction.

The token has only `contents: read`; checkouts disable persisted credentials.
There are no secrets, write permissions, deployment or `pull_request_target`.
Fork PRs use the same unprivileged verification path.

Official references: [Go releases](https://go.dev/dl/),
[Mage release](https://github.com/magefile/mage/releases/tag/v1.17.2),
[setup-go cache implementation](https://github.com/actions/setup-go/blob/v7.0.0/src/cache-restore.ts),
[Ubuntu runner tools](https://github.com/actions/runner-images/blob/main/images/ubuntu/Ubuntu2404-Readme.md),
[artifact retention](https://github.com/actions/upload-artifact/blob/v7.0.1/README.md#retention-period).

Follow-ups: observe the first hosted Linux run and cache timing; optionally add
a separate Go-minimum compatibility job. Signing, releases, packaging and
deployment remain separate future work.

Final local verification also used CI's exact Go 1.27.1 toolchain: host build,
clean rebuild of all six platforms, `check`, and both full 100-repeat test
targets passed. An inherited Linux/arm64/CGO=1 environment still produced the
Windows/amd64 host binary with CGO=0. `actionlint` v1.7.12 validated the workflow;
a separate YAML parse checked the local composite action, matrix/API agreement,
actual artifact paths, permissions and verification dependencies. The linter
was installed only in a task-temporary directory, without a project dependency.
Independent Mage and fresh full-diff reviews passed after the junction fix.
GitHub-hosted execution is not claimed by these local checks.
