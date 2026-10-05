# Локальные performance benchmarks

Эта система измеряет baseline NVRemoted, локализует затраты relay и показывает
поведение около saturation. Она не оптимизирует production код, не включает
PGO в сборки и не задаёт аппаратно зависимых pass/fail thresholds.

## Быстрый запуск

Все команды запускаются из корня проекта. Для offline запуска нужны
установленные Go/Mage и закэшированные module dependencies. Workloads используют
только loopback; suite не устанавливает дополнительные инструменты.

```text
mage bench
mage benchLong
```

`bench` запускает стандартные component benchmarks и quick TCP/TLS matrix.
Ожидаемое время — несколько минут; фактическое время зависит от машины и
стоимости большого JSON. `benchLong` расширяет выборки и matrix, включая fan-out
100 и 500/1000 клиентов; рассчитан на десятки минут. Это presets, не обещание
фиксированного времени исполнения. Результаты каждого завершённого сценария
сохраняются сразу, поэтому прерванный long run оставляет частичный отчёт.

```text
mage benchMicro
mage benchRelay
mage benchSmoke
```

`benchMicro` сохраняет Go benchmark output для benchstat. `benchRelay` пропускает
component benchmarks. `benchSmoke` проверяет корректность tagged harness без
performance assertions. `benchSmokeRace` делает то же с race detector; на
Windows нужны LLVM/Clang и PATH/CC из [runbook](../AGENTS.md).

Performance benchmarks **не запускаются в CI**. Hosted runners меняют частоту
CPU и конкурируют за ресурсы с другими задачами. Обычные `test`, `testRace`,
`check`, `buildAll` продолжают работать. Обычные tests проверяют histogram,
payload templates, configuration и result schema; network harness и benchmarks
доступны только с build tag `performance`. Даже с этим tag длинный runner требует
отдельного opt-in, который выставляет Mage.

Результаты: новый каталог `perf-results/<UTC>-<preset>-<unique>/`:

* `components.txt`: стандартный Go benchmark format, `ns/op`, `B/op`, `allocs/op`;
* `results.json`: агрегированный отчёт, schema version 1;
* `summary.txt`: human-readable итог;
* `runner.txt`: ход исполнения и диагностика;
* `relay.test[.exe]`: тот же test executable для разбора профилей;
* `profiles/`: только при отдельном profiling run.

Каталог игнорируется Git и не удаляется `mage clean`. Пользователь управляет
архивированием результатов. `NVREMOTED_BENCH_OUTPUT` задаёт новый каталог;
повторный запуск в существующий каталог запрещён, чтобы не затереть baseline.

## Presets и overrides

Quick: measurement 1s, warm-up 250ms, drain до 2s, одна repetition. Long:
measurement 10s, warm-up 1s. Интервалы относятся к каждому relay сценарию;
admission/handshake/boundary используют конечные batches или observation windows
и явно описывают свои интервалы в `observations`.

Пример PowerShell 7:

```powershell
$env:NVREMOTED_BENCH_TRANSPORT = 'tcp'
$env:NVREMOTED_BENCH_DURATION = '10s'
$env:NVREMOTED_BENCH_WARMUP = '1s'
$env:NVREMOTED_BENCH_RATE = '5000'
$env:NVREMOTED_BENCH_CHANNELS = '10'
$env:NVREMOTED_BENCH_FANOUT = '4'
$env:NVREMOTED_BENCH_PAYLOAD = 'mixed'
mage benchRelay
```

После эксперимента удалите task-specific env overrides или используйте новую
shell, чтобы они случайно не меняли следующий preset.

| Variable suffix после `NVREMOTED_BENCH_` | Назначение |
| --- | --- |
| `TRANSPORT` | `tcp`, `tls`, `both` (default) |
| `DURATION`, `WARMUP`, `DRAIN` | Go durations; warm-up может быть `0s` |
| `RATE` | Общий target messages/s по всем senders; включает open-loop |
| `CHANNELS`, `FANOUT` | Число channels и recipients на channel |
| `CLIENTS` | Запрошенное количество idle/lifecycle клиентов |
| `PAYLOAD` | Payload profile либо `mixed` |
| `REPETITIONS` | Повторения E2E scenarios |
| `FILTER` | Подстрока имени сценария: `payload/`, `saturation/`, `clients/`, `lifecycle/recovery` |
| `MAX_IN_FLIGHT` | Bounded outstanding measurement window (default 4096); exhaustion отражается в результате |
| `OUTPUT` | Новый result directory |
| `COMPONENT_FILTER` | Стандартный Go `-bench` regexp |
| `BENCHTIME`, `COMPONENT_COUNT` | Go microbenchmark duration/iterations и repetitions |
| `PROFILE` | Только `benchProfile`: `cpu`, `heap`, `allocs`, `goroutine`, `block`, `mutex`, `trace`, `all` |
| `TIMEOUT` | Watchdog E2E test process (default `6h`); `0s` отключает, короткое значение полезно для диагностики зависаний |

Watchdog ограничивает один E2E subprocess, включая setup и cleanup; это не latency
assertion. При срабатывании Go выводит goroutine stacks и завершает test process,
освобождая его sockets/listeners. Последний незавершённый сценарий не считается
успешным. Для runs дольше шести часов увеличьте `TIMEOUT`. Component benchmarks
имеют собственный стандартный Go lifecycle и не используют этот override.
При `PROFILE=all` каждый из трёх subprocesses имеет свой watchdog.

Без `FILTER` relay overrides формируют компактный custom scenario. `CLIENTS`
формирует idle и joined custom scenarios. С `FILTER` overrides применяются к
выбранным сценариям preset. Ни одна настройка не делает production limits
неограниченными; слишком большая topology может не пройти admission.

Quick/long не строят полный Cartesian product. Сначала меняется одна ось:
payload, fan-out, rate, channel count, client count. Для исследования комбинаций
есть overrides. Long component benchmarks используют 200ms × 5; quick — 100ms
× 1. Для statistically useful microbenchmark comparisons увеличьте repetitions.

## Workloads

Payload matrix включает key/control/gesture, короткий и обычный speak, большой
speech sequence с текстом и командами, braille 20/40/80/160 cells, clipboard
1/16/64/256 KiB и 1 MiB, flat/nested/string/array arbitrary JSON. Это synthetic
fixtures, соответствующие классам wire traffic; это не записи реальных
пользовательских данных. Размер clipboard означает размер строки, а не полного
JSON. `bench_seq` — дополнительное arbitrary поле только тестовых сообщений.

Wire shapes сверены с upstream [NVDA session](https://github.com/nvaccess/nvda/blob/master/source/_remoteClient/session.py)
и [serializer](https://github.com/nvaccess/nvda/blob/master/source/_remoteClient/serializer.py).
Harness не запускает NVDA, speech synthesizer или обработку клавиатуры клиента.

Fan-out: 1→1/2/4/10/32, long также 1→100. Multi-channel: 1/10/100 channels,
несколько горячих channels, смешанная hot/cold нагрузка. Long также имеет
нагруженный fan-out. Saturation ladder: 100, 1k, 5k, 10k, 25k messages/s; long
добавляет 50k/100k. Это стартовая сетка, её следует уточнять вокруг найденного
knee, а не принимать последний успешный rate за абсолютный предел.

Idle/joined client scenarios: 100, long 500/1000. **Unjoined idle клиент имеет
startup deadline и занимает pending capacity.** По умолчанию сервер владеет не
более 128 pending connections; idle harness ограничивает фактическое число
попыток этим пределом и отдельно записывает неисполненные запросы. Поэтому
requested=1000 не означает 1000 idle объектов сервера. Смотрите
requested/attempted/unattempted/owned/joined counts и capacity snapshots.
Joined idle clients — отдельный режим с другим lifetime и budget.
Unjoined observation сокращается перед default startup deadline, если заданный
measurement interval превышает оставшийся lifetime; effective interval и
warm-up явно записаны. Это не продление production timeout ради benchmark.

Lifecycle scenarios отдельно измеряют ordinary admission, complementary
protected join, generic recovery, configured overlap reconnect, capacity
refusal, concurrent rejection/speech window, pending TLS/startup, TCP connect,
TLS handshake и startup/join. Для отказов используются явно записанные finite
конфигурации с заполненными admitted credits. Измеряется decision отдельно от
cleanup; default 5s rejection window не сокращается для ускорения теста.

Boundary cases: small/medium/1MiB/near default limit/exact limit и oversized
offenders. Лимит — размер incoming JSON value, без разделяющего LF, а не размер
clipboard string. Oversized case проверяет отказ без forwarding и cleanup;
expected refusal не считается успешной доставкой.

## Что именно измеряется

Setup → TCP connect/TLS handshake → protocol join/control notifications →
warm-up → measurement → bounded drain → validation → cleanup.

Основной E2E path использует настоящий `Server.Serve`, JSON reader, protocol
handlers, channel worker, recipient handlers и socket writes. Connections
persistent. TLS проверяет изолированный локальный сертификат через собственный
trust pool; production verification не меняется. Certificate generation и
handshakes исключены из steady-state latency.

Sender и receiver живут в одном процессе. Sequence ID связывает сообщение с
монотонным временем harness; timestamp не передаётся через JSON. Actual-send
latency начинается перед socket Write, заканчивается после получения полного
response frame. Это upper bound пути localhost client→relay→client, включая
transport/OS и framing клиента. Нельзя назвать её чистым CPU-временем relay или
задержкой реального NVDA UI. Sender write latency, recipient distributions и
last-recipient completion помогают разделить причины задержки.

На Windows harness использует общий process epoch и
[QueryPerformanceCounter](https://learn.microsoft.com/en-us/windows/win32/api/profileapi/nf-profileapi-queryperformancecounter),
как Windows backend стандартного Go benchmark timer. Native `time.Now()` в
некоторых Go/Windows конфигурациях имеет слишком грубый monotonic clock для
sub-ms latency. На других платформах используется Go monotonic clock. Metadata
содержит источник, QPC frequency, минимальный наблюдавшийся positive step и
среднюю стоимость calibration probe. `BenchmarkPerformanceClock` отдельно
показывает overhead часов и allocations; calibration не является гарантией
разрешения всех будущих измерений. Socket deadlines и сертификаты используют
обычное время ОС. Ни clock, ни suite не меняют system timer resolution.

CPU accounting ОС может округлять короткий admission batch до нулевого CPU
time. Для CPU/admission нужны большие batches/repetitions; ноль в одном коротком
замере не означает бесплатную обработку.

Closed-loop ждёт получения перед следующей отправкой. Он полезен для latency
floor, но **не описывает tails при перегрузке**: медленный ответ сокращает
следующую нагрузку (coordinated omission).

Open-loop задаёт расписание независимо от получения предыдущих сообщений.
Scheduled latency начинается в запланированный момент отправки; actual-send —
когда sender реально начал Write. Generator lag, missed offers, achieved rate и
write latency показывают ограничения генератора. При backpressure нагрузка не
переклассифицируется в успешный меньший rate. Catch-up/bursts из-за scheduling
отражаются в distributions; не предполагается идеальная точность таймеров OS.
`requested_load_fully_offered` требует отсутствия missed offers, включая последние
slots на границе measurement window. Несколько таких cutoff misses при полной
доставке реально отправленных сообщений — ограничение генератора/window, а не
доказательство saturation сервера. Смотрите `delivery_valid` отдельно; comparator
консервативно исключает и эти строки.

Считаются sent, expected recipient deliveries, received, in-window receive,
missing, duplicates, out-of-order, corrupted/unexpected messages и disconnects.
Каждый payload проверяется, а не только sequence ID. Delayed drain delivery
не увеличивает in-window receive throughput. Результат с нарушенной доставкой
или неспособностью поддерживать target помечается; его нельзя использовать как
успешную performance точку. Saturation cases могут быть ожидаемо invalid.

Distribution: count, min, mean, p50/p90/p95/p99, max, population standard
deviation. p99.9 публикуется при ≥10000 samples (минимум десять ожидаемых
наблюдений в верхних 0.1%). Histogram bounded, logarithmic bins с шириной ≤1%;
moments и extrema точные. Это не математическая коррекция coordinated omission:
scheduled latency и open-loop решают другую часть проблемы напрямую.

Queue utilization периодически sampling `len(events)`, а overflow disconnects
считаются из существующих server logs. Sampling может пропустить краткие пики.
При первом overflow log harness сохраняет elapsed measurement time и cumulative
send/receive counters. `pre_overflow_*_per_second` показывает среднюю скорость
до первого наблюдённого overflow, исключая оставшееся время после disconnect.
Это не instantaneous last-bucket rate; send counters считают завершённые Write,
но их deliveries могут оставаться in flight. Snapshot берётся только при overflow,
не на normal per-message path.
Нет дополнительных production counters/hooks или изменения protocol/defaults.

CPU user/system time, utilization (100% = один CPU core), allocations, heap,
objects, goroutines и GC относятся ко **всему harness process**, включая
server, clients, validation и telemetry. Они не являются server-only metrics.
Component benchmarks локализуют production costs; CPU/block/mutex profiles и
trace показывают contribution harness. Не складывайте allocations/op разных
fragments: этапы могут иметь общий объект и разные lifetime.

Histograms принадлежат отдельным workers, но bounded bookkeeping sequence IDs
использует общий mutex и выделяет stamp на сообщение. При большом fan-out это,
как и decode/validation клиентов, может ограничить harness раньше сервера.
Нельзя объявлять найденный knee пределом relay без проверки generator lag,
server queue/overflow и CPU/block/mutex profiles. Падение achieved rate само по
себе не определяет, какая сторона стала bottleneck.
Даже настоящий server overflow характеризует всю локальную конфигурацию:
clients harness конкурируют с server за CPU и shared GC. Это наблюдение overload,
а не доказательство изолированного server maximum для другой transport/machine.

Windows RSS — current working set, Linux — current RSS, macOS — **peak RSS**;
`rss_kind` явно различает их. Unsupported OS metrics помечаются unavailable.
GC CPU — runtime cumulative class metric; pauses — последние максимум 256
циклов, truncation отмечена. Pause duration не равна полной длительности GC и
не объясняет автоматически p99. GC не принуждается. Heap может содержать garbage
до следующего обычного цикла; memory/client delta включает клиентов harness и
не должна трактоваться как точный размер production client struct.
На Windows runtime pause statistics могут быть квантованы независимо от QPC
часов harness: нулевой `PauseNs` не доказывает отсутствие паузы. Для выводов о GC
сопоставляйте cycles, GC CPU, allocation rate, profiles и latency distributions.

## Profiles и будущий PGO experiment

```powershell
$env:NVREMOTED_BENCH_TRANSPORT = 'tcp'
$env:NVREMOTED_BENCH_DURATION = '30s'
mage benchProfile
```

Default profiling workload: 10 channels, fan-out 4, sustained mixed payloads,
target 5k messages/s. Rate следует подобрать по baseline, чтобы профиль отражал
типичную production нагрузку и не был профилем перегруженного генератора.
Overrides применимы. Для trace обычно достаточно 1–3s; trace, block и mutex
имеют заметную стоимость и запускаются отдельно от baseline. `PROFILE=all`
последовательно запускает CPU, block и mutex в трёх свежих subprocesses с одним
workload/config. Результаты, binary и profiles находятся соответственно в
`<run>/cpu/`, `<run>/block/`, `<run>/mutex/`. Measurement duration применяется
к каждому run: общее время примерно втрое больше плюс setup/warm-up. Trace
запускается отдельно. CPU и contention samplers не работают одновременно:
такое сочетание с записью каждого события зависало в локальных Windows runs;
точная причина внутри runtime не установлена.

Профили стартуют после setup/warm-up. CPU run также сохраняет heap/allocs,
goroutine и доступные block/mutex snapshots. Для block/mutex sampling включайте
соответствующий режим. Block sampling использует интервал 1 ms; это sampling
events пропорционально времени блокировки, а не periodical polling. Mutex
sampling записывает в среднем одно из десяти contention events. Pprof масштабирует
sampled values; profiles не являются полным журналом событий. Heap profile не
вызывает искусственный GC. Прямой вызов harness с `PROFILE=all` отвергается:
orchestration выполняет Mage, чтобы samplers оставались изолированы.

```text
go tool pprof -top perf-results/<run>/relay.test.exe perf-results/<run>/profiles/tcp/1/cpu.pprof
go tool pprof -top -alloc_space perf-results/<run>/profiles/tcp/1/allocs.pprof
go tool trace perf-results/<run>/profiles/tcp/1/execution.trace
```

В Unix имя binary без `.exe`. При наличии Browser MCP следуйте локальным
инструкциям открытия UI; `-top` не запускает browser/server. Profile содержит
clients harness вместе с relay. Перед будущим PGO experiment проверьте hotspots
и representativeness: профиль tiny microbenchmark для этого не подходит.
Source matching общего server/TLS/JSON кода возможно, но test executable не
покрывает CLI startup. Общие JSON/TLS/runtime функции получают смешанные веса
от server и benchmark clients; отсутствие harness функций в production binary
не устраняет это смешение. Поэтому такой профиль подходит для исследовательского
PGO experiment, но не доказывает representativeness production. Для окончательного
выбора release profile предпочтителен server-only профиль из отдельного process
или production workload. Сохраняйте профиль, commit, config и binary вместе.

`allocs.pprof` содержит sampled cumulative allocations с начала process, включая
setup и предыдущие сценарии. Для сравнения transports используйте отдельные
profiling runs в свежих processes; interval allocation totals находятся в JSON.
Block/mutex snapshots также накопительные внутри process. Если один invocation
проходит TCP и TLS либо repetitions, поздний snapshot включает более ранние
sampling windows. Для attribution конкретного transport выбирайте его отдельным
`TRANSPORT=tcp` или `TRANSPORT=tls` запуском. CPU stream начинается заново для
каждого сценария; heap и allocation snapshots имеют другую semantics.
Payload validation проверяет полное содержимое ациклического JSON структурным
сравнением без reflection/cycle bookkeeping. Client JSON decode и проверка всё
равно участвуют в process CPU/GC и могут ограничить workload.

PGO пока не включён в Mage release targets. Позднее baseline и PGO следует
запускать с идентичными payloads/topology/rate/duration и сравнивать отдельно.
[Официальная документация Go PGO](https://go.dev/doc/pgo).

## Сравнение и воспроизводимость

```text
mage benchCompare baseline/results.json candidate/results.json
```

Comparator сопоставляет одинаковые scenario parameters/repetition и показывает
absolute/relative delta p50/p95/p99, delivery throughput, allocations, memory,
CPU efficiency. Invalid results не получают performance delta; разные machine,
compiler или measurement settings вызывают предупреждение. Это descriptive
comparison, а не statistical significance test.

Для standard Go benchmarks:

```powershell
$env:NVREMOTED_BENCH_COMPONENT_COUNT = '10'
$env:NVREMOTED_BENCH_COMPONENT_FILTER = 'BenchmarkPerformanceMessageDecode'
mage benchMicro
# После второй сборки/реализации:
mage benchMicro
benchstat baseline/components.txt candidate/components.txt
```

`benchstat` — внешний optional developer tool, не module/runtime dependency.
Он принимает стандартный Go output и рассчитывает intervals/A/B significance.
Установите выбранную версию отдельно, если она нужна; suite её не скачивает.
[Документация benchstat](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat).

Сравнивайте на одной контролируемой машине, с одинаковым Go/GOMAXPROCS,
CGO/architecture settings, server config, topology и нагрузкой. Не запускайте
другие builds/tests одновременно с baseline. Питание, thermal throttling,
frequency scaling, antivirus, background programs и OS timer behavior меняют
tails. Suite сама не меняет priority, affinity, governor или системные настройки.
Повторите одинаковые local runs; single long run всё равно не доказывает
воспроизводимость. Metadata фиксирует среду; frequency/governor пока не меняются
и не собираются автоматически.

Финальный вопрос baseline — где тратится время и есть ли queues/GC pressure,
а не удалось ли заранее выбранное «ускорение на 5 ms». Если localhost relay
добавляет около 100 µs, искать в нём 5 ms steady-state экономии бессмысленно;
tail behavior около saturation всё равно требует отдельной оценки.
