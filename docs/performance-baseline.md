# Локальный baseline NVRemoted — 5 октября 2026

Это baseline без изменения production hot paths, protocol semantics, resource
defaults или release optimization flags. Руководство по запуску и интерпретации:
[benchmarks.md](benchmarks.md). Числа относятся к указанной машине и workload;
они не являются SLA или performance thresholds для CI.

На этой машине маленькое сообщение без backlog проходило полный localhost путь
с p50 35.5 µs TCP / 38.1 µs TLS и p99 72.7 / 85.2 µs. В таком сценарии искать
5 ms выигрыша в relay бессмысленно. Миллисекундные задержки видны у больших
payloads и при нагрузке; для них следующий шаг — отдельный измеряемый эксперимент.
Production optimization в эту задачу не входила.

## Среда и артефакты

Основные latency/component измерения: чистый `575c3b92`, production parent `ebfe87a5`.
Повторы, memory scaling и уточняющие rates: чистый `ff17524a` (watchdog/profile
shutdown safeguards). Финальные isolated profiles, trace и pre-overflow counters:
чистый `75ef3271`. Production relay и обычный measurement path между ними не менялись;
последняя ревизия добавляет отдельный snapshot только при overflow и profiling orchestration.
Windows/amd64, AMD Ryzen 9 7945HX, 32 logical CPUs, GOMAXPROCS=32, Go 1.27.1,
CGO=0, GOAMD64=v1. Persistent localhost TCP и TLS с проверкой сертификата
из isolated benchmark trust store. Handshake/setup/join исключены из steady-state.

Clock: общий process monotonic QPC, 10 MHz, observed step 100 ns. Стоимость чтения
измеряется отдельно; suite не меняет system timer resolution, priority или affinity.
Frequency/governor и background system load не фиксировались автоматически.

Канонические локальные результаты, schema version 1:

* `perf-results/20261005-final-quick/`: `mage bench`, 1s + 250ms warm-up;
* `perf-results/20261005-final-long/`: `mage benchLong`, 10s + 1s warm-up,
standard microbenchmarks 200ms × 5;
* `perf-results/20261005-final-profile-tcp/` и `-tls/`: CPU profiles 30s;
* `perf-results/20261005-final-isolated-a/` и `-b/`: два `PROFILE=all` runs,
  по 10s CPU/block/mutex в отдельных subprocesses;
* `perf-results/20261005-final-trace/`: отдельный 2s execution trace;
* `perf-results/20261005-final-repeat-a/` и `-b/`: 10s key TCP/TLS повторы;
* `perf-results/20261005-final-memory-{100,500,1000}-{tcp,tls}/`: fresh process scaling;
* `perf-results/20261005-final-rate-{60000,75000}/`: 10s уточнение overload;
* `perf-results/20261005-final-overflow-{60000,100000}/`: 5s runs с first-overflow snapshot.

Каждый каталог содержит JSON, human-readable summary и runner log; microbenchmark
каталоги также содержат `components.txt`. Артефакты игнорируются Git; этот документ
сохраняет основные числа, а для подробного сравнения следует архивировать каталоги.

Quick выполнил 103 scenarios (98 полностью valid); long — 135 (120 valid).
В long два 100k/s scenarios завершились queue overflow и 4096 missing deliveries
на transport. Ещё 13 scenarios имели только generator cutoff misses: все реально
отправленные сообщения были доставлены. Duplicates, out-of-order, corrupted,
unexpected payload и server errors отсутствовали во всех relay cases. `PASS`
означает корректность самого измерения, а не достижение каждого target rate.

Первый exploratory run `680114af` и старые `20261005-baseline-*` профили не являются
финальным mixed-workload baseline. CPU/allocs profile обнаружил allocation-heavy
`reflect.DeepEqual` в harness validation. Исправление проверяет весь JSON напрямую,
включая absent versus null, и покрыто correctness/allocation tests. Production не
оптимизировался; изменение результатов между этими ревизиями нельзя считать
ускорением самого relay.

## Internal/component costs

| Component / fixture | Median ns/op | Range ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: |
| ChannelHandoff/1-recipients | 946.4 | 928.4–957 | 16 | 1 |
| ChannelHandoff/32-recipients | 16035 | 15457–16246 | 512 | 32 |
| ChannelMessagePipeline/1-recipients/full-reader-decode-dispatch-encode | 15503 | 14918–15819 | 4588 | 84 |
| ChannelMessagePipeline/10-recipients/full-reader-decode-dispatch-encode | 47385 | 46893–48371 | 9345 | 220 |
| ChannelMessagePipeline/2-recipients/full-reader-decode-dispatch-encode | 22951 | 22867–23193 | 5119 | 99 |
| ChannelMessagePipeline/32-recipients/full-reader-decode-dispatch-encode | 79235 | 77303–86802 | 21427 | 554 |
| ChannelMessagePipeline/4-recipients/full-reader-decode-dispatch-encode | 29537 | 29061–31679 | 6174 | 129 |
| Clock | 46.01 | 45.44–46.12 | 0 | 0 |
| MessageDecode/arbitrary-map-unmarshal | 3971 | 3636–4139 | 2307 | 52 |
| MessageDecode/key/protocol-unmarshal | 1815 | 1736–1869 | 497 | 18 |
| MessageDecode/key/reader | 1260 | 1123–1377 | 1338 | 13 |
| MessageDecode/key/type | 414.6 | 412.9–423.5 | 16 | 1 |
| MessageDecode/typed-second-unmarshal | 423.3 | 421.4–424.9 | 72 | 3 |
| RecipientPreparation/key/add-origin | 9.06 | 9–9.11 | 0 | 0 |
| RecipientPreparation/key/copy-map | 426.2 | 351.8–462.5 | 336 | 2 |
| RecipientPreparation/key/copy-map-and-origin | 353.8 | 324.7–383.6 | 336 | 2 |
| RecipientPreparation/key/json-encode | 1062 | 1048–1085 | 200 | 16 |

Это медианы пяти `testing.B` измерений, не latency percentiles. `reader`, `type`
и `protocol-unmarshal` перекрывают части работы; их нельзя складывать как независимые
stages. Полный internal pipeline вызывает production reader/unmarshal/channel
worker/recipient handler, использует discard transport и ACK для completion.
Его fixture отличается от E2E key; разность этих чисел не изолирует socket cost.

## Маленькое сообщение: TCP и TLS

| Metric | TCP | TLS |
| --- | ---: | ---: |
| count | 209712 | 185844 |
| min, µs | 16.6 | 20.5 |
| mean, µs | 36.6 | 41.6 |
| p50, µs | 35.5 | 38.1 |
| p90, µs | 42.1 | 51.3 |
| p95, µs | 51.3 | 59.0 |
| p99, µs | 72.7 | 85.2 |
| p99_9, µs | 237.6 | 257.3 |
| max, µs | 771.2 | 494.4 |
| stddev, µs | 13.4 | 14.8 |

Latency начинается перед sender Write и заканчивается после чтения полного frame,
до decode/validation этого frame. Она включает реальный relay, два socket пути,
TLS и scheduling клиентов/ОС. Это не pure server CPU time и не WAN/NVDA UI latency.
Histogram quantiles приблизительны с bin width ≤1%; count/moments/extrema точные.
p99.9 публикуется только при ≥10000 samples.

В этом workload незагруженный localhost путь занимает десятки микросекунд,
а p99.9 — около 0.24–0.26 ms. Даже полное устранение стоимости этого пути
не даст 5 ms экономии для обычного маленького сообщения без backlog.
TLS в этой паре увеличил median примерно на 2.6 µs (7.2%), mean на 5.0 µs
(13.7%), p99 на 12.5 µs (17.3%); это описание одного последовательного сравнения,
не универсальная константа TLS. Использованы TLS 1.3 и TLS_AES_128_GCM_SHA256.
Quick дал более низкие p50 27.4/28.5 µs, поэтому сравнивать следует одинаковые
duration, warm-up и порядок workloads. Bytes/s относятся к JSON frames, включая
newline, и не включают TCP/IP или TLS record overhead.

## Payload size и JSON shape

| Payload | TCP count | TCP p50 / p99, µs | TLS count | TLS p50 / p99, µs |
| --- | ---: | ---: | ---: | ---: |
| key | 209712 | 35.5 / 72.7 | 185844 | 38.1 / 85.2 |
| control | 206891 | 34.1 / 77.9 | 197263 | 36.6 / 80.3 |
| gesture | 192305 | 36.2 / 81.1 | 189522 | 38.1 / 82.7 |
| speak-short | 190138 | 36.6 / 81.1 | 183215 | 38.5 / 87.0 |
| speak | 156030 | 42.9 / 111.5 | 149292 | 46.0 / 116.1 |
| speak-large | 13165 | 339.9 / 995.6 | 13891 | 346.7 / 947.2 |
| braille-20 | 160934 | 41.2 / 105.1 | 158039 | 43.8 / 109.3 |
| braille-40 | 145532 | 45.1 / 132.1 | 136017 | 48.8 / 138.8 |
| braille-80 | 117980 | 53.4 / 206.7 | 114347 | 56.1 / 210.8 |
| braille-160 | 83096 | 70.6 / 304.7 | 80821 | 74.2 / 304.7 |
| clipboard-1KiB | 166584 | 39.6 / 150.3 | 152494 | 44.2 / 161.2 |
| clipboard-16KiB | 45466 | 104.0 / 462.7 | 40071 | 120.8 / 526.6 |
| clipboard-64KiB | 14860 | 320.2 / 2587.7 | 13079 | 379.2 / 6028.9 |
| clipboard-256KiB | 4231 | 1341.9 / 13364.3 | 4010 | 1527.2 / 12589.8 |
| clipboard-1MiB | 1348 | 4517.7 / 20500.5 | 1103 | 5193.0 / 27631.6 |
| arbitrary-flat | 184233 | 37.0 / 87.8 | 176551 | 38.8 / 89.6 |
| arbitrary-nested | 155489 | 42.9 / 112.6 | 141834 | 46.0 / 125.7 |
| arbitrary-strings | 106852 | 56.7 / 244.8 | 103065 | 60.8 / 249.7 |
| arbitrary-arrays | 20281 | 223.8 / 799.8 | 20485 | 232.9 / 815.9 |

Speech fixtures включают команды и текст; arbitrary fixtures включают flat/nested
maps, Unicode strings и arrays. Большой payload отражает стоимость сканирования,
повторного decode/encode и transport; выводы для clipboard нельзя переносить на key.
Closed-loop ограничивает backlog и не описывает tail latency при overload.

## Fan-out и несколько channels

| Fan-out | Transport | Sender/s | Deliveries/s | Recipient p50 / p99, µs | Last p99, µs | Write p99, µs | Harness CPU cores |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | tcp | 18207 | 18207 | 37.0 / 88.7 | 88.7 | 21.8 | 1.45 |
| 1 | tls | 17905 | 17905 | 38.5 / 88.7 | 88.7 | 22.9 | 1.45 |
| 2 | tcp | 18419 | 36837 | 41.2 / 82.7 | 86.1 | 18.8 | 2.15 |
| 2 | tls | 15951 | 31901 | 45.5 / 101.0 | 103.0 | 24.6 | 2.05 |
| 4 | tcp | 13415 | 53660 | 50.3 / 113.8 | 129.5 | 23.4 | 2.92 |
| 4 | tls | 12855 | 51421 | 53.4 / 118.4 | 134.7 | 25.1 | 2.72 |
| 10 | tcp | 10126 | 101256 | 65.2 / 167.7 | 232.9 | 24.3 | 4.41 |
| 10 | tls | 9920 | 99197 | 67.8 / 176.3 | 247.2 | 25.3 | 4.47 |
| 32 | tcp | 4969 | 159005 | 112.6 / 247.2 | 440.3 | 24.1 | 7.87 |
| 32 | tls | 4851 | 155229 | 116.1 / 257.3 | 431.6 | 26.3 | 7.13 |
| 100 | tcp | 3030 | 303010 | 206.7 / 402.6 | 1155.8 | 23.2 | 13.42 |
| 100 | tls | 2995 | 299509 | 210.8 / 386.8 | 1110.7 | 24.6 | 13.52 |

`recipient` percentiles объединяют deliveries всех recipients; распределения каждого
получателя находятся в JSON. `last-recipient completion` считает завершение всех
recipients одного sender message. Aggregate deliveries/s и sender messages/s — разные
величины. Internal fan-out отдельно локализует копирование/encode и goroutine handoff.

| Scenario | Transport | Channels / fan-out | Deliveries | p50 / p99, µs | Missed offers | Heap / RSS, MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| channels/1 | tcp | 1 / 1 | 10000 | 62.0 / 124.4 | 0 | 6.4 / 76.5 |
| channels/1 | tls | 1 / 1 | 10000 | 64.5 / 123.2 | 0 | 6.7 / 75.4 |
| channels/10 | tcp | 10 / 1 | 10000 | 56.1 / 113.8 | 0 | 9.4 / 84.1 |
| channels/10 | tls | 10 / 1 | 10000 | 62.6 / 120.8 | 0 | 12.8 / 85.0 |
| channels/100 | tcp | 100 / 1 | 10000 | 60.8 / 116.1 | 0 | 49.4 / 163.5 |
| channels/100 | tls | 100 / 1 | 10000 | 65.2 / 123.2 | 0 | 92.8 / 171.5 |
| clients/active/500 | tcp | 250 / 1 | 49999 | 51.8 / 104.0 | 1 | 221.2 / 308.2 |
| clients/active/500 | tls | 250 / 1 | 49998 | 59.6 / 109.3 | 2 | 214.0 / 348.7 |
| clients/active/1000 | tcp | 500 / 1 | 49999 | 55.6 / 103.0 | 1 | 388.6 / 473.0 |
| clients/active/1000 | tls | 500 / 1 | 49998 | 60.8 / 109.3 | 2 | 393.1 / 532.2 |
| channels/hot | tcp | 10 / 2 | 99998 | 59.0 / 350.2 | 1 | 18.8 / 111.9 |
| channels/hot | tls | 10 / 2 | 99998 | 61.4 / 357.2 | 1 | 21.1 / 115.4 |
| channels/hot-cold | tcp | 100 / 1 | 10000 | 63.2 / 346.7 | 0 | 61.7 / 201.1 |
| channels/hot-cold | tls | 100 / 1 | 10000 | 69.2 / 346.7 | 0 | 90.8 / 208.5 |

Для hot/cold каждый channel имеет собственные счётчики; агрегированный результат
не является доказательством fairness при любой конфигурации.

## Open-loop и saturation

| Target/s | Transport | Sent/s / recv/s | Actual p50 / p99, µs | Scheduled p99, µs | Missed | Missing deliveries | Queue peak / overflow | Valid |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 100 | tcp | 100.0 / 100.0 | 93.2 / 161.2 | 688.9 | 0 | 0 | 0/64 / 0 | True |
| 100 | tls | 100.0 / 100.0 | 100.0 / 158.0 | 655.5 | 0 | 0 | 0/64 / 0 | True |
| 1000 | tcp | 1000.0 / 1000.0 | 52.4 / 113.8 | 642.6 | 0 | 0 | 0/64 / 0 | True |
| 1000 | tls | 1000.0 / 1000.0 | 55.0 / 120.8 | 675.4 | 0 | 0 | 0/64 / 0 | True |
| 5000 | tcp | 4999.8 / 4999.8 | 49.3 / 125.7 | 937.9 | 1 | 0 | 0/64 / 0 | False |
| 5000 | tls | 4999.9 / 4999.9 | 52.9 / 128.2 | 937.9 | 1 | 0 | 0/64 / 0 | False |
| 10000 | tcp | 10000.0 / 9999.6 | 54.5 / 148.8 | 910.3 | 0 | 0 | 1/64 / 0 | True |
| 10000 | tls | 9999.6 / 9999.5 | 56.7 / 154.9 | 910.3 | 3 | 0 | 3/64 / 0 | False |
| 25000 | tcp | 24999.5 / 24998.9 | 69.2 / 267.7 | 947.2 | 4 | 0 | 6/64 / 0 | False |
| 25000 | tls | 24998.9 / 24998.9 | 70.6 / 287.0 | 919.4 | 11 | 0 | 5/64 / 0 | False |
| 50000 | tcp | 49997.7 / 49997.7 | 95.1 / 537.2 | 1036.0 | 23 | 0 | 20/64 / 0 | False |
| 50000 | tls | 49998.2 / 49997.3 | 93.2 / 506.1 | 1025.7 | 17 | 0 | 18/64 / 0 | False |
| 100000 | tcp | 468.2 / 58.6 | 548.0 / 1542.4 | 1934.5 | 995318 | 4096 | 0/64 / 1 | False |
| 100000 | tls | 442.3 / 32.7 | 472.0 / 928.6 | 1352.0 | 995577 | 4096 | 0/64 / 1 | False |

Actual-send latency начинается с реальной отправки. Scheduled latency включает
generator lag от запланированного slot и помогает видеть coordinated omission.
Open-loop не ждёт предыдущего receive; ограниченные queues/in-flight state делают
неспособность генератора поддерживать rate явной, а не скрывают её.

`valid=false` из-за нескольких final cutoff misses не означает потери отправленных
messages. Такие distributions можно описывать с achieved rate, но comparator исключает
их как доказательство полностью предъявленного target. При overload missing deliveries
и disconnects считаются явно; они не входят в успешный throughput. Rates после раннего
disconnect усреднены по всему measurement interval и не означают мгновенную скорость
непосредственно перед отказом.

| Additional target/s | Transport | Interval sent/s / recv/s | Actual p99, µs | Missing / overflow | Sampled peak |
| ---: | --- | ---: | ---: | ---: | ---: |
| 60000 | tcp | 57836.6 / 57427.0 | 668.7 | 4096 / 1 | 38/64 |
| 60000 | tls | 9854.3 / 9444.7 | 928.6 | 4096 / 1 | 39/64 |
| 75000 | tcp | 51257.6 / 50848.0 | 506.1 | 4096 / 1 | 31/64 |
| 75000 | tls | 20049.1 / 19639.5 | 542.6 | 4096 / 1 | 21/64 |

Уточняющие 60k/75k runs также завершились overflow/disconnect. В этой сетке около 50k/s — последняя точка без потерь и overflow, но с небольшими cutoff misses; это условная верхняя наблюдённая скорость этой конфигурации, не доказанный maximum sustainable rate сервера. P99 рос от ~0.1–0.15 ms при низких rates до ~0.5 ms на 50k/s. Точный knee требует повторов и более плотной сетки около порога, с контролем scheduler bursts и генератора.

Дополнительный 5s run со snapshot первой observed overflow log показывает скорость
до disconnect, отдельно от усреднения оставшегося пустого measurement interval:

| Target/s | Transport | First overflow after, ms | Completed writes / deliveries before log | Pre-overflow send/s / receive/s |
| ---: | --- | ---: | ---: | ---: |
| 60000 | tcp | none within 5s | — | — |
| 60000 | tls | 2511.44 | 150683 / 150603 | 59999 / 59967 |
| 100000 | tcp | 23.96 | 2365 / 2263 | 98722 / 94464 |
| 100000 | tls | 5.82 | 545 / 456 | 93628 / 78338 |

Rates здесь cumulative от начала measurement до log, не instantaneous last-bucket
скорость. Log появляется после обнаружения overflow/cleanup; часть deliveries
ещё может быть in flight. Кратковременные 94k delivered/s перед отказом за 24 ms
не означают sustainable 94k/s. Тот 60k TCP run доставил все 299989 sent messages,
но имел 24 cutoff misses; предыдущий 10s TCP run на том же target переполнился.
Зависимость от длительности и scheduler bursts исключает точную capacity-константу
по одному проходу сетки.

Наблюдаемый queue overflow подтверждает overload этой co-resident конфигурации.
Clients, validation и server делят CPU/GC; это не изолированный server maximum.
Periodic queue sampling может пропустить короткий пик; overflow log counters важнее
нулевого sampled peak. Default recipient event queue — 64 events, limits не ослаблялись.

## CPU, allocations и GC

| Workload | Transport | CPU s / wall s | Sender messages/CPU s | B / allocs per sender message | Alloc MiB/s | GC count / GC CPU s |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| payload/key | tcp | 13.91 / 10.00 | 15080 | 4265 / 82.2 | 85.3 | 473 / 0.69 |
| payload/key | tls | 14.73 / 10.00 | 12613 | 4307 / 82.3 | 76.3 | 489 / 1.05 |
| payload/clipboard-1MiB | tcp | 14.05 / 10.00 | 96 | 17326352 / 153.5 | 2227.4 | 2713 / 5.03 |
| payload/clipboard-1MiB | tls | 14.86 / 10.00 | 74 | 17297817 / 164.5 | 1819.5 | 2209 / 4.10 |
| payload/arbitrary-arrays | tcp | 14.72 / 10.00 | 1378 | 189299 / 6498.0 | 366.1 | 1983 / 3.68 |
| payload/arbitrary-arrays | tls | 14.64 / 10.00 | 1399 | 190558 / 6498.1 | 372.3 | 1785 / 3.82 |
| fanout/100 | tcp | 134.19 / 10.00 | 226 | 183361 / 4540.8 | 529.9 | 265 / 6.06 |
| fanout/100 | tls | 135.19 / 10.00 | 222 | 183365 / 4541.0 | 523.7 | 247 / 5.31 |
| channels/hot | tcp | 9.86 / 10.00 | 5071 | 46790 / 1398.4 | 223.1 | 245 / 1.98 |
| channels/hot | tls | 12.41 / 10.00 | 4030 | 47149 / 1398.5 | 224.8 | 237 / 1.90 |

Все E2E resource metrics относятся к **server + benchmark clients + validation +
instrumentation**. Messages/CPU-second относятся к sender messages; deliveries/CPU-second
при fan-out имеют другой знаменатель. Component B/op и allocs/op локализуют production
costs. Runtime total allocations считаются по интервалу, profiles sampled/cumulative.

Runtime pause statistics на Windows могут быть квантованы независимо от QPC.
Нулевые pauses не доказывают отсутствие GC; GC CPU не равен stop-the-world pause time,
а суммарный pause time не является автоматическим объяснением p99. GC не принуждался
в E2E harness; стандартный `testing.B` управляет собственным GC.

Representative CPU runs: 30s, 10 channels, fan-out 4, mixed, target 5k/s,
раздельные свежие TCP/TLS processes. TCP доставил 600000 deliveries, TLS — 599992
при двух generator cutoff misses; потерь/overflow не было. Profile distributions
диагностические и не включены в основную latency таблицу.

| CPU profile frame | TCP cumulative % | TLS cumulative % |
| --- | ---: | ---: |
| Server.handleClient | 14.33 | 15.69 |
| handleClientChannelEvent | 13.48 | 14.80 |
| client.send | 12.84 | 14.16 |
| Server.readFromClient | 6.06 | 5.26 |
| unmarshalClientMessage | 2.67 | 2.54 |
| benchmark peer.readLoop | 21.52 | 35.25 |
| runtime.schedule | 35.16 | 25.83 |
| encoding/json/v2.Unmarshal | 17.38 | 30.75 |

Это вложенные cumulative frames, не независимые CPU доли. В flat TCP лидировали
runtime.semawakeup (20.32%), runtime.cgocall (15.46%) и runtime.semasleep (10.34%);
TLS — cgocall (13.97%), semasleep (11.05%), semawakeup (10.92%). На Windows
`cgocall` здесь также обслуживает syscall wrappers при CGO=0. Профиль показывает
существенную стоимость runtime/OS scheduling, socket IO и JSON, но не доказывает,
какой из них вызывает отдельный latency tail.

TLS Conn.Write имел 12.73% cumulative samples, halfConn.encrypt — 0.72%:
первая цифра включает запись в socket и другие вложенные действия, её нельзя
называть чистой стоимостью шифрования. Validator JSONEqual теперь имел 1.76% TCP /
1.33% TLS cumulative CPU и не добавлял allocations в equality traversal.

Sampled alloc_space всё ещё dominated клиентским JSON decode: peer.readLoop
69.16% TCP / 68.28% TLS. Production readFromClient: 18.31/17.96%, recipient handler:
11.76/12.78%. Reflect slice growth/new и JSON buffers — основные allocation leaves.
Это подтверждает, что E2E B/message нельзя приписывать только серверу. Runtime
allocation delta mixed profile был ~77.4/77.9 KiB на sender message при fan-out 4;
GC count 1226/1114, GC CPU 7.82/7.24s. Для key component allocations существенно
меньше, а большие clipboard/array workloads создают заметный GC pressure.

В финальном standalone block profile (10s) 84.69% суммарного delay приходится
на selectgo, 13.91% — chanrecv2. Server.handleClient и channel.start в основном
ждут следующую работу; это не доказательство медленного channel handoff.
Mutex profile преимущественно относит delay к runtime.unlock; root-frame
атрибуция runtime locks недостаточна для вывода о конкретном application mutex.
Из отдельного trace также извлечены `net.pprof`, `sync.pprof`, `sched.pprof`
и соответствующие `*-top.txt`. В этом TCP workload network wait относится
к socket reads через FD.execIO/Read и ожиданию следующего frame. Это не доказывает
write bottleneck; обычное ожидание работы не равно дополнительной relay latency.
Block profile сам по себе не измеряет network I/O wait, поэтому trace дополняет его.

Изначальный одновременный CPU + every-block + every-mutex режим зависал
не всегда; watchdog сохранил stacks, но native runtime причина не установлена.
Новая orchestration разносит samplers по свежим processes (block 1ms, mutex 1/10).
5s probe и два полных 10s набора profiles завершились без hang, missing,
disconnect или overflow. Некоторые имели 1–2 cutoff misses и честный invalid-load
marker; profile timings не считаются baseline. Trace сохранил 40000 validated
deliveries отдельного 2s run, без потерь и cutoff misses.

Block/mutex delay — суммарное ожидание многих goroutines, не доля wall-clock latency.
Ожидание idle channel/select обычно нормально; attribution из профилей нельзя складывать
по вложенным cumulative frames. Execution trace сохранён отдельно от main measurements.

## Память и idle/active clients

| Clients | Transport | Heap / RSS, MiB | Heap / RSS delta per client, KiB | Objects | Goroutines | Idle CPU s / GC |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 100 | tcp | 8.62 / 67.64 | 77.1 / 575.2 | 10569 | 554 | 0.000 / 0 |
| 100 | tls | 10.95 / 71.43 | 100.5 / 607.0 | 29543 | 554 | 0.000 / 0 |
| 500 | tcp | 41.01 / 119.79 | 81.8 / 221.8 | 69217 | 2754 | 0.000 / 0 |
| 500 | tls | 55.11 / 131.41 | 110.6 / 244.3 | 188093 | 2754 | 0.000 / 0 |
| 1000 | tcp | 80.78 / 184.08 | 81.6 / 176.7 | 132327 | 5504 | 0.000 / 0 |
| 1000 | tls | 89.75 / 212.95 | 90.7 / 205.7 | 144090 | 5504 | 0.000 / 0 |

Это свежие processes с paired joined clients, N/2 channels и 5s observation после
1s warm-up. Heap delta/client включает обе стороны loopback и не является размером
production client struct; snapshot может содержать garbage до следующего GC. RSS включает
runtime/code/stacks и зависит от амортизации fixed process cost.

Незавершённые startup clients ограничены production StartupLimit=128: для
запрошенных 500/1000 suite открыл 128 и явно записал 372/872 unattempted.
Это не измерение памяти тысячи pending connections и не отключение лимита.
Их steady observation сократился до ~7.9s, чтобы не включать startup timeout.

Active 500/1000 joined clients (250/500 channels) при общем target 5k/s
доставили все отправленные сообщения; 1–2 final offers были пропущены генератором.
В длинном suite p99 составил около 104/103 µs TCP и 109/109 µs TLS.
Heap/RSS для active snapshots приведены выше; они включают bounded measurement
state и ранее выделенную память, поэтому не заменяют isolated idle scaling.

В большом suite отрицательные memory deltas возможны из-за освобождения прежнего heap.
Они не означают отрицательную стоимость клиента; для memory scaling нужны isolated runs.

## Admission, recovery, rejection и message limits

| Scenario | Transport | Samples | p50 / p99, µs | Batch decisions/s | Alloc B / sample | Mean cleanup, µs |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| lifecycle/ordinary | tcp | 64 | 593.4 / 1266.5 | 39781 | 6719 | 1263.0 |
| lifecycle/ordinary | tls | 64 | 1453.0 / 1693.2 | 35766 | 8272 | 1090.0 |
| lifecycle/protected-complementary | tcp | 64 | 414.7 / 878.4 | 64516 | 5490 | 1263.6 |
| lifecycle/protected-complementary | tls | 64 | 1863.4 / 2498.0 | 25294 | 7054 | 1183.2 |
| lifecycle/recovery | tcp | 64 | 1424.4 / 1824.7 | 33319 | 6726 | 1250.6 |
| lifecycle/recovery | tls | 64 | 1355.3 / 1933.3 | 31730 | 7881 | 1060.0 |
| lifecycle/protected-reconnect | tcp | 64 | 1302.4 / 1884.7 | 32289 | 6450 | 1275.8 |
| lifecycle/protected-reconnect | tls | 64 | 2413.6 / 2689.0 | 22547 | 8466 | 1156.5 |
| lifecycle/capacity-reject | tcp | 1 | 65.3 / 65.3 | 8961 | 41536 | — |
| lifecycle/capacity-reject | tls | 1 | 75.9 / 75.9 | 7692 | 25760 | — |
| lifecycle/reject-burst | tcp | 64 | 910.3 / 1180.4 | 46808 | 15826 | — |
| lifecycle/reject-burst | tls | 64 | 709.8 / 1448.0 | 32091 | 16892 | — |
| lifecycle/tcp-connect | tcp | 64 | 183.4 / 290.0 | 4786 | 17460 | — |
| lifecycle/tls-handshake | tls | 64 | 526.6 / 876.9 | 1716 | 93070 | — |
| lifecycle/startup-join | tcp | 64 | 273.1 / 538.1 | 3288 | 21438 | — |
| lifecycle/startup-join | tls | 64 | 593.4 / 993.0 | 1550 | 97620 | — |
| lifecycle/oversized-offender | tcp | 64 | 190437.4 / 203715.2 | — | 20984340 | 1401.9 |
| lifecycle/oversized-offender | tls | 64 | 134432.4 / 167098.2 | — | 21324766 | 1651.6 |

Admission decision excludes TCP/TLS setup; startup-join включает их. Concurrent finite
batches измеряют time-to-decision и batch throughput, не steady-state admissions capacity.
При 64 samples p99 близок к sample maximum: это descriptive batch tail,
а не стабилизированная оценка редкого percentile. Для неё нужны большие повторные batches.
Короткий Windows CPU accounting interval может дать ноль: это недостаточное разрешение
для CPU/admission, не бесплатная обработка. Allocations/admission и cleanup доступны в JSON.

64 rejected clients проверялись параллельно. Решение занимало ~0.81 ms mean TCP
и ~0.77 ms TLS; полный speech lifecycle длился ~5.00s на всю группу, не 64 × 5s.
Получено 384 speech frames на каждый transport. Decision allocations составили
645088/658944 B на batch, а весь пятисекундный lifecycle — 1012832/1081080 B.
В окне наблюдалось 64 pending rejection clients и 333 goroutines всего;
whole-process heap/RSS были 16.8/130.6 MiB TCP и 13.0/125.0 MiB TLS.
Эти snapshots после предыдущих scenarios не дают чистую memory/client оценку.

64 pending TLS handshakes наблюдались отдельно: heap 23.0 MiB, RSS 125.2 MiB,
68 goroutines; после закрытия batch cleanup занял 2.57 ms. Для 64 pending
startup clients cleanup занял 2.55 ms TCP и 3.63 ms TLS. Pending counters вернулись
к нулю; setup/observation resources и GC доступны в JSON.

| Boundary JSON value bytes | TCP single transfer, ms | TLS single transfer, ms |
| ---: | ---: | ---: |
| 73 | 0.0468 | 0.0548 |
| 1077 | 0.0755 | 0.0576 |
| 1048629 | 5.8669 | 4.6775 |
| 4193280 (limit − 1024) | 28.0607 | 43.7657 |
| 4194304 (exact limit) | 23.7525 | 30.8681 |

64 offenders с 4194305-byte messages были отключены с ожидаемой причиной.
Batch занял 207 ms TCP / 169 ms TLS, CPU 4.81/3.95s, allocations 1.25/1.27 GiB;
cleanup после batch — 1.40/1.65 ms. After snapshots: heap 652/579 MiB,
RSS 997/1351 MiB. Это подчёркивает resource cost concurrent oversized traffic,
а не throughput корректно доставленных сообщений. Близкие размеры и одиночные
transfers не обязаны давать монотонные latency samples.

Boundary cases с одной delivery проверяют корректность и стоимость одного transfer;
они не дают устойчивого p99. Большой concurrent offender batch — robustness/resource
workload, его время включает передачу oversized payload и наблюдение server disconnect.

## Повторы и дальнейшие build experiments

| Metric | TCP A → B | Delta % | TLS A → B | Delta % |
| --- | ---: | ---: | ---: | ---: |
| p50 µs | 36.59 → 39.62 | 8.29 | 39.23 → 51.83 | 32.13 |
| p95 µs | 65.16 → 69.17 | 6.15 | 71.27 → 81.11 | 13.81 |
| p99 µs | 88.71 → 91.40 | 3.03 | 97.02 → 101.97 | 5.10 |
| p99.9 µs | 242.34 → 275.81 | 13.81 | 252.18 → 301.65 | 19.61 |
| deliveries/s | 18587.00 → 17461.90 | -6.05 | 16888.20 → 14334.70 | -15.12 |
| B/message | 4268.20 → 4263.51 | -0.11 | 4311.11 → 4308.58 | -0.06 |
| messages/CPU s | 13487.17 → 11991.07 | -11.09 | 11377.38 → 9646.91 | -15.21 |
| heap MiB | 2.58 → 2.97 | 15.35 | 2.69 → 2.35 | -12.74 |
| RSS MiB | 57.47 → 57.88 | 0.71 | 58.57 → 58.65 | 0.13 |

Это descriptive A/B comparison двух одинаковых runs, не significance test. Live heap
может заметно меняться при стабильных B/message/RSS из-за фазы естественного GC. Для
небольших улучшений нужны больше repetitions и benchstat для standard microbenchmarks.
В этих двух повторах p50 TLS изменился на 32%, p99 — на 5%, throughput — на 15%,
при почти одинаковых B/message и RSS. Среда не была достаточно стабилизирована,
чтобы оценивать небольшое улучшение по одному run. Причины разброса (frequency,
thermal state, scheduler/background load) этим измерением не разделены.

Обычная release сборка Go уже оптимизирована; универсального аналога «включить -O3»
нет. Текущий Mage build не включает PGO или повышенный CPU baseline. Практические
следующие эксперименты — representative PGO и отдельные ISA variants, сохраняя
идентичную workload matrix. [Go PGO](https://go.dev/doc/pgo) приводит 2–14% улучшений
для representative benchmarks Go 1.22; это не прогноз latency NVRemoted/Go 1.27.1.
[CPU architecture levels](https://go.dev/wiki/MinimumRequirements) могут разрешить
дополнительные инструкции, но не гарантируют выигрыш и ограничивают совместимость.

При незагруженном пути Amdahl даёт максимум экономии
`L × p × (1 − 1/S)`, где p — доля исходной wall-clock latency L на
оптимизируемом последовательном критическом пути, S — ускорение этой части
при прочих равных. Cumulative CPU profile всего процесса не определяет p;
его проценты нельзя напрямую подставлять в эту формулу.
CPU optimization не убирает WAN/scheduling/другие фиксированные затраты. Около saturation
экономия CPU может нелинейно уменьшать backlog и менять threshold disconnects; это требует
отдельного A/B experiment, а не экстраполяции latency floor.

CPU profiles реалистичного TCP/TLS mixed server workload подготовлены, но они содержат
clients и shared JSON/TLS/runtime weights. Для production PGO selection предпочтителен
server-only профиль; ничего из этих профилей не включено в release pipeline.

## Проверки и review

`mage test`, `mage testRace` (по 100 repetitions), `benchSmokeRace`, `check`,
`benchCheck`, `benchCompileAll`, `buildAll` прошли. Tagged harness cross-compiled для
Linux/Windows/macOS amd64/arm64. Performance suite в CI не добавлена. Независимый review
проверил correctness и methodology: clocks, omission, generator limits, loss validation,
allocation contamination, bounded state, TLS setup, cleanup и attribution.







