# The probe: verbose on demand

`atchung-probe` is the profiling seam the whole stack reports through. It is off unless asked, it costs
essentially nothing when off, and when it is on it answers three questions in one block of text:

- **how long** — spans, with percentiles and a self time
- **how many** — counters, with a peak
- **what is still open** — a resource ledger, which is the leak half

It lives in the Atchung repo because Atchung is the only artifact the whole graph already roots on, and it is
a **separate module with no dependencies of its own** so that a layer can take the probe without taking the
event bus. That is what lets `vexelray-vulkan`, `vastir` and `supir` carry instrumentation at all.

## Turning it on

The switch is one setting, available as a system property or — identically, on every operating system — an
environment variable. The property wins if both are set.

```bash
-Dprobe=all                  # every lane
-Dprobe=frame,gpu            # two lanes
PROBE=all                    # the same thing, and the form a native binary understands
```

| property | env | what it does |
|---|---|---|
| `probe` | `PROBE` | `off` (default), `all`, or a comma-separated list of lanes |
| `probe.out` | `PROBE_OUT` | write to this file instead of stdout |
| `probe.trace` | `PROBE_TRACE` | one line per event as it happens, not just the rollup |
| `probe.every` | `PROBE_EVERY` | also print a rollup every N milliseconds |
| `probe.slow` | `PROBE_SLOW` | print any span over N milliseconds the moment it closes |
| `probe.format` | `PROBE_FORMAT` | `text` (default) or `csv` — the correlation log, [below](#reading-a-run-back-the-correlation-csv). Implies `probe.trace` |
| `probe.stacks` | `PROBE_STACKS` | record allocation stacks in the resource ledger |
| `probe.top` | `PROBE_TOP` | rows per lane in the rollup (default 12) |

A rollup is always printed at exit, through a shutdown hook. `probe.every` exists because a shutdown hook
does not run on a hard kill, and the run worth profiling is often the one that ends that way.

### From the calculator demo

```bash
mvn compile exec:exec -Pprofiler
mvn compile exec:exec -Pprofiler "-Dprobe=gpu,frame" "-Dprobe.out=run.log"
```

The `profiler` profile sets `probe=all`, a rollup every 2 s, a 16 ms slow threshold and ledger stacks.
Anything on the command line overrides it.

## The lanes

| lane | what reports there |
|---|---|
| `bus` | Atchung publish, fan-out, mailbox depth, drops, pump drains |
| `state` | `State<T>` commits and CAS retries |
| `time` | the Kronometer kernel: ticks, batches, handoffs, overrun, and the baton's `gate.open` / `gate.woke` |
| `anim` | effects and graph invalidation |
| `input` | Tactroller's OS snapshot, the publish, drag recognition |
| `frame` | the GUI frame loop, and the wait between frames |
| `layout` | dispatch, drain, flex layout, geometry resolution |
| `draw` | canvas emission, vertex and run counts |
| `gpu` | fence wait, acquire, submit, present, swapchain rebuilds, native resource lifetimes, and the marks `frame.image`, `frame.latency`, `dwm.start` / `dwm.presented` |
| `shader` | Supir parsing, SPIR-V lowering, shader-cache hits and misses |
| `app` | yours; nothing in the framework publishes here |

### Marks the stack publishes for reading a stall

Marks are instants, not spans, and **exist only in the trace** (`-Dprobe.format=csv`, which an automation run turns on):
the rollup counts them and nothing more. All of them are guarded by `Probe.ON`, so a normal build pays nothing.

| lane | kind | `detail` | answers |
|---|---|---|---|
| `frame` | `frame.present`, `loop.park` | frame number; the park's budget | was a gap covered by a park (see *The gap hunt*) |
| `frame` | `wake`, `wake.post` | which tree; what was nudged | what ended a park, and from which thread |
| `time` | `gate.open` | `-> <thread>` | the sender of a baton pass between the kernel and a shred |
| `time` | `gate.woke` | empty | the receiver's first instruction after it. **The gap between the two is the scheduler's** (a virtual thread waiting for a carrier, a carrier waiting for the OS), which no span on either side can see. On the machine measured it is 50 to 100 us |
| `gpu` | `frame.image` | `frame=N image=I acquire=R` | which swapchain image a frame got. The same index on consecutive frames was seen in steady state on Windows, where the compositor releases the image at once |
| `gpu` | `frame.latency` | `frame=N slot=S submit-to-fence Tus` | submit to the fence's wait returning, from the CPU's side. Climbing over a burst and then plateauing at a multiple of the refresh interval is a **full present queue**, not a slow GPU |
| `gpu` | `dwm.start`, `dwm.presented` | `vblank=… phaseUs=… composed=… submitted=… confirmed=… late=… outstanding=…` | where in the refresh interval a frame began and was presented. **Read `phaseUs`**, which counts 0 to the interval (6944 at 144 Hz): five frames whose phases ascend within one interval were produced faster than they can be shown. The counts only follow this application's own presents. Windows only |

### Reading a stall: the method that found the burst

A late frame is a symptom, and the order that works is: find it, find what the loop was doing, then ask of each
candidate cause whether the rows say it was *busy* or *waiting*.

1. **Find the gap.** `loop.park` rows say whether a gap was allowed; a `frame.present` after a gap the park does not
   cover is the stall.
2. **Read the rows inside it, in time order.** Frame-loop rows (`frame`, `time`, `gpu`) with the thread name beside them
   say which thread was doing what. A `wait fence` of 20 ms with a `queue submit` of 50 us before it is the CPU waiting,
   not working.
3. **Separate the GPU from the queue in front of it.** `frame.latency` small for most frames and then one large is a
   queue draining. Large for every frame is a slow GPU.
4. **Put the frames on the display's clock.** `dwm.*` `phaseUs` on consecutive frames. A burst is frames whose phases
   rise inside one interval; the wait that follows is the display catching up at one frame per refresh.
5. **Prove it by removing it.** Change one thing (`-Dvexelray.present.flush=true`, a frame ceiling, a swapchain image
   count) and look at the same rows. A cause that is only inferred from timings is one experiment short.

The worked case, with the numbers, is `vexelray-framework/docs/architecture.md`, *what the stall probes found*.

## Reading a report

```
lane   span                                count      total      mean       p50       p99       max       self
frame  wait for events                        28      4.20s  149.93ms  167.77ms  167.77ms  200.67ms      4.20s
frame  frame                                  29    63.18ms    2.18ms    1.05ms   20.97ms   24.47ms     3.61ms
gpu    present frame                          29    59.57ms    2.05ms    1.05ms   20.97ms   22.79ms    12.85ms
gpu    wait fence                             29     2.58ms    89.0us    57.3us   655.4us   667.0us     2.58ms
```

Spans **nest**, and `self` is the column that localises a cost: `frame` totals 63 ms but only 3.6 ms of that is
the loop's own work — the rest is in `present frame`, whose own self time is 12.85 ms. Read down the self
column, not the total column, to find where time actually went.

**p99 and max, not mean.** A loop averaging 2 ms that spikes to 25 ms is a visibly broken loop with a healthy
average, and every stutter complaint has that shape. Durations go into a log-scale histogram (four buckets per
octave, ~12% resolution), so percentiles cost a fixed 2 KB per name and no allocation per sample.

Three rows are worth knowing by name:

- **`gpu wait fence`** — one frame is in flight, so this is the CPU waiting on the GPU to finish the previous
  frame. A large fence wait next to a small everything-else means the application is not the problem.
- **`frame wait for events`** — the loop parked on an empty queue. This is *supposed* to dominate: a
  render-on-demand application spends its life here. It is an **idle span** (`Probe.idleZone`), so it is
  measured but never trips `probe.slow`.
- **`<topic> depth`** in the counter table — a pumped mailbox's occupancy at publish time. Read the `max`
  column. A mailbox that averaged 2 and once reached its capacity is one publish away from dropping, or did.

### The resource ledger

```
resource                                     opened     closed       LIVE
AtlasTexture                                      2          0          2
SampledColorTarget                               37         12         25
```

A leak in this stack is rarely a leak of Java objects — it is a swapchain that outlived its window, a render
target remade on every resize, a subscription nothing closed. All of those own a native handle behind a
`long`, so a heap dump cannot see them. The ledger registers on construction and strikes off in `close()`, and
**a kind whose LIVE count grows across two rollups of the same workload is the leak.** With `probe.stacks=true`
each outstanding entry carries the stack that allocated it.

The ledger holds a **strong** reference to everything registered. That is deliberate: a weak one would let the
object under investigation vanish before it could be named, and "something leaked but it has been collected"
is not a report.

## Reading a run back: the correlation CSV

A log a program reads is not a log a person watches, and the two want opposite things: columns that line up,
or fields that split. `probe.format=csv` emits the same events as one row per line.

```bash
-Dprobe=all -Dprobe.format=csv -Dprobe.out=run.csv
```

`csv` implies `probe.trace`, because a correlation log holding only the spans that happened to be slow is not
one. The file opens with its header and then holds rows and nothing else:

```
seq,t_mono_ns,t_wall,thread,lane,kind,detail
1,4185700,2026-09-02T04:31:07.882145Z,main,bus,input depth,3
2,4213900,2026-09-02T04:31:07.882173Z,Loop,frame,frame.present,#41
3,4219100,2026-09-02T04:31:07.882178Z,Loop,frame,loop.park,16ms
```

The banner and every rollup go to **stderr** instead — including `Probe.dump()` — so the data file needs no
skip-the-prose rule from any consumer, and the header is the one line they can all be told about.

Four decisions in that row are worth knowing, because reading the file depends on them:

- **`t_mono_ns` is the sort key, never `seq`.** It is monotonic, so it cannot step sideways and sort two events
  into an order that never happened. `t_wall` is for people, and for joining this file against anything
  outside the process.
- **`seq` is there so that loss is *detectable*.** A gap that is a dropped line looks exactly like a gap that
  is a stall unless the numbering says which it was. It also breaks ties when two threads land in the same
  nanosecond.
- **One event is one physical line, always.** RFC 4180 permits a raw newline inside a quoted field, and a
  single stack trace written that way ends the file's usefulness: `sort`, `grep -n` and `awk` stop describing
  events and start describing fragments of them. Newlines are flattened to spaces; commas and quotes are
  quoted the way a reader expects.
- **`detail` is last**, so a naive split on commas keeps working when it contains one.

The format is decided in the same static initialiser as the switch, so one process is one format for its whole
life. There is one writer and one file on purpose: parallel writers are what make correlation lie, because
separate buffers flush at different times and the order on disk stops being the order that happened.

### `csvview`

`CsvView` reads the file back. It lives beside the writer because a reader in another repository would drift
from the *format* the first time it changed — the same failure one step further out.

```bash
java -cp atchung-probe/target/classes sibarum.probe.CsvView run.csv --gaps
```

```
csvview <run.csv> [options]
  --gaps [ms]     stretches with no frame in them, judged against the preceding park
  --lane <names>  comma-separated lanes to keep
  --kind <text>   kinds containing this
  --thread <text> threads containing this
  --grep <text>   rows mentioning this anywhere
  --tail <n>      only the last n rows
```

`--gaps` defaults to 100 ms. The filters combine as "and" — an investigation narrows — and match
case-insensitively, since the producer chose the casing and the person at the terminal should not have to
guess it; `--lane` is the exception, an exact set, because a lane is a closed vocabulary and a partial match
there would silently widen the answer. Rows are returned in time order, and a torn last line is skipped
rather than fatal: a run that was killed mid-write is the normal case here.

Missing sequence numbers are reported on stderr first and unconditionally. Every other answer is computed
from the rows that are present, so a reader who is not told about the missing ones is being invited to
conclude something from a hole.

### The gap hunt

The highest-value question this format serves, and the one thing `awk` cannot answer on its own, because **a
gap is only a finding if the loop was supposed to be running.** Two marks carry that rule, and a loop has to
emit them under these exact names:

| kind | what it has to mean |
|---|---|
| `frame.present` | emitted every frame, so a stretch without one is a stretch with no frames |
| `loop.park` | the loop went to sleep, with `detail` saying how long it was *allowed* to sleep — a millisecond count (`16ms` or `16`), or `forever` |

Nothing in this repository publishes them; a frame loop does, with `Probe.mark(Lane.FRAME, "frame.present",
...)`. The park is what makes a verdict possible: a render-on-demand loop parks indefinitely when nobody is
looking, so without it a thirty-second doze and a thirty-second hang are the same silence. A gap covered by
the park that precedes it is expected, and a gap that is not is a stall — and the row before it is the
suspect, which is the whole reason the log is one file in time order rather than several.

```
1 gap of 100ms or more:

  2026-09-02T04:31:11.113806Z for 2103.4ms  [STALL - parked on 16ms, which does not cover it]
    from  41,7103400000,2026-09-02T04:31:11.113806Z,Loop,frame,frame.present,#41
    last  42,7180200000,2026-09-02T04:31:11.190604Z,worker,app,save,writing 40MB
    to    43,9206800000,2026-09-02T04:31:13.217206Z,Loop,frame,frame.present,#42
```

`read`, `filter`, `gaps` and `loss` are public, because reading a run back is not only this command's job: a
test that asserts what its own run recorded is doing the same thing, and would otherwise reimplement it
slightly differently.

## Instrumenting new code

```java
if (Probe.ON) Probe.count(Lane.BUS, someName);          // the guard comes first, always

try (Zone z = Probe.zone(Lane.APP, "recompute")) { ... } // spans need no guard unless the name is built

Probe.opened(Lane.GPU, "MyTarget", this);                // in the constructor
Probe.closed(Lane.GPU, "MyTarget", this);                // in close()
```

Three rules, and they are the whole discipline:

1. **`Probe.ON` is a `static final boolean` assigned in a static initialiser.** Not a compile-time constant, so
   downstream modules do not bake it in and the switch stays a runtime switch — but the JIT sees a field that
   can never change and folds `if (Probe.ON)` out of every method it compiles.
2. **Guard before you build an argument.** `Probe.count(lane, topic.name() + " depth")` computes the string
   whether or not anything is profiling. Cache such a label in a field at construction time, guarded by
   `Probe.ON` (see `PumpedReg`), or use a name the caller already holds.
3. **Nothing throws.** A probe that fails must lose a line, never take the process with it.

## What it costs

Off: a predictable branch on a folded constant, which is to say nothing measurable.

On: one `System.nanoTime()` pair per span, a `ConcurrentHashMap` lookup on a bare name (the maps are per lane,
so no key has to be composed), and lock-free adders. Zones are **pooled per thread and per nesting depth** and
never allocated — a profiler that allocates on the path it is measuring changes the allocation rate it was
installed to explain. Output is serialised on the sink, which is a lock taken once per traced line and once per
report, never per span.
