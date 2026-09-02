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
| `time` | the Kronometer kernel: ticks, batches, handoffs, overrun |
| `anim` | effects and graph invalidation |
| `input` | Tactroller's OS snapshot, the publish, drag recognition |
| `frame` | the GUI frame loop, and the wait between frames |
| `layout` | dispatch, drain, flex layout, geometry resolution |
| `draw` | canvas emission, vertex and run counts |
| `gpu` | fence wait, acquire, submit, present, swapchain rebuilds, native resource lifetimes |
| `shader` | Supir parsing, SPIR-V lowering, shader-cache hits and misses |
| `app` | yours; nothing in the framework publishes here |

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
