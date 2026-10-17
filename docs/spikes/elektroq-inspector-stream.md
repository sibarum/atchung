# Spike: the Inspector's live stream over elektro-Q

Status: **spike, done 2026-10-17.** The code is throwaway, on branch `claude/elektroq-stream-spike-048b76` only, in
`elektroq/elektroq-spike-inspector-stream` (built only with `-Pspike`). The raw outputs every number below comes from
are in that module's `results/`, and a copy is kept on `develop` beside this report, in
[`elektroq-inspector-stream-results/`](elektroq-inspector-stream-results/), so `results/x.txt` below names a file there.

The question: should the VexelRay Inspector's live stream (vexelray-gui `docs/plans/flight-recorder.md` §6) be
intrabuf records over elektro-Q, instead of the CSV text rows §6 proposes? Agents keep the text line protocol either
way. This spike decides only the Inspector's channel.

## Verdict

**Adopt, with changes.** Text is fast enough, so speed is not the reason to switch. The reasons to switch are the
structured snapshots, the typed queries, the version checks, and the security of a socket file. A UDS endpoint
works on this machine, on the JVM and in a native image.

- **Text can keep up.** A real full-trace run peaks at **94.8k rows/s over 100 ms** (68.5k over 1 s, 287k over
  10 ms). CSV over loopback sustains **2.5M rows/s**, 27 times the 100 ms peak, limited by the Inspector's parsing.
  Q3's condition, "a measured run shows text cannot keep up", **is not met**.
- **elektro-Q is cheaper and leaner.** It sustains **6.8–10.1M rows/s over UDS** (72–107 times the 100 ms peak). It
  costs a third of the CPU per row on the application side (63–65 ns against 176 ns), a sixth on the Inspector side
  (41–45 ns against 282 ns), and 51–67 bytes a row against 102.
- **Those savings are small at real rates.** At 100k rows/s the stream costs about **0.65% of one core** in the
  application as elektro-Q and **1.7%** as text. Both deliver a row in well under a frame.
- **What decides it** is what text does badly. It has no shape for snapshots of maps and lists (mailbox depths, kron
  state). It has no typed reply for a `why`, and no loud refusal when the two builds disagree on a schema. It
  flattens newlines in `detail`. Its socket is a loopback port that any local process can dial. elektro-Q handles
  all of these, it is the direction the stack has already taken (automation.md, *a remote peer across an
  ElektroBridge*), and it cost no reachability configuration under native-image.
- **The changes needed** (§5): put the transport in a new module, never in `vexelray-gui-automation`. Make the ring
  the queue, with one sender thread per connection. Never broadcast. Choose list-of-rows records. Fix three gaps in
  elektro-Q (a blocking send with no timeout, stale socket files, no peer events on `Conduit`).

## Machine and method

- **Machine:** Intel Core Ultra 7 255HX (20 logical cores), 15.4 GB, Windows 11 Home 10.0.26200. Oracle GraalVM
  25.0.3+9.1 (HotSpot with JVMCI) for the JVM runs, and `native-image` 25.0.3 with Visual Studio 18 for the native
  run. On AC power, other sessions idle.
- **Real rows:** the calculator demo (`calculator-vexel-demo` at 8c21530), run with `PROBE=all PROBE_FORMAT=csv`
  and driven by ottermate from a 30-round script of resizes, zooms, typing and `settle`
  (`results/capture-drive-script.txt`). That gave **160,741 rows in 4.65 s, 383 frames**, 16 MB. A shorter run
  (21,570 rows) and `vexelray-inspector/samples/calculator.csv` were analysed too (`results/rates.txt`).
- **The ring:** `Ring` loads a capture into parallel arrays, one slot per row, holding the strings, as the
  recorder will. The frame stamp is rebuilt as the frame in progress, advanced on each `frame.present`. The bench
  only reads the ring.
- **The sender** is one platform thread playing the recorder's stream sender. It reads slots, builds the batch
  record (or the CSV text), and hands it to the conduit (or the socket). All of its allocation and CPU is charged to
  encoding, including building the `Row` objects and the lists.
- **The receiver** plays the Inspector. It decodes every row to fields (intrabuf decode, or the CSV split with
  `Long.parseLong` and a lane lookup) and reads them.
- **Both ends run in one JVM.** "Process CPU" is therefore both ends together. The sender thread's CPU and
  allocation come from `ThreadMXBean`. On Windows the thread CPU clock ticks in 15.6 ms steps, so CPU percentages
  from the 5 s paced runs are only good to about ±0.3%. The per-row costs from the encode runs are the precise
  figures.
- **Candidates:**
  - **A, `RowBatch`**: `(sentNanos, lostFrom, lostTo, List<Row>)`, where `Row` holds
    `(seq, tMonoNs, tWallNs, frame, thread, Lane, kind, detail)` and `Lane` is an enum.
  - **B, `RowColumns`**: the same data as columns, with seq implicit from `firstSeq`, times and frame as deltas, and
    every column a boxed `List`, because intrabuf has no primitive arrays.
  - **Baseline**: §6's text row, meaning the file's seven columns quoted exactly as `Probe.field` quotes them, plus
    a `frame` column. Sent over a plain socket with one flush per batch, TCP (`TCP_NODELAY`) or UDS.
- **Not covered:** a second process for the Inspector, a recorder ring under concurrent writers, other lanes'
  shapes (`snap`), and Linux. JIT runs only: throughput was not measured in the native image, only correctness and
  latency.

## 1. UDS on Windows

`elektroq-transport-uds` works on Windows 11 with JDK 25's AF_UNIX: on the JVM, and as a native image built with no
configuration in 22.9 s, giving a 16.9 MB exe. Both gave the same answers (`results/uds-check-*.txt`):

| check | JVM | native |
|---|---|---|
| connect (warm) | 0.11–0.13 ms (first 4.6–5.1 ms, class loading) | 0.06–0.10 ms |
| one-way emit | received | received |
| request/reply, p50 / p99 | **25–27 µs / 45–55 µs** | 28–30 µs / 68–76 µs |
| same over TCP loopback | 36–37 µs / 62–67 µs | 41 µs / 122 µs |
| server notices client close | 0.2–1.1 ms | 0.05–0.1 ms |
| client notices server close | 0.5–1.1 ms | 0.45 ms |
| in-flight request when the server closes | fails at once: `Peer 1 disconnected before replying` | same |
| socket file while bound | exists, not a regular file (a reparse point) | same |
| socket file after `close()` | deleted | deleted |
| socket file after a crash (simulated) | **left behind; the next bind fails with `BindException: Address already in use`** | same |
| dialling a path nobody listens on | `ConnectException: Connection refused` | same |
| longest path | **106 characters**; 107 gives `SocketException: Unix domain path too long` | same |
| path with spaces | works | works |

**Where the socket file should live.** Put it beside the discovery file that vexelray-gui `docs/plans/discovery.md`
already plans: `~/.vexelray/automation/<app>-<pid>.sock`. Advertise it in that file as the
`stream=uds:<path>` line discovery.md §3 anticipates. Here that path is about 50 characters, well inside 106. The
discovery sidecar lock tells a reader whether the process is alive, so a stale `.sock` can be deleted safely before
binding. That is the step `UdsTransport` does not take (§4). When `vexelray.home` redirects the folder somewhere
deep and the path would pass 106 characters, fall back to a TCP loopback port and advertise `stream=tcp:<port>`.

**Security.** A Windows AF_UNIX socket is guarded by the file's ACL. A folder under the user's profile is open to
that user, SYSTEM and administrators. That is narrower than a loopback port, which any local process can dial.
Cross-user access was not tested in this spike.

## 2. Row-batch throughput at full trace

### The real peak (`results/rates.txt`)

| capture | rows | mean/s | peak 1 s | peak 100 ms | peak 10 ms | peak 1 ms | CSV bytes/row |
|---|---|---|---|---|---|---|---|
| driven, 30 rounds (the one used) | 160,741 | 34.5k | **68.5k** | **94.8k** | **287k** | 447k | 102.3 |
| driven, 12 rounds | 21,570 | 9.7k | 18.7k | 42.2k | 93.7k | 373k | 90.7 |
| `samples/calculator.csv` (idle-ish) | 1,876 | 386 | 917 | 3.3k | 22.4k | 58k | 83.4 |

Peaks use a sliding window. The `bus` lane is 88% of a full trace (`vexelray.gui.mutations` and its `depth`
counter, one pair per mutation). The average `detail` is 4.3 characters, and there are 8 threads and 54 kinds. The
100 ms peak is used as "the real peak" below, because an Inspector frame is about 16 ms and short bursts are
absorbed by the queue.

### Encode and decode cost, no socket (`results/encode.txt`)

Per row, by batch size. "enc" is sender wall time per row, including building the records. "dec" is the
Inspector's decode-and-read time per row.

| batch | A rows: enc / B per row / alloc per row / dec | B columns | CSV text |
|---|---|---|---|
| 16 | 106 ns / 68.7 B / 293 B / 48 ns | 89 ns / 54.3 B / 217 B / 53 ns | 208 ns / 102.3 B / 345 B / 424 ns |
| 64 | 70 / 67.6 / 291 / 42 | 68 / 52.3 / 190 / 48 | 164 / 102.3 / 373 / 274 |
| **256** | **65 / 67.3 / 283 / 41** | **63 / 51.8 / 183 / 45** | **176 / 102.3 / 373 / 282** |
| 1024 | 67 / 67.3 / 279 / 44 | 64 / 51.6 / 181 / 45 | 165 / 102.3 / 373 / 256 |
| 4096 | 72 / 66.0 / 305 / 46 | 61 / 50.6 / 177 / 46 | 170 / 100.4 / 366 / 267 |

Bytes include elektro-Q's envelope and the transport's 4-byte length prefix. Per-frame overhead stops mattering
above about 64 rows. Most of A's 280 B per row of allocation is the frame being copied three times (§4), plus a
`Row` and an `ArrayList` slot. B saves a third of that and a quarter of the bytes, at the price of boxed columns
and implicit seqs.

### Maximum sustained throughput over a socket (`results/max.txt`)

The sender runs flat out for 5 s after a 2 s warm-up, until the receiver has every row. Transit is from emit to
the moment the receiver has decoded the batch.

| link | batch 64 rows/s | batch 256 rows/s (MB/s) | batch 1024 rows/s | sender CPU/row @256 | transit p50 @256 |
|---|---|---|---|---|---|
| elektro-Q UDS, A rows | 6.00M | **6.83M (460)** | 7.84M | 143 ns | 61 µs |
| elektro-Q UDS, B cols | 7.87M | **8.89M (461)** | 10.1M | 109 ns | 51 µs |
| elektro-Q TCP, A rows | 4.53M | 5.86M (395) | 6.75M | 169 ns | 60 µs |
| elektro-Q TCP, B cols | 5.54M | 6.76M (350) | 9.21M | 147 ns | 54 µs |
| CSV over UDS | 2.68M | **2.59M (265)** | 2.53M | 203 ns | 463 µs (queued) |
| CSV over TCP | 2.54M | **2.52M (258)** | 2.45M | 189 ns | 2.7 ms (queued) |

CSV is bound by the receiver (282 ns of parsing per row), so its transit time is mostly time spent queued in the
socket. UDS beats TCP by 15–30% for elektro-Q. For text it makes no difference, because parsing dominates.

**Headroom over the real peak (batch 256):**

| | vs 1 s peak (68.5k) | vs 100 ms peak (94.8k) | vs 10 ms burst (287k) |
|---|---|---|---|
| elektro-Q UDS, rows / cols | 100× / 130× | **72× / 94×** | 24× / 31× |
| elektro-Q TCP, rows / cols | 86× / 99× | 62× / 71× | 20× / 24× |
| CSV, UDS / TCP | 38× / 37× | **27× / 27×** | 9× / 9× |

### Paced at the real rate: batch interval against latency and cost (`results/paced-100k.txt`)

100,000 rows/s, slightly above the 100 ms peak, for 5 s after warm-up. Every configuration delivered the full rate.
Transit p50/p99 is in µs. "proc" is process CPU (both ends) as a percentage of one core.

| interval (rows/batch) | elektro-Q UDS rows | UDS cols | TCP rows | TCP cols | CSV UDS | CSV TCP |
|---|---|---|---|---|---|---|
| 1 ms (100) | 74/179, 15.6% | 63/137, 12.8% | 75/153, 15.0% | 70/154, 11.6% | 104/199, 11.9% | 112/219, 13.8% |
| **10 ms (1,000)** | **270/465, 5.0%** | 240/399, 3.4% | 321/509, 4.7% | 280/513, 2.2% | **628/1091, 8.8%** | 710/1267, 7.2% |
| 100 ms (10,000) | 2397/3086, 3.1% | 1712/3174, 1.9% | 1603/30652*, 2.8% | 1549/2572, 4.4% | 5526/6616, 5.9% | 5830/16815, 5.0% |

\* One 30 ms outlier among the run's 50 batches, probably a GC pause. With so few batches, p99 is the max.

- **1 ms batches cost about 12–15% of a core whatever the format.** That is a thousand sender wake-ups and frame
  writes a second. **10 ms (or once per frame) is the sweet spot.** A row is then at most about 10.3 ms old on
  arrival with elektro-Q, or 10.7 ms with text. Either is inside one Inspector frame, so latency does not choose
  between them.
- Sender allocation per 1,000-row batch: A 350 KB, B 236 KB, CSV 348 KB, so about 35 MB/s at 100k rows/s whichever
  is used. That is young-generation garbage on the sender thread, never on the loop.
- From the per-row costs: at 100k rows/s the application pays **0.65%** of a core (elektro-Q) or **1.7%** (text),
  and the Inspector pays 0.45% or 2.8%.

## 3. Typed snapshots and replies (a sketch, not built)

`snap` rows and query replies become records next to `Row`, in one record module both sides compile against.

```java
@IntrabufMessage(name = "inspector.Snap", id = 902, compat = Compat.BACKWARD)
record Snap(long tMonoNs, long frame, Loop loop, List<Mailbox> mailboxes,
            Optional<Kron> kron, Optional<Heap> heap) { }          // domains absent until their hook lands

record Loop(long frame, long drawn, long presented, long wakes, long wakesCoalesced,
            boolean focused, Map<Park, Long> parks) { }            // LoopCounters, as landed (447753d)
record Mailbox(String topic, String owner, int depth, long published, long dropped, Policy policy) { }
record Kron(int shreds, int scheduled, int effects, int animations, String whyBusy, long slipNs) { }
record Heap(long usedBytes, long committedBytes, long gcCount, long gcPauseNs) { }

// Queries: request/reply. The request is the agent's own verb line, so the two protocols cannot drift apart.
@IntrabufMessage(name = "inspector.Query", id = 920) record Query(String line) { }
@IntrabufMessage(name = "inspector.Why", id = 921)
record Why(Optional<String> err, Coverage over, long frame, long tookNs, long budgetNs,
           List<Cause> causes, String see) { }
record Coverage(long fromSeq, long toSeq, long fromMonoNs, long toMonoNs, long lost) { }   // §2's "states what it covers"
record Cause(Lane lane, String kind, long tookNs, String detail) { }

@IntrabufMessage(name = "inspector.StreamOpen", id = 930)
record StreamOpen(Set<Lane> lanes, long fromSeq, int snapEveryMs) { }   // reply: StreamOpened(headSeq, oldestSeq)
```

**Does request/reply cover the Inspector's queries?** For one question and one answer, yes. Requests are
correlated, the 4-argument conduit gives each a timeout, and a request in flight fails at once when the peer goes
away (measured in §1). There are three gaps:

- **The reply type is fixed by the caller** (`request(msg, peer, replyType)`), and intrabuf has no sealed or
  polymorphic records. An `err` therefore has to live inside each reply record, as the `Optional<String> err` above.
- **A reply is one frame.** A large answer (`rows` over a wide span) has to be paginated (`more=N`, which matches
  §2) or sent as stream batches tagged with the request.
- **There is no cancellation.** Stopping a stream is a message of its own.

**Agent-readable rendering.** Yes. Every generated codec writes the text format, and `fromBytes` reads it back. A
five-row `RowBatch` renders like this (`results/backpressure.txt`):

```
#intrabuf 1 inspector.RowBatch 1.0 BACKWARD id=0x00000384 fp=0xf63b65e9 snapshot
sentNanos,lostFrom,lostTo,rows
0,0,0,[&1,&2,&3,&4,&5]

#message inspector.Row 1.0
&,seq,tMonoNs,tWallNs,frame,thread,lane,kind,detail
&1,2001,278919700,1792272968127743800,0,main,BUS,vexelray.gui.mutations,5.7us
&2,2002,278926900,1792272968127743800,0,main,BUS,vexelray.gui.mutations depth,153
```

That is close to §6's CSV, though not the same bytes. `intrabuf decode --header elektroq.Envelope` reads captured
frames. Even so, it is a debugging view, not the agent interface. Agents keep the text verbs through ottermate
(which stays dependency-free), and a reply to `why 18342` is still otter.md's `ok …` shape. The Inspector can use
the same verb through `Query` and get `Why` back typed.

## 4. Costs and fit

**Dependencies.** `elektroq-core` (55 KB), `elektroq-transport-uds` (11 KB), `elektroq-transport-tcp` (11 KB) and
`intrabuf-runtime` (138 KB). That is about 216 KB, all JDK-only, with `intrabuf-processor` on the processor path
only. None of it goes into `vexelray-gui-automation`, which stays `-core`-only, or into ottermate, which stays
dependency-free. Proposed: a new module, **`vexelray-gui-inspector-link`**. It depends on
`vexelray-gui-automation` (or on the recorder module when that lands), elektro-Q and intrabuf. It holds the record
module's types, the sender threads and the UDS/TCP endpoint. An application opts in by depending on it. How the automation
server finds it (a service hook, or the application starting it next to the server) is a design detail this spike
did not look at. architecture-proof.md §2 rule 1
holds: `vexelray-gui-core` and `-widget` still never import elektro-Q.

**Hot-path safety.** `LiveSink.row` stays what the recorder plan says: slot stores into the ring, under the probe's
output lock, with no encoding. Encoding happens only on the link's sender threads. Measured at 63–72 ns a row, with
280 B of short-lived garbage a row, on those threads.

**Loss and backpressure: the ring is the queue.** Measured (`results/backpressure.txt`): Windows AF_UNIX buffered
only **about 0.1 MB (five 256-row batches)** before `emit` blocked. It then blocked for the **full 3 s** the
reader stalled. elektro-Q's send is synchronous, under a per-connection lock, with no timeout. A broadcast `emit`
writes to each connection in turn, so one stalled Inspector would hold up all of them. The design therefore has to
be:

- One sender thread per streaming connection, each holding a cursor (a seq) into the recorder's ring.
- Every 10 ms it sends `[cursor, head)` with `emit(batch, peer)`, never broadcast.
- If it blocks, only that connection waits. When the writer laps the cursor, the cursor jumps to the oldest row
  still held, and the next batch carries `lostFrom..lostTo`, the in-band twin of §6's `lost` row. The recorder
  counts the loss.
- There is no per-connection copy of the rows, and the loop never waits.
- An `emit` to a vanished peer throws `ElektroException: No connected peer with id 1`. That throw is the sender's
  signal to stop.

**Versioning.** Each record is `BACKWARD`, with fields added at the end with `since`, so an older Inspector reads a
newer app's rows and a mismatched major is refused loudly. **Snapshot builds** carry a fingerprint and refuse a
peer whose schema drifted under the same version number. During development the app and the Inspector must
therefore be built from the same record module. That is the right failure, but it is friction worth knowing about.
Release builds drop to an 8-byte header.

**Threads.** elektro-Q's transports read on unnamed virtual threads. vexelray-framework `threading.md` T1.1 says
the lanes are a closed list, and the inspector-skeleton notes already say there is no lane for a long-lived
reader or stream thread. The link's sender threads, and elektro-Q's readers inside the app, need a decision there.
The simplest answer is to treat them as `vexelray-automation-*` daemon threads, outside the lanes, as §4 of the
recorder plan already does for connections.

**Maturity issues found** (in elektro-Q at `bdddbb4`; none blocks a spike, but the first three should be fixed
before the link ships):

1. **Blocking send with no timeout and no non-blocking offer** (`UdsTransport`/`TcpTransport.Connection.write`),
   and a broadcast couples all peers. The README's roadmap already lists "backpressure controls". The link works
   around it as above. A send timeout (or `offer`) in the transport would let it drop a peer that is alive but
   stuck.
2. **A stale socket file blocks the next bind** (`BindException: Address already in use`), and `UdsTransport` takes
   no step to clear it. A `listening(path, replaceStale)` that dials first and deletes on refusal would fix it, or
   the link does it, guided by the discovery lock.
3. **`Conduit` exposes no peer events.** `DefaultConduit` installs itself as the transport's `PeerListener`, so an
   application can only poll `peers()`. The link works because the Inspector opens the stream with a `StreamOpen`
   request, and a failed `emit` means the peer is gone. An `onPeer` on `Conduit` would be cleaner.
4. **Three copies per frame on send**: `IbWriter` growth from 64 bytes, `drain()` into a fresh `byte[]`, and the
   `BufferedOutputStream`. There is also a fresh `byte[]` per frame on receive and a flush per frame. That is the
   280 B a row measured. It is acceptable at these rates, but not allocation-free.
5. **intrabuf has no primitive arrays**, so the columnar form needs boxed lists. Its bundles (the natural "many
   rows" form) are text-only so far.
6. **Faults go to stderr only**, because the transports have no diagnostic seam (the code says so).
7. **The envelope is 10 bytes per emit**, irrelevant once a batch has more than about 16 rows.
8. **Discovery has to say `uds:` or `tcp:`**, because a 106-character limit means UDS cannot be assumed.

**A stale line, flagged, not edited.** The task brief said architecture-proof.md claims that elektro-Q has no
checkout on this machine. architecture-proof.md itself does not say so. The stale line is in
**vexelray-gui `docs/README.md:100`**, in its index entry for that plan: *"Plan, not started, and currently
blocked: it rests on Elektro-Q for the wire, which has no sibling checkout on this machine"*. elektro-Q lives at
`atchung/elektroq` (modules `elektroq-core`, `-codegen`, `-transport-{tcp,uds,local}`, `-netcode`, installed in
`~/.m2` as `sibarum.elektro.queue:*:1.1-SNAPSHOT`), so the plan is not blocked on a checkout.

## 5. What flight-recorder.md §6–§7 should say

Suggested replacement text for §6, keeping its structure:

> ## 6. The stream
>
> The stream has two forms over one source, the ring.
>
> **For the Inspector: typed records over elektro-Q.** A separate module, `vexelray-gui-inspector-link`, serves a
> Unix domain socket at `~/.vexelray/automation/<app>-<pid>.sock`, or a loopback TCP port when the path would pass
> 106 characters, and advertises it in the discovery file as `stream=uds:<path>` or `stream=tcp:<port>`. The
> Inspector sends `StreamOpen(lanes, fromSeq, snapEveryMs)`. The application answers with `StreamOpened(headSeq,
> oldestSeq)`, then emits a `RowBatch` to that peer every 10 ms. `vexelray-gui-automation` and ottermate do not
> depend on it.
>
> - **What it sends.** Batches of `Row(seq, tMonoNs, tWallNs, frame, thread, lane, kind, detail)`, with `detail`
>   raw (newlines kept), and each batch's `lostFrom..lostTo` when rows were lost before it.
> - **Backfill.** `fromSeq` starts the connection's cursor there, if the ring still holds it.
> - **Snapshots.** A typed `Snap` (loop counters, mailboxes, kron, heap) every `snapEveryMs`, default 100. A domain
>   whose hook has not landed is absent, not zero.
> - **Queries.** `Query(line)` carries any read verb. The reply is that verb's typed record (`Why`, …) with an
>   `err` field. Agents get the same verbs as text.
> - **Backpressure.** The ring is the queue. Each streaming connection has one sender thread and a cursor. A slow
>   reader blocks only its own thread. When the writer laps its cursor, the gap goes out in the next batch and the
>   recorder counts it. Encoding never happens in the probe's live sink.
> - **Measured** (atchung `docs/spikes/elektroq-inspector-stream.md`): a full trace peaks at about 95k rows/s
>   over 100 ms. The link sustains 6.8M rows/s over UDS and costs the application about 0.65% of a core at 100k
>   rows/s.
>
> **For agents and files: text.** `stream <lanes|all> [from <seq>] [for <ms>]` on the automation socket sends the
> header row and then CSV rows with the frame stamp, as before, bounded by `for`, so that ottermate and `csvview`
> read it. It runs on the same per-connection sender and cursor. Text sustains about 2.5M rows/s, 27 times the
> real peak, so it is not a bottleneck for anyone who wants it raw.

Then in §7, after "Through ottermate, with nothing new", add one sentence: *the Inspector does not use this
path; it uses the typed link of §6, and an agent never needs it.* In §11, close **Q3**: *measured. Text keeps
up, at 27 times the real peak. The Inspector moves to elektro-Q for its shapes, typed queries, version checks
and socket-file access, not for speed.* Add an open question: *Q6. A lane, or an exemption, for the link's sender
threads and elektro-Q's virtual reader threads (threading.md T1.1).* The order of work (§9 step 5) becomes: the
stream sender and cursor first, with the text form, because agents and tests can use it at once. Then the
link module, with the three elektro-Q fixes in §4 above landing first or alongside it.

The Inspector side changes little. `RowSource` gets a link source that maps `Row` records straight to its rows,
skipping `RowFormat`. `--replay` keeps reading CSV files, so "files and live streams the same way" holds at the
row level, not at the byte level.

## Reproducing

From `atchung/elektroq` on this branch:

```bash
mvn -Pspike -pl elektroq-spike-inspector-stream -am package -DskipTests
java -jar elektroq-spike-inspector-stream/target/spike.jar                       # §1, UdsCheck
mvn -Pspike,spike-native -pl elektroq-spike-inspector-stream -am package -DskipTests   # §1 native: target/uds-check.exe
CP=elektroq-spike-inspector-stream/target/spike.jar; P=sibarum.elektro.queue.spike.inspector
java -cp $CP $P.Rates <capture.csv>...
java -Xmx2g -cp $CP $P.Bench <capture.csv> encode
java -Xmx2g -cp $CP $P.Bench <capture.csv> max 256
java -Xmx2g -cp $CP $P.Bench <capture.csv> paced 100000
java -cp $CP $P.Backpressure <capture.csv>
```

The capture came from the calculator demo, run as `PROBE=all PROBE_FORMAT=csv PROBE_OUT=<file> ottermate
--keep-going --script results/capture-drive-script.txt --launch mvn.cmd -q compile exec:exec -Dautomation=0`. It is
16 MB, so it is not committed.
