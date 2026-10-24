# Correlation: every message knows what caused it

Status: **proposed 2026-10-24; step 1 done 2026-10-24.** Written for `vexelray-gui/docs/plans/export.md` (its M2), which needs to say, for
every GUI write in a capture, what set it off. Lands here because the bus, the log and the probe are all here.

## Why

Pix playing a video posted 400–600 GUI writes a second, and finding the code that sent them meant reading four
repos. The first plan for answering that cheaply was to sample the stack at each post. A stack says *where* a write
was sent from, but not *why* that code ran, and it ends at the thread's run loop: a write posted from a decoder
thread, or from a handler the bus called, has lost its cause by the time anyone looks. A correlation ID travels
with the message instead, across threads, mailboxes and processes, and costs one `long`.

## The idea

**Something that comes from outside the system starts a cause.** An input event, a timer firing, an agent's
command, a file the OS says has changed: each gets a fresh ID where it enters. Everything done because of it runs
inside that cause, and every message published from inside it carries it. A handler the bus calls runs inside the
cause of the message it was handed, so the cause passes through any number of mailboxes without anyone passing it
along by hand.

**Every source of causes has a default.** A source is anything that begins causes: an input backend, a timer, the
automation server, a mailbox. Its default cause covers what it does on its own initiative rather than because
something arrived (a mailbox's startup), and each cause it begins shares the default's prefix with the next
sequence number. So a timer's firings are told apart by their sequence, and grouped by their source without any
lookup.

**Logs and probe rows carry the cause automatically.** The runtime recorder (the probe ring) then needs to hold
only the ID, and the story of a cause is read by joining its rows with its log lines. That join is the tradeoff
this design makes on purpose: a light recorder, and the log as the narrative.

## The ID: a prefix and a sequence number

One `long`, so nothing is allocated to make, carry or store one. The high 34 bits are the prefix, which names the
source; the low 30 are the sequence number:

| Bits | What |
| --- | --- |
| 20 | **Process**, random at startup. Written once into the log's header and the probe's first row, with the application, pid and start time, so it names its process |
| 4 | **Kind** of source: `i` input, `t` timer, `c` command (automation, agent), `o` I/O completion, `m` mailbox, `b` boot, `x` other |
| 10 | **Source**: its index, given when it registers. 1,024 per process |
| 30 | **Sequence**, per source. `0` is the source's default cause; each cause it begins takes the next. A billion before it wraps, which at a thousand causes a second is twelve days |

Written as text `3f2a1.t07.4f2`: the process in hex, the kind's letter and the source's index, then the sequence
in hex. A reader can tell an input from a timer, and one timer from another, at a glance. A source registers by
name, and its registration is a row and a log record, so `t07` resolves to a name. `0`, the whole `long`, means
*no cause*: see [Gaps](#gaps-are-counted-not-hidden).

Each source keeps its own counter, so beginning a cause touches nothing another thread is incrementing.

## Carrying it

**On the thread: a per-thread slot with no setter**, held by `sibarum.probe.Cause` in `atchung-probe`, which the
bus, the log and every repo already depend on. `Cause.current()` reads it, `Cause.begin(source, body)` starts the
source's next one around `body`, and `Cause.within(id, body)` re-enters one that was carried; both put back the
cause they found when the body ends, however it ends. A message runtime that cannot afford a lambda per message
uses `Cause.enter(id)` and `Cause.exit(previous)`, always paired in a `finally`. So a cause cannot outlive the work it
was entered for, and cannot leak into the next message a thread handles.

The first draft said a `ScopedValue`, which gives that guarantee by construction. It was given up on building it:
binding a scoped value allocates, and a pumped mailbox re-enters a cause for every event it drains while promising
that a drain allocates nothing (`DrainBufferTest`). The slot gives the same guarantee by having no way to set a
cause except around a body.

**Through the bus, by delivery mode:**

- **Inline** (publisher's thread): nothing to do. The handler runs inside the publisher's cause already.
- **Async** (an executor): `AsyncReg` captures `Cause.current()` at publish and runs the handler `within` it. The
  lambda it already allocates per event carries the extra `long`.
- **Pumped** (a mailbox, drained by its owner): the ID is stored beside the event at enqueue and bound around the
  handler at drain. An ordinary mailbox keeps a ring of `long`s parallel to its queue, swapped with it at each
  drain, so a drain still allocates nothing. A folding mailbox's `Slot` gains a `long cause` field. When a fold
  supersedes a write, the surviving write's cause is the one delivered, because that is the cause of what the
  handler sees.

The event object is untouched: the bus stays zero-copy.

**Across processes: elektro-Q's `Envelope`** gains an optional `cause` field (a minor version: an old peer ignores
it, a new one reading an old peer's frame finds none). `DefaultConduit` writes `Cause.current()` going out and
binds the envelope's cause around delivery coming in, so the remote ID keeps its own prefix and a chain that
crosses into the Inspector or an agent stays one chain. `ElektroBridge` needs nothing of its own: it publishes and
emits from inside the cause already.

**Where causes begin** is the boundary code, not this repo: the input backends in `vexelray-os`, the frame clock
and timers, the automation server (each command its own `c` cause, so an agent's command and everything it did
share an ID), file watching, and the framework's offload pool, which binds the submitter's cause around each task
it runs. Each is a one-line wrap at the place the outside world comes in.

## Logs and the probe

- **`Log`** writes the cause on every record, as a field of the file format, filled from `Cause.current()` when
  the record is made. Nobody passes it.
- **`Probe`** gives its CSV a `cause` column, before `detail`, which stays last (see `probe.md`). Readers that go
  by column name, as the Inspector does, see nothing change, and `CsvView` finds `detail` by the header. Live
  sinks get the cause through a second `LiveSink.row` overload whose default drops it, so a sink written before
  causes goes on working.
  Every row made inside a cause carries it, the mutation rows of `vexelray-gui`'s `layout` lane included, with no
  change at their call sites.
- **`CsvView`** gains a by-cause view: rows and their count per cause, its kind, and the mailboxes it passed
  through, which is the first table an export needs.

## Gaps are counted, not hidden

A publish made with no cause bound (a thread nobody wrapped, a library's own pool) carries `0`. Each one is
counted under its thread's name, and `CsvView` reports them, so a missing boundary shows up as a named gap rather
than as rows that seem to have no history.

## Trace sites

A method annotated `@Trace` writes a row naming itself, inside whatever cause it runs in. That is `vexelray`'s
(the annotation, a post-compile weaver and the site tables): see [`vexelray/docs/trace.md`](../../vexelray/docs/trace.md).
Its rows go through `Probe` and so carry the cause like any other.

## Decided 2026-10-24

- **A timer firing** begins a cause of its own: its source's prefix, the next sequence number.
- **`@Trace` is `vexelray`'s**, not this repo's. This repo carries the cause; what is traced is the engine's tooling.

## Steps

1. **`Cause`, sources, the ID and the probe column.** Verified by a test that begins a cause, publishes through inline,
   async and pumped subscribers (an ordinary and a folding mailbox), and finds the same ID in every handler and
   every row, and none leaking into the next message.
2. **`Log` writes the cause.** Verified with `Logging.capture()`.
3. **elektro-Q's envelope carries it.** Verified over the local transport, both ways, and against a peer that
   predates the field.
4. **The boundaries** (other repos): input, clock and timers, automation commands, the offload pool, each
   registered as a source.
