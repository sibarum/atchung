# Logging: one model for the whole stack

This is the design and the conventions of `sibarum.probe.Log` — the logger every repo on this machine reaches,
because every repo already depends on `atchung-probe` and on nothing else it does not need.

It answers a narrow question that had four answers: *where does a program say what it is doing?* Before this there
were raw `System.out`/`System.err` calls (about 190 across the repos, in five formats), `Diagnostics.dropped`
(warn-once, `vexelray-diagnostics`), and `Probe` (spans and counters, off by default). Three mechanisms, no levels,
no file, and no way for a run that somebody is *driving* to say more than one that nobody is watching.

## The shape

| Piece | Is | Lives in |
| --- | --- | --- |
| `Log` | a named logger: `Log.of("gui.frame").info("...")` | `sibarum.probe` |
| `Level` | `TRACE DEBUG INFO WARN ERROR` (+ `OFF` as a threshold) | `sibarum.probe` |
| `Mode` | what kind of run this is: `TEST`, `AUTOMATION`, `DEV`, `PACKAGED` | `sibarum.probe` |
| `LogConfig` | everything decided from the mode and the settings — a pure function of an `Env`, so every default is a test | `sibarum.probe` |
| `Logging` | configuration, delivery, runtime level changes, `capture()` for tests | `sibarum.probe` |
| `Probe` | *how long / how often / how much*: rollups and a correlation trace. Unchanged, except that its default now follows the mode | `sibarum.probe` |
| `Diagnostics.dropped` | "a capability was silently dropped" — now a `WARN` on the log, still once per call site | `vexelray-diagnostics` |

**Why here.** A new module would be a new edge in every repo's dependency graph, which is the cost this avoids.
`atchung-probe` is pure Java with no dependencies, no reflection, and starts no thread unless something is on — it
already describes itself as *"the one seam every layer of this stack reports through."* The log and the probe are two
halves of one question (*what happened* / *how long did it take*), and in an automation run they are read as one
story: every log record at `INFO` or below is also written into the probe's trace.

## Context decides the defaults

Nobody should set five properties to get sensible output. The situation already says most of it, and it is worked
out once, at startup:

| Mode | When | Console | File | Probe |
| --- | --- | --- | --- | --- |
| `TEST` | under a test runner | `WARN` | off | off |
| `DEV` | a JVM that is not a native image, nobody driving | `INFO` | `DEBUG` | off |
| `PACKAGED` | a native image — what a user runs | `WARN` | `INFO` | off |
| `AUTOMATION` | the automation socket is on (`--automation`, `-Dautomation`) | `DEBUG` | `TRACE` | `frame,input,layout,app`, correlation trace beside the log |

**Being driven is what makes a run loud**, and it beats `PACKAGED`: somebody or something is reading along, and the
point of driving an application is to find out what it did. So `ottermate --launch … ` gets a `TRACE` file, the
agent's own commands and their replies in it, and a probe trace to read it against — without anybody having asked
for any of it in advance, which is the only time it is ever wanted.

Precedence, for the mode: an explicit mode from the caller (the framework sees `--automation=0` before any property
says so) → `-Dlog.mode` → automation → test runner → native image → dev.

## Where the file goes

1. `-Dlog.dir=…`, if set;
2. `<app>.home/logs`, if the `<app>.home` property is set — the same redirect `AppHome` honours, so a test rig that
   moves an application's home moves its logs with it;
3. in `DEV` and `AUTOMATION`, `./target/logs` when the working directory is a Maven project — beside the build, and
   `mvn clean` removes it;
4. otherwise `$HOME/.<app>/logs/`, the per-user directory `AppHome` names.

The file is `<app>.log`; the probe's trace, when the mode turns it on, is `<app>-probe.csv` beside it. One layout on
every platform, for the reason `AppHome` gives: a convention that fits in one line is one a user can find without
documentation.

**Reading leaves no mark.** Nothing touches the disk until the first record that reaches the file. An application
launched and closed without logging anything leaves no directory behind.

**Rolling.** 5 MB, five rolled files (`app.log.1` … `app.log.5`), then the oldest goes. A line is never split across
files. A file that cannot be written — read-only directory, full disk — is reported once on stderr and given up on;
the records still reach the console. A log must not be able to stop the application it describes.

## Settings

Each is a system property, or the same name in upper case with dots as underscores as an environment variable
(`log.level` / `LOG_LEVEL`); the property wins. A framework application also takes `--log=debug` (or `--log=off`).

```
log.mode=automation         force a mode instead of detecting one
log.level=debug             both sinks; the two below win over it
log.console=warn            console threshold, or off
log.file=trace              file threshold, or off
log.level.gui.frame=trace   one logger and everything beneath it
log.dir=/var/log/app        where the file goes
log.format=json             text (default) or json, one object per line
log.rotate.size=10          megabytes before the file rolls (default 5)
log.rotate.keep=3           rolled files kept (default 5)
log.app=editor              the name the file and banner use
```

A value that is present and not understood is **reported in the startup banner** (`log setting ignored: …`) and the
default stands. Silently ignoring a typo in `log.level` would be the failure this whole exercise is meant to remove.

**A per-logger level is an instruction about that logger.** A sink's threshold filters the loggers that have nothing
to say about themselves. A logger with its own `log.level.<name>` is being asked for by name, so it reaches every sink
that is on, whatever that sink's threshold. Otherwise `-Dlog.level.gui.frame=trace` would change nothing in a shipped
configuration — which is exactly when somebody types it.

## Changing it while a run is going

`ottermate` has a verb for it, because asking for more detail *after* a click did something unexpected is the moment
a launch flag is too late:

```
log                       what is in force, and where the file is
log debug                 both sinks
log gui.frame trace       one logger and everything beneath it
```

The change is to the running process only; nothing is written down for the next launch.

## Conventions

**Standard output is not a log.** Records go to standard error and the file. Standard output is for what a program
*produces*: `ottermate --launch` reads `automation: localhost:<port>` from it, and a pipeline reads a tool's answer
from it. That line is protocol and stays a `println`; the log says the same thing in its own words.

**Levels mean the same thing everywhere** — see `Level`. The two rules that matter: *INFO is a story, not a stream*
(a run's INFO lines read in order as what happened, never once per frame or per event), and *log the decision, not
the data* (a path chosen, a fallback taken and why; never a document, a token, a password).

**Names are dotted and hierarchical, and name a subsystem rather than a class** when they can: `gui.frame`, not
`GuiApp`. A level is something a person types on a command line, and they should not have to know which class does
the work. Existing names: `log` (the banner), `uncaught`, `vexelray.diag`, `framework.{app,shell,input,faults,automation}`,
`gui`, `gui.app`, `gui.window`, `gui.window.memory`, `automation.server`, `vulkan.swapchain`, `vulkan.present`.

`gui.app` says at `DEBUG` what the frame ceiling is and when it changes (*frame ceiling follows the display: 6944 us a
frame (144 Hz)*); `vulkan.swapchain` and `vulkan.present` say what was built: the swapchain's extent and image count
against the driver's minimum, and the frames in flight. A run that is slow to start or odd to look at is read from these
first, and `-Dlog.level.vulkan=debug` turns them on in a shipped build, where a per-logger level reaches every sink.

**Not inside the frame loop.** Nothing under `FrameHooks.run` or `Pacing.nanosUntilNextFrame` logs, at any level. A
disabled call is a volatile read and a comparison — the string is never built — but it is still a branch on the hot
path, and an enabled one takes a lock and writes. Use `Probe` there: it was built for that budget.

**Instrumenting a hot path: log once what was built, mark per frame what happened.** The split that served the frame
loop and the presenter: a *decision or a construction* (the swapchain's size, the ceiling in force) is a `DEBUG` log,
written once and read by a person; a *per-frame fact* (which image, how long to the fence, where in the refresh interval)
is a `Probe.mark` behind `if (Probe.ON)`, written to the trace, read against the rows around it. An automation run
captures both in the same file in time order, which is what lets a mark be read next to the log line that explains it.
See [probe.md](probe.md), *Marks the stack publishes for reading a stall*, for the vocabulary.

**Debug logging is for the next person to read a trace, so it stays in.** The instrumentation added to find a stall
(the presenter's marks, the baton's `gate.open` / `gate.woke`, the compositor timing) is not scaffolding to remove after
the fix: each is off unless asked for and costs a `static final` check, and the next stall is read from the same rows.
An *experiment's* switch is the exception: it goes when the experiment is answered, or it is named as one and is off.

**Guard what is expensive to compute.** `if (LOG.isDebug()) LOG.debug("{}", expensive())`, or pass a `Supplier`. The
`{}` form allocates a small array when it is enabled.

**An exception is the last argument.** `LOG.warn("could not save {}", path, e)` — as in SLF4J, a `Throwable` left over
after the placeholders is the exception, and its stack goes in the file as well as the console.

**`warnOnce(key, …)` is for seams that run per pipeline, per frame or per glyph**, where a repeated warning is one
that gets filtered out by eye. The key identifies the *call site*, never the data; the number of keys is capped, and
reaching the cap is itself reported.

**`-api` and `-core` stay JDK-only.** They do not log. Logging starts at `-shell` and up, which is where the
framework's dependency edges already fall.

## Every process gets the same three things for free

`Logging.configure(app, mode)` — which the framework's `VexelApplication.run` calls before anything else, so no
application does — also installs:

- a **startup banner** at `INFO`: application, mode, sink levels, file, format, pid, JVM, OS;
- an **uncaught-exception handler**, if nobody installed one, that logs the exception at `ERROR` with its stack;
- a **shutdown hook** that flushes and closes the file.

A file is flushed at once for `WARN` and above, and once a second otherwise — at once for everything in `AUTOMATION`
and `TEST`, where somebody is reading along.

## Testing

```java
try (Logging.Capture log = Logging.capture()) {
    thing.doIt();
    assertTrue(log.has(Level.WARN, "clipboard unavailable"));
}
```

`capture()` records every record that passes a logger's level, into memory, whatever the sinks do with it — for the
reason `Diagnostics.recorded()` exists: a warning nobody has watched fire is one more instrument taken on faith, and
a test has to be able to watch without the suite being noisy. To capture `DEBUG`, raise the logger first
(`Logging.setLevel("x", Level.DEBUG)`).

## Adopting it in a repo

1. It already depends on `atchung-probe` (check the pom); nothing to add.
2. `private static final Log LOG = Log.of("subsystem");`
3. Replace each `System.err.println` / `System.out.println` that is *diagnostic* with the right level. A `println`
   that is the program's **output** (a CLI's answer, a protocol line) stays.
4. Do not call `Logging.configure` from a library. Applications do, once, first.

Sites still to migrate, at the time of writing: `atchung` (~15), `supirvast` (~22), `vexelray-designer` (~53), and
`vexelray-gui-demo`'s `Demo` (a program whose output it is). `vexelray-framework`, `vexelray-gui`'s library code and
**`vexelray`'s libraries are done**: what remains in `vexelray` is three programs in `vexelray-demo` and three in
`vexelray-experimental`, whose output it is. `kronometer`'s remaining sites are all in `kronometer-bench` and
`kronometer-demo`, programs likewise. (The counts for `atchung`, `supirvast` and `vexelray-designer` are the earlier
ones, not rechecked.)
