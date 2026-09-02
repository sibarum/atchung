package sibarum.probe;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

/**
 * Verbose on demand: the one seam every layer of this stack reports through, and the one switch that opens it.
 *
 * <h2>The switch</h2>
 *
 * Off unless asked. Ask with the system property {@code probe}, or — identically, and on every operating
 * system — the environment variable {@code PROBE}. The property wins if both are set.
 *
 * <pre>
 * -Dprobe=all                  every lane
 * -Dprobe=frame,gpu,anim       three lanes (see {@link Lane})
 * -Dprobe=off                  the default
 * </pre>
 *
 * Modifiers, each also available as an environment variable ({@code probe.out} / {@code PROBE_OUT}, and so on):
 *
 * <pre>
 * probe.out=path/run.log       write here instead of stdout
 * probe.trace=true             one line per event as it happens, not just the rollup
 * probe.every=1000             also print a rollup every N milliseconds
 * probe.slow=8                 always print any span over N milliseconds, whatever the mode
 * probe.format=csv             emit the trace as a correlation CSV (implies trace; see below)
 * probe.stacks=true            record allocation stacks in the resource ledger
 * probe.top=20                 rows per lane in the rollup (default 12)
 * </pre>
 *
 * <h2>What it costs when it is off</h2>
 *
 * {@link #ON} is a {@code static final boolean} assigned once in this class's initialiser. It is not a
 * compile-time constant, so a downstream module does not bake the value in at build time and the switch stays
 * a runtime switch — but the JIT sees a field that can never change and folds {@code if (Probe.ON)} out of
 * every method it compiles. The rule at every call site follows from that, and it is the whole discipline:
 *
 * <pre>{@code
 * if (Probe.ON) Probe.count(Lane.BUS, "publish:" + topic.name());
 * }</pre>
 *
 * The guard comes first, always — including around the string concatenation, which is otherwise the expensive
 * part. A call site that computes its own argument before the guard has paid for profiling in a build that is
 * not profiling, which is the one thing this class must never make easy.
 *
 * <h2>What it reports</h2>
 *
 * Three things, because performance questions in a rendering stack come in three shapes.
 *
 * <ul>
 *   <li><b>Spans</b> ({@link #zone}) — how long, with a percentile distribution and a self time. The mean
 *       hides stutter; p99 and max are what a dropped frame looks like in a table.</li>
 *   <li><b>Counters</b> ({@link #count}) — how many, and how big the biggest one was. A mailbox depth, a
 *       vertex count, a publish rate.</li>
 *   <li><b>Resources</b> ({@link #opened}/{@link #closed}) — what is still open. The leak half: a native
 *       handle nothing closed is invisible to a heap dump, so it is counted here instead.</li>
 * </ul>
 *
 * A rollup is printed when the process exits, and every {@code probe.every} milliseconds if that was asked
 * for. {@link #dump()} prints one immediately.
 *
 * <p>Everything here is safe from any thread. Nothing here throws.
 */
public final class Probe {

    /**
     * Whether this process is profiling.
     *
     * <p>Guard every call site with it. See the class notes for why this one field is the difference between
     * a facility that costs nothing and one that costs a branch and a string per event.
     */
    public static final boolean ON;

    /** Emit a line per event as well as accumulating it. */
    static final boolean TRACE;

    /**
     * Emit those lines as a correlation CSV rather than as aligned text.
     *
     * <p>The format a run is <em>read</em> in, as opposed to watched in. One file, one writer, one row per
     * event, sorted on the monotonic clock and hunted for gaps — which is the whole reason this exists rather
     * than a directory of per-subsystem logs. Parallel writers are what make correlation lie: separate buffers
     * flush at different times, so the order on disk is not the order that happened. One sink makes the file's
     * own order the truth.
     *
     * <p>Implies {@link #TRACE}. A correlation log with only the slow spans in it is not one.
     */
    static final boolean CSV;

    /**
     * The row number, so that loss is detectable. Not the sort key — {@code t_mono_ns} is — but a gap in this
     * is the difference between "nothing happened here" and "we did not hear about it".
     */
    private static final java.util.concurrent.atomic.AtomicLong SEQ = new java.util.concurrent.atomic.AtomicLong();

    /** Spans at or over this many nanoseconds are printed the moment they close, in any mode. 0 disables. */
    static final long SLOW_NANOS;

    private static final int TOP;
    private static final Sink SINK;
    private static final Ledger LEDGER;
    private static final long ORIGIN = System.nanoTime();

    static {
        String spec = setting("probe", "PROBE", "off");
        boolean on = !(spec.isEmpty() || spec.equalsIgnoreCase("off") || spec.equalsIgnoreCase("false")
                || spec.equalsIgnoreCase("none") || spec.equals("0"));
        ON = on;
        if (!on) {
            TRACE = false;
            CSV = false;
            SLOW_NANOS = 0L;
            TOP = 0;
            SINK = null;
            LEDGER = null;
        } else {
            selectLanes(spec);
            CSV = setting("probe.format", "PROBE_FORMAT", "text").equalsIgnoreCase("csv");
            // csv implies trace: the format exists to be read afterwards, and a correlation log holding only
            // the spans that happened to be slow is not one.
            TRACE = CSV || flag("probe.trace", "PROBE_TRACE");
            SLOW_NANOS = number("probe.slow", "PROBE_SLOW", 0L) * 1_000_000L;
            TOP = (int) number("probe.top", "PROBE_TOP", 12L);
            String out = setting("probe.out", "PROBE_OUT", "");
            SINK = out.isEmpty() ? Sink.stdout() : Sink.file(Path.of(out));
            LEDGER = new Ledger(flag("probe.stacks", "PROBE_STACKS"));
            banner();
            install(number("probe.every", "PROBE_EVERY", 0L));
        }
    }

    private Probe() {
    }

    // --- call sites --------------------------------------------------------

    /**
     * Open a span on {@code lane}, closed by the try-with-resources that holds it.
     *
     * <p>Returns a shared do-nothing zone when the lane is off, so the block need not be guarded twice — but
     * still guard the whole statement with {@link #ON} if the name has to be built.
     */
    public static Zone zone(Lane lane, String name) {
        if (!ON || !lane.on) {
            return Zone.NONE;
        }
        return Zone.STACKS.get().push(span(lane, name, false), System.nanoTime());
    }

    /**
     * As {@link #zone}, but for time the process spends deliberately doing nothing - parked on an event
     * queue, waiting on a gate, sleeping out a frame cap.
     *
     * <p>Measured exactly like any other span and excluded from the slow-span report. Idling is not a
     * performance problem, it is the absence of one, and a {@code probe.slow} threshold that fires on every
     * park buries the single frame that genuinely ran long under a hundred lines saying the window was still.
     */
    public static Zone idleZone(Lane lane, String name) {
        if (!ON || !lane.on) {
            return Zone.NONE;
        }
        return Zone.STACKS.get().push(span(lane, name, true), System.nanoTime());
    }

    /** One observation of {@code name}, counting 1. */
    public static void count(Lane lane, String name) {
        count(lane, name, 1L);
    }

    /**
     * One observation of {@code name} worth {@code n} — a queue depth, a vertex count, a byte total.
     *
     * <p>The rollup shows the number of observations, their sum, and the largest single one. The largest is
     * usually the interesting one: a mailbox whose depth averaged 2 and once reached 4096 is a mailbox that
     * dropped events, and only the peak says so.
     */
    public static void count(Lane lane, String name, long n) {
        if (!ON || !lane.on) {
            return;
        }
        counter(lane, name).bump(n);
        if (TRACE) {
            emit(lane, name, Long.toString(n));
        }
    }

    /**
     * An instant worth seeing in the trace: a state change, a mode switch, a fault. Recorded as a counter so
     * it still appears in the rollup, and printed with {@code detail} when tracing.
     */
    public static void mark(Lane lane, String name, String detail) {
        if (!ON || !lane.on) {
            return;
        }
        counter(lane, name).bump(1L);
        if (TRACE) {
            emit(lane, name, detail);
        }
    }

    /**
     * Register {@code what} as an open resource of {@code kind}. Pair with {@link #closed}.
     *
     * <p>Call it where the resource is created, not where it is stored, and pass the object itself — the
     * ledger keys by identity and holds a strong reference, which is what lets the final report name a thing
     * that is still alive rather than a thing that was.
     */
    public static void opened(Lane lane, String kind, Object what) {
        if (ON && lane.on) {
            LEDGER.opened(lane, kind, what);
            if (TRACE) {
                emit(lane, "open", kind + " #" + System.identityHashCode(what));
            }
        }
    }

    /** Strike {@code what} off the ledger. Call it from {@code close()}, not from a cleaner. */
    public static void closed(Lane lane, String kind, Object what) {
        if (ON && lane.on) {
            LEDGER.closed(kind, what);
            if (TRACE) {
                emit(lane, "close", kind + " #" + System.identityHashCode(what));
            }
        }
    }

    /** Print a rollup now. Safe from anywhere, including a debugger or a keyboard shortcut. */
    public static void dump() {
        if (!ON) {
            return;
        }
        if (CSV) {
            // The rollup is a table for a person to read, and this sink is a table for a program to read. Put
            // it in the file and every consumer needs a rule for skipping it; put it on stderr and the run
            // still prints its summary where a person is already looking.
            System.err.print(report());
        } else {
            SINK.block(report());
        }
    }

    /** Whether {@code lane} is selected — for a caller that wants to skip building an argument. */
    public static boolean on(Lane lane) {
        return ON && lane.on;
    }

    // --- internals ---------------------------------------------------------

    /** Called by {@link Zone#close}: trace the span, or print it if it blew the {@code probe.slow} threshold. */
    static void slow(Tally tally, long nanos) {
        if (TRACE) {
            emit(tally.lane, tally.name, dur(nanos).trim());
        } else if (SLOW_NANOS > 0 && nanos >= SLOW_NANOS && !tally.idle) {
            emit(tally.lane, "SLOW " + tally.name, dur(nanos).trim());
        }
    }

    private static Tally span(Lane lane, String name, boolean idle) {
        return lane.spans.computeIfAbsent(name, k -> new Tally(lane, k, true, idle));
    }

    private static Tally counter(Lane lane, String name) {
        return lane.counts.computeIfAbsent(name, k -> new Tally(lane, k, false, false));
    }

    private static String prefix(Lane lane) {
        return String.format(Locale.ROOT, "%9s %-6s [%s] ",
                dur(System.nanoTime() - ORIGIN), lane.key(), Thread.currentThread().getName());
    }

    // --- emission ----------------------------------------------------------

    /**
     * One traced event, in whichever format this run asked for. Every trace line in this class goes through
     * here, which is the point: a second place that formats an event is a second format to keep in step.
     */
    private static void emit(Lane lane, String kind, String detail) {
        SINK.line(CSV ? csv(lane, kind, detail)
                : prefix(lane) + kind + (detail == null || detail.isEmpty() ? "" : " " + detail));
    }

    /**
     * One row of the correlation log.
     *
     * <p><b>{@code seq} is not the sort key</b> — {@code t_mono_ns} is. It is there so that loss is
     * <em>detectable</em>: a run is read by sorting on time and hunting for gaps, and a gap that is a dropped
     * line looks exactly like a gap that is a stall unless the numbering says which it was. It also breaks
     * ties when two threads land in the same nanosecond.
     *
     * <p><b>Two clocks, deliberately.</b> {@code t_mono_ns} is monotonic, and is what durations and ordering
     * are computed from; a wall clock can step sideways and would sort two events into an order that never
     * happened. {@code t_wall} is for people, and for joining this file against anything outside the process.
     *
     * <p>{@code thread} and {@code lane} say who and where, {@code kind} says what, and {@code detail} is the
     * producer's own — always last, so a naive split on commas keeps working when it contains one.
     */
    private static String csv(Lane lane, String kind, String detail) {
        StringBuilder sb = new StringBuilder(96);
        sb.append(SEQ.incrementAndGet()).append(',')
                .append(System.nanoTime() - ORIGIN).append(',')
                .append(Instant.now()).append(',');
        field(sb, Thread.currentThread().getName()).append(',');
        field(sb, lane.key()).append(',');
        field(sb, kind).append(',');
        field(sb, detail == null ? "" : detail);
        return sb.toString();
    }

    /**
     * One CSV field, quoted when it has to be, and <b>never spanning a line</b>.
     *
     * <p>The newline handling is the load-bearing part. RFC 4180 permits a quoted field to contain a raw
     * newline, and one stack trace written that way ends the file's usefulness: {@code sort}, {@code grep -n}
     * and {@code awk} stop describing events and start describing fragments of them. One event, one physical
     * line, always — the format's whole value is that line tools work on it.
     */
    private static StringBuilder field(StringBuilder sb, String v) {
        String s = v.replace("\r\n", " ").replace('\n', ' ').replace('\r', ' ');
        if (s.indexOf(',') < 0 && s.indexOf('"') < 0) {
            return sb.append(s);
        }
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"') {
                sb.append('"');           // RFC 4180: a quote inside a quoted field is doubled
            }
            sb.append(c);
        }
        return sb.append('"');
    }

    // --- configuration -----------------------------------------------------

    /**
     * A setting from the system property, else the environment variable, else {@code fallback}.
     *
     * <p>Both, rather than one, because the two are used at different moments. A property is what a Maven
     * profile or a launcher script sets; an environment variable is what survives into a native binary with
     * no JVM to pass {@code -D} to, and into a shell about to run the app four different ways.
     */
    private static String setting(String property, String env, String fallback) {
        String v = System.getProperty(property);
        if (v == null || v.isBlank()) {
            v = System.getenv(env);
        }
        return v == null || v.isBlank() ? fallback : v.trim();
    }

    private static boolean flag(String property, String env) {
        String v = setting(property, env, "false");
        return v.equalsIgnoreCase("true") || v.equals("1") || v.equalsIgnoreCase("yes");
    }

    private static long number(String property, String env, long fallback) {
        String v = setting(property, env, "");
        if (v.isEmpty()) {
            return fallback;
        }
        try {
            return Long.parseLong(v);
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    /**
     * Turn on the lanes {@code spec} names. {@code all}/{@code on}/{@code true} means every lane; anything
     * else is a comma-separated list of {@link Lane#key()}s, and an unknown name is reported rather than
     * ignored — a typo that silently profiles nothing wastes the reproduction, which is the expensive part.
     */
    private static void selectLanes(String spec) {
        if (spec.equalsIgnoreCase("all") || spec.equalsIgnoreCase("on") || spec.equalsIgnoreCase("true")
                || spec.equals("1")) {
            for (Lane lane : Lane.values()) {
                lane.on = true;
            }
            return;
        }
        for (String part : spec.split(",")) {
            if (part.isBlank()) {
                continue;
            }
            Lane lane = Lane.byKey(part);
            if (lane == null) {
                System.err.println("probe: no lane named '" + part.trim() + "'; known lanes are " + laneKeys());
            } else {
                lane.on = true;
            }
        }
    }

    private static String laneKeys() {
        StringBuilder sb = new StringBuilder();
        for (Lane lane : Lane.values()) {
            sb.append(sb.isEmpty() ? "" : ", ").append(lane.key());
        }
        return sb.toString();
    }

    private static void banner() {
        StringBuilder on = new StringBuilder();
        for (Lane lane : Lane.values()) {
            if (lane.on) {
                on.append(on.isEmpty() ? "" : " ").append(lane.key());
            }
        }
        String banner = "probe: lanes [" + on + "]" + (TRACE ? " tracing" : "")
                + (SLOW_NANOS > 0 ? " slow>" + SLOW_NANOS / 1_000_000 + "ms" : "")
                + (SINK.path() == null ? "" : " -> " + SINK.path().toAbsolutePath());
        if (CSV) {
            // The data file holds rows and nothing else. A banner in it is one line every consumer has to know
            // to skip, and the header is the one line they can all be told about instead.
            System.err.println(banner + " (csv)");
            SINK.line("seq,t_mono_ns,t_wall,thread,lane,kind,detail");
        } else {
            SINK.line(banner);
        }
    }

    /**
     * The periodic rollup, if asked for, and the one at exit, always.
     *
     * <p>The exit report is the point of the whole facility. A performance run ends when the window is closed
     * or the process is killed, and a profiler whose findings only appear on a clean shutdown reports nothing
     * about the case that matters. A shutdown hook covers the close and the interrupt; nothing covers a hard
     * kill, which is what {@code probe.every} is for.
     */
    private static void install(long everyMillis) {
        if (everyMillis > 0) {
            Thread t = new Thread(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        Thread.sleep(everyMillis);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    dump();
                }
            }, "probe-rollup");
            t.setDaemon(true);
            t.start();
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            SINK.block(report());
            SINK.close();
        }, "probe-exit"));
    }

    // --- rendering ---------------------------------------------------------

    private static String report() {
        StringBuilder sb = new StringBuilder(4096);
        sb.append("\n=== probe: ").append(dur(System.nanoTime() - ORIGIN))
                .append(" in ==========================================\n");
        spans(sb);
        counters(sb);
        resources(sb);
        memory(sb);
        sb.append("=== end probe ================================================\n");
        return sb.toString();
    }

    /** Spans, worst total first within each lane: the table you read top-down to find the frame budget. */
    private static void spans(StringBuilder sb) {
        boolean any = false;
        for (Lane lane : Lane.values()) {
            if (lane.spans.isEmpty()) {
                continue;
            }
            if (!any) {
                any = true;
                sb.append(String.format(Locale.ROOT, "%-6s %-32s %8s %10s %9s %9s %9s %9s %10s%n",
                        "lane", "span", "count", "total", "mean", "p50", "p99", "max", "self"));
            }
            int shown = 0;
            for (Tally t : sorted(lane.spans.values())) {
                if (shown++ == TOP) {
                    sb.append(String.format(Locale.ROOT, "%-6s %s%n", lane.key(), "  ..."));
                    break;
                }
                sb.append(String.format(Locale.ROOT, "%-6s %-32s %8d %10s %9s %9s %9s %9s %10s%n",
                        lane.key(), clip(t.name, 32), t.count(), dur(t.total()), dur(t.mean()),
                        dur(t.percentile(0.50)), dur(t.percentile(0.99)), dur(t.max()), dur(t.selfTotal())));
            }
        }
    }

    /** Counters, biggest sum first within each lane. */
    private static void counters(StringBuilder sb) {
        boolean any = false;
        for (Lane lane : Lane.values()) {
            if (lane.counts.isEmpty()) {
                continue;
            }
            if (!any) {
                any = true;
                sb.append(String.format(Locale.ROOT, "%n%-6s %-32s %8s %14s %10s %10s%n",
                        "lane", "counter", "n", "sum", "mean", "max"));
            }
            int shown = 0;
            for (Tally t : sorted(lane.counts.values())) {
                if (shown++ == TOP) {
                    sb.append(String.format(Locale.ROOT, "%-6s %s%n", lane.key(), "  ..."));
                    break;
                }
                sb.append(String.format(Locale.ROOT, "%-6s %-32s %8d %14d %10d %10d%n",
                        lane.key(), clip(t.name, 32), t.count(), t.total(), t.mean(), t.max()));
            }
        }
    }

    /**
     * The ledger: opened, closed, and — the column to look at — still live.
     *
     * <p>A kind whose live count grows across two rollups of the same workload is the leak, and it is named
     * without anyone having to reason about it.
     */
    private static void resources(StringBuilder sb) {
        List<String> kinds = LEDGER.kinds();
        if (kinds.isEmpty()) {
            return;
        }
        sb.append(String.format(Locale.ROOT, "%n%-40s %10s %10s %10s%n",
                "resource", "opened", "closed", "LIVE"));
        for (String kind : kinds) {
            long opened = LEDGER.opened(kind);
            long closed = LEDGER.closed(kind);
            sb.append(String.format(Locale.ROOT, "%-40s %10d %10d %10d%n",
                    clip(kind, 40), opened, closed, opened - closed));
        }
        List<Ledger.Entry> oldest = LEDGER.oldest(TOP);
        if (!oldest.isEmpty()) {
            sb.append(String.format(Locale.ROOT, "%n%d live; oldest %d:%n", LEDGER.liveCount(), oldest.size()));
            for (Ledger.Entry e : oldest) {
                sb.append(String.format(Locale.ROOT, "  #%-6d %-30s opened at %9s on %s%n",
                        e.seq(), clip(e.kind(), 30), dur(e.nanos()), e.thread()));
                if (e.stack() != null) {
                    for (StackTraceElement f : e.stack()) {
                        sb.append("        at ").append(f).append('\n');
                    }
                }
            }
        }
    }

    /**
     * The heap, for context.
     *
     * <p>Not a substitute for a heap dump and not meant to be. It is here so a report showing 25 live render
     * targets can be read next to the number that says whether that matters yet, in the same block of text,
     * without a second tool.
     */
    private static void memory(StringBuilder sb) {
        Runtime rt = Runtime.getRuntime();
        long used = rt.totalMemory() - rt.freeMemory();
        sb.append(String.format(Locale.ROOT, "%nheap %s used / %s committed / %s max   threads %d%n",
                bytes(used), bytes(rt.totalMemory()), bytes(rt.maxMemory()), Thread.activeCount()));
    }

    private static List<Tally> sorted(java.util.Collection<Tally> in) {
        List<Tally> out = new ArrayList<>(in);
        out.sort(Comparator.comparingLong(Tally::total).reversed());
        return out;
    }

    private static String clip(String s, int width) {
        return s.length() <= width ? s : s.substring(0, width - 1) + "~";
    }

    /** A duration in whatever unit reads best. */
    static String dur(long nanos) {
        if (nanos < 1_000L) {
            return nanos + "ns";
        }
        if (nanos < 1_000_000L) {
            return String.format(Locale.ROOT, "%.1fus", nanos / 1_000.0);
        }
        if (nanos < 1_000_000_000L) {
            return String.format(Locale.ROOT, "%.2fms", nanos / 1_000_000.0);
        }
        return String.format(Locale.ROOT, "%.2fs", nanos / 1_000_000_000.0);
    }

    private static String bytes(long b) {
        if (b < 1024) {
            return b + "B";
        }
        if (b < 1024 * 1024) {
            return String.format(Locale.ROOT, "%.1fKB", b / 1024.0);
        }
        if (b < 1024L * 1024 * 1024) {
            return String.format(Locale.ROOT, "%.1fMB", b / (1024.0 * 1024));
        }
        return String.format(Locale.ROOT, "%.2fGB", b / (1024.0 * 1024 * 1024));
    }
}
