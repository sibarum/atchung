package sibarum.probe;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The one place logging is configured and delivered from. Application and library code writes through {@link Log};
 * this is what an application's startup calls, and what a test or a driver reaches to change a level.
 *
 * <h2>What an application does</h2>
 *
 * One line, first, before anything that might log:
 *
 * <pre>{@code
 * Logging.configure("editor", null);       // null: detect the mode, as LogConfig describes
 * }</pre>
 *
 * The framework's {@code VexelApplication.run} does this for every application built on it, with the mode it
 * knows from the launch. A library never calls it: a library writes to {@link Log} and lets whoever started the
 * process decide where that goes. If nobody configures, the first record resolves the same defaults from the
 * environment, so a library used on its own still behaves.
 *
 * <h2>Standard output is not a log</h2>
 *
 * Records go to <b>standard error</b> and to the file, never to standard output. Standard output is for what a
 * program <i>produces</i>: {@code ottermate --launch} reads the application's {@code automation: localhost:<port>}
 * line from it, and a pipeline reads a tool's answer from it. A log line in that stream is a line every consumer
 * has to know to skip.
 *
 * <h2>Why this lives beside {@link Probe}</h2>
 *
 * Every layer of the stack already depends on this module and on nothing else it does not need, so putting logging
 * here gives each of them a logger without a new edge in any dependency graph. The two are halves of one
 * question: the probe answers <i>how long, how often and how much</i>, in rollups and correlation traces; the log
 * answers <i>what happened</i>, in sentences. In {@link Mode#AUTOMATION} the probe is switched on by default,
 * and every log record at {@code DEBUG} or below is also written into its trace, so the two can be read as one
 * story in time order.
 *
 * <h2>Threads and cost</h2>
 *
 * A record is formatted and written on the calling thread, under a lock, and a file is flushed at once for
 * {@code WARN} and above and once a second otherwise (at once for everything in {@link Mode#AUTOMATION}, where
 * somebody is reading along). A disabled record costs a volatile read and a comparison, and the string is never
 * built — but <b>not inside the frame loop</b>: code under {@code FrameHooks.run} or {@code Pacing} must not log
 * at all, because even a disabled call site there is a branch on the hot path that {@link Probe} already pays
 * for, and an enabled one takes a lock.
 */
public final class Logging {

    /** How many distinct {@link Log#warnOnce} keys are remembered; see {@code Diagnostics.KEY_LIMIT}. */
    static final int ONCE_LIMIT = 256;

    /** Bumped whenever levels change, so a {@link Log} knows to recompute its threshold. */
    static final AtomicInteger VERSION = new AtomicInteger();

    private static final DateTimeFormatter FILE_TIME =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter CONSOLE_TIME =
            DateTimeFormatter.ofPattern("HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

    private static final Object CONSOLE_LOCK = new Object();
    private static final java.util.Set<String> ONCE = ConcurrentHashMap.newKeySet();
    private static final CopyOnWriteArrayList<Capture> CAPTURES = new CopyOnWriteArrayList<>();
    private static final AtomicBoolean HOOKED = new AtomicBoolean();
    private static final Object LOCK = new Object();

    private static volatile State state;
    private static volatile Thread flusher;

    private Logging() {
    }

    /** The configuration in force and the file it writes, swapped together. */
    private record State(LogConfig config, LogFile file, boolean flushEach) {
    }

    // --- configuration ------------------------------------------------------------------------------------

    /**
     * Configure logging for the application called {@code app}, and log a banner saying what it decided.
     *
     * <p>Replaces whatever was in force, so it may be called after a library has already logged. Call it once,
     * first. It also installs, if nobody has, an uncaught-exception handler that logs the exception at
     * {@code ERROR} — with its stack, into the file as well as the console — and a shutdown hook that flushes
     * the file.
     *
     * @param app  the application's name, which names the file and the directory; {@code null} to use
     *             {@code -Dlog.app} and then {@code "vexelray"}
     * @param mode the mode if the caller knows it; {@code null} to detect it
     * @return what was resolved, for a caller that wants to show it
     */
    public static LogConfig configure(String app, Mode mode) {
        return configure(app, mode, Map.of());
    }

    /**
     * As {@link #configure(String, Mode)}, with settings this launch knows and the properties do not. A value in
     * {@code launch} wins over the same key as a property or an environment variable: the framework hands over
     * {@code automation} (from {@code --automation=0}) and {@code log.level} (from {@code --log=debug}) so that
     * the mode and the level are right from the first record, before anything has set a property.
     */
    public static LogConfig configure(String app, Mode mode, Map<String, String> launch) {
        LogConfig config = LogConfig.resolve(app, mode, LogConfig.Env.system().with(launch));
        install(config, true);
        return config;
    }

    /** Configure with a configuration built elsewhere — a test, or a host with its own rules. */
    public static LogConfig configure(LogConfig config) {
        install(config, false);
        return config;
    }

    /** The configuration in force. Resolves the defaults from the environment if nothing has configured yet. */
    public static LogConfig config() {
        return state().config();
    }

    private static State state() {
        State s = state;
        if (s == null) {
            synchronized (LOCK) {
                s = state;
                if (s == null) {
                    // Not announced: a library that logged before the application configured has nothing to
                    // announce, and the application's own configure() will.
                    install(LogConfig.resolve(null, null, LogConfig.Env.system()), false, false);
                    s = state;
                }
            }
        }
        return s;
    }

    private static void install(LogConfig config, boolean process) {
        install(config, process, true);
    }

    private static void install(LogConfig config, boolean process, boolean announce) {
        synchronized (LOCK) {
            State old = state;
            LogFile file = config.fileEnabled() ? new LogFile(config.logFile(), config.rotateBytes(), config.rotateKeep()) : null;
            boolean flushEach = config.mode() == Mode.AUTOMATION || config.mode() == Mode.TEST;
            state = new State(config, file, flushEach);
            VERSION.incrementAndGet();
            if (old != null && old.file() != null) {
                old.file().close();
            }
            if (file != null && !flushEach) {
                startFlusher();
            }
            hookShutdown();
            if (process && Thread.getDefaultUncaughtExceptionHandler() == null) {
                Log uncaught = Log.of("uncaught");
                Thread.setDefaultUncaughtExceptionHandler((thread, thrown) ->
                        uncaught.error("uncaught exception in thread " + thread.getName(), thrown));
            }
        }
        if (announce) {
            banner(config);
        }
    }

    private static void banner(LogConfig c) {
        Log log = Log.of("log");
        log.info("starting {}: mode={} console={} file={} format={} pid={} java={} os={}", c.app(), c.mode(),
                c.console(), c.fileEnabled() ? c.file() + " -> " + c.logFile() : "off",
                c.format().name().toLowerCase(java.util.Locale.ROOT), ProcessHandle.current().pid(),
                System.getProperty("java.version"), System.getProperty("os.name"));
        if (!c.overrides().isEmpty()) {
            log.info("per-logger levels: {}", c.overrides());
        }
        for (String problem : c.problems()) {
            log.warn("log setting ignored: {}", problem);
        }
    }

    /**
     * Change a level while the application runs — what {@code ottermate}'s {@code log} verb calls.
     *
     * @param logger the logger, and everything beneath it, to hold to {@code level}; {@code null} or empty to
     *               change both sinks' thresholds instead
     */
    public static void setLevel(String logger, Level level) {
        synchronized (LOCK) {
            State s = state();
            LogConfig c = s.config();
            LogConfig next;
            if (logger == null || logger.isBlank()) {
                next = new LogConfig(c.app(), c.mode(), level, c.fileEnabled() ? level : c.file(), c.overrides(),
                        c.dir(), c.format(), c.rotateBytes(), c.rotateKeep(), c.probeLanes(), c.problems());
            } else {
                Map<String, Level> overrides = new java.util.TreeMap<>(c.overrides());
                overrides.put(logger.trim(), level);
                next = new LogConfig(c.app(), c.mode(), c.console(), c.file(), overrides, c.dir(), c.format(),
                        c.rotateBytes(), c.rotateKeep(), c.probeLanes(), c.problems());
            }
            state = new State(next, s.file(), s.flushEach());
            VERSION.incrementAndGet();
        }
    }

    // --- what the probe asks -------------------------------------------------------------------------------

    /** The probe lanes this run's mode turns on when nobody said, or {@code null}. Read once, by {@link Probe}. */
    static String probeLanes() {
        LogConfig c = state().config();
        // With the file off there is nowhere beside the log to put a trace, and the probe's own default is
        // standard output, which is not somewhere a trace may go. So no file means no default probe.
        return c.probeFile() == null ? null : c.probeLanes();
    }

    /** Where the probe's trace goes when {@link #probeLanes} applies, or {@code null}. */
    static Path probeFile() {
        return state().config().probeFile();
    }

    // --- capture, for tests --------------------------------------------------------------------------------

    /** One delivered record. */
    public record Entry(Instant time, Level level, String logger, String thread, String message, Throwable thrown) {
    }

    /**
     * Start recording every record that passes a logger's level, into memory, whatever the sinks do with it.
     *
     * <p>Independent of printing, for the reason {@code Diagnostics} records: a warning nobody has watched fire is
     * one more instrument taken on faith, and a test has to be able to watch without the suite being noisy.
     * Close it when done.
     */
    public static Capture capture() {
        Capture c = new Capture();
        CAPTURES.add(c);
        return c;
    }

    /** Records, collected until closed. */
    public static final class Capture implements AutoCloseable {

        private final List<Entry> entries = new CopyOnWriteArrayList<>();

        private Capture() {
        }

        public List<Entry> records() {
            return List.copyOf(entries);
        }

        /** Whether a record at exactly {@code level} contains {@code text}. */
        public boolean has(Level level, String text) {
            return entries.stream().anyMatch(e -> e.level() == level && e.message().contains(text));
        }

        @Override
        public void close() {
            CAPTURES.remove(this);
        }
    }

    // --- delivery -------------------------------------------------------------------------------------------

    /** The threshold a logger called {@code name} is held to: its own override, or the lowest any sink wants. */
    static Level thresholdFor(String name) {
        LogConfig c = state().config();
        Level own = c.overrideFor(name);
        return own != null ? own : c.floor();
    }

    static boolean firstTime(String key) {
        if (ONCE.size() >= ONCE_LIMIT && !ONCE.contains(key)) {
            // A data-derived key: say so once, and stop remembering rather than grow without bound.
            if (ONCE.add("Logging/onceLimit")) {
                Log.of("log").warn("more than {} distinct warn-once keys; a key is built from data, not from a call site",
                        ONCE_LIMIT);
            }
            return false;
        }
        return ONCE.add(key);
    }

    static void emit(String logger, Level level, String message, Throwable thrown) {
        State s = state();
        LogConfig c = s.config();
        Level own = c.overrideFor(logger);
        boolean toConsole;
        boolean toFile;
        boolean toCaptures;
        if (own != null) {
            boolean admitted = own.admits(level);
            toConsole = admitted && c.console() != Level.OFF;
            toFile = admitted && s.file() != null;
            toCaptures = admitted;
        } else {
            toConsole = c.console().admits(level);
            toFile = s.file() != null && c.file().admits(level);
            toCaptures = c.floor().admits(level);
        }
        toCaptures &= !CAPTURES.isEmpty();
        if (!toConsole && !toFile && !toCaptures) {
            return;
        }
        Instant now = Instant.now();
        String thread = Thread.currentThread().getName();
        if (toCaptures) {
            Entry entry = new Entry(now, level, logger, thread, message, thrown);
            for (Capture capture : CAPTURES) {
                capture.entries.add(entry);
            }
        }
        if (toConsole) {
            String line = format(c.format(), CONSOLE_TIME, now, level, thread, logger, message, thrown);
            synchronized (CONSOLE_LOCK) {
                System.err.print(line);
                System.err.flush();
            }
        }
        if (toFile) {
            s.file().write(format(c.format(), FILE_TIME, now, level, thread, logger, message, thrown),
                    s.flushEach() || level.ordinal() >= Level.WARN.ordinal());
        }
        if (Probe.ON && level.ordinal() <= Level.DEBUG.ordinal() + 1) {
            // Into the correlation trace, so the log and the spans read as one story in time order. INFO and
            // below only: a WARN or ERROR is already on the console and does not need a second copy in a trace
            // whose job is timing.
            Probe.mark(Lane.APP, "log." + level.name().toLowerCase(java.util.Locale.ROOT), logger + ": " + message);
        }
    }

    /** Flush the file, now. A host about to exit without a normal shutdown calls this. */
    public static void flush() {
        State s = state;
        if (s != null && s.file() != null) {
            s.file().flush();
        }
    }

    // --- formats --------------------------------------------------------------------------------------------

    private static String format(LogConfig.Format format, DateTimeFormatter time, Instant at, Level level,
                                 String thread, String logger, String message, Throwable thrown) {
        if (format == LogConfig.Format.JSON) {
            StringBuilder sb = new StringBuilder(160);
            sb.append("{\"t\":\"").append(FILE_TIME.format(at)).append("\",\"level\":\"").append(level)
                    .append("\",\"thread\":\"");
            escape(sb, thread);
            sb.append("\",\"logger\":\"");
            escape(sb, logger);
            sb.append("\",\"msg\":\"");
            escape(sb, message);
            sb.append('"');
            if (thrown != null) {
                sb.append(",\"exception\":\"");
                escape(sb, stack(thrown));
                sb.append('"');
            }
            return sb.append("}\n").toString();
        }
        StringBuilder sb = new StringBuilder(128);
        sb.append(time.format(at)).append(' ').append(level.padded()).append(" [").append(thread).append("] ")
                .append(logger).append(" - ").append(message).append('\n');
        if (thrown != null) {
            sb.append(stack(thrown)).append('\n');
        }
        return sb.toString();
    }

    private static String stack(Throwable thrown) {
        StringWriter sw = new StringWriter();
        thrown.printStackTrace(new PrintWriter(sw));
        String text = sw.toString();
        return text.endsWith("\n") ? text.substring(0, text.length() - 1) : text;
    }

    private static void escape(StringBuilder sb, String s) {
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            switch (ch) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (ch < 0x20) {
                        sb.append(String.format("\\u%04x", (int) ch));
                    } else {
                        sb.append(ch);
                    }
                }
            }
        }
    }

    // --- housekeeping ---------------------------------------------------------------------------------------

    private static void hookShutdown() {
        if (HOOKED.compareAndSet(false, true)) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                State s = state;
                if (s != null && s.file() != null) {
                    s.file().close();
                }
            }, "vexelray-log-close"));
        }
    }

    /** One daemon that flushes the file once a second: a record is never more than that late, even if the process dies. */
    private static void startFlusher() {
        if (flusher != null) {
            return;
        }
        Thread t = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    return;
                }
                State s = state;
                if (s != null && s.file() != null) {
                    s.file().flush();
                }
            }
        }, "vexelray-log-flush");
        t.setDaemon(true);
        flusher = t;
        t.start();
    }

    /** Forget everything, for a test that needs to start from nothing. Not for application code. */
    static void reset() {
        synchronized (LOCK) {
            State s = state;
            if (s != null && s.file() != null) {
                s.file().close();
            }
            state = null;
            ONCE.clear();
            VERSION.incrementAndGet();
        }
    }
}
