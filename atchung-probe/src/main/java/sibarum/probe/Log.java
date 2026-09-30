package sibarum.probe;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * A named logger. Get one per class, once, and keep it:
 *
 * <pre>{@code
 * private static final Log LOG = Log.of(Shell.class);
 *
 * LOG.info("window opened: {}x{}", width, height);
 * LOG.warn("clipboard unavailable, copy and paste are off", cause);
 * if (LOG.isDebug()) {
 *     LOG.debug("layout took {} nodes", expensiveCount());
 * }
 * }</pre>
 *
 * <h2>Names</h2>
 *
 * A name is dotted and hierarchical, and a level set for one applies to everything beneath it:
 * {@code -Dlog.level.gui=debug} raises {@code gui}, {@code gui.frame} and {@code gui.frame.pacing}. {@link #of(Class)}
 * uses the class's simple name; pass a string ({@code Log.of("gui.frame")}) when a subsystem spans classes, which
 * is usually better — a level is something a person types on a command line, and they should not have to know
 * which class does the work.
 *
 * <h2>What to log, and at which level</h2>
 *
 * See {@link Level}. The two rules that matter: <b>INFO is a story, not a stream</b> — a run's INFO lines should
 * read in order as what happened, and never once per frame or per event — and <b>log the decision, not the
 * data</b>: a path that was chosen, a fallback that was taken and why; not a document, and never a secret, a
 * token or a password. Whatever is passed here is written to a file that outlives the run.
 *
 * <h2>Cost</h2>
 *
 * A call at a disabled level is a volatile read and a comparison; no string is built. The one thing that is
 * still paid is the arguments themselves, so an argument that is expensive to compute goes behind
 * {@link #isDebug()} or is passed as a {@link Supplier}. The {@code {}} form allocates a small array for its
 * arguments when it is enabled, which is why nothing inside the frame loop logs.
 *
 * <p>Loggers are interned: {@code Log.of("x") == Log.of("x")}.
 */
public final class Log {

    private static final ConcurrentHashMap<String, Log> LOGGERS = new ConcurrentHashMap<>();

    private final String name;
    private volatile int seen = -1;
    private volatile Level threshold = Level.OFF;

    private Log(String name) {
        this.name = name;
    }

    public static Log of(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("a logger needs a name");
        }
        return LOGGERS.computeIfAbsent(name.trim(), Log::new);
    }

    public static Log of(Class<?> type) {
        return of(type.getSimpleName().isEmpty() ? type.getName() : type.getSimpleName());
    }

    public String name() {
        return name;
    }

    // --- enabled ------------------------------------------------------------------------------------------

    /** Whether a record at {@code level} would be delivered anywhere. */
    public boolean enabled(Level level) {
        int version = Logging.VERSION.get();
        if (version != seen) {
            threshold = Logging.thresholdFor(name);
            seen = version;
        }
        return threshold.admits(level);
    }

    public boolean isTrace() {
        return enabled(Level.TRACE);
    }

    public boolean isDebug() {
        return enabled(Level.DEBUG);
    }

    public boolean isInfo() {
        return enabled(Level.INFO);
    }

    // --- writing ------------------------------------------------------------------------------------------

    public void error(String message) {
        log(Level.ERROR, message, null);
    }

    public void error(String message, Throwable thrown) {
        log(Level.ERROR, message, thrown);
    }

    public void error(String format, Object... args) {
        logf(Level.ERROR, format, args);
    }

    public void warn(String message) {
        log(Level.WARN, message, null);
    }

    public void warn(String message, Throwable thrown) {
        log(Level.WARN, message, thrown);
    }

    public void warn(String format, Object... args) {
        logf(Level.WARN, format, args);
    }

    public void info(String message) {
        log(Level.INFO, message, null);
    }

    public void info(String message, Throwable thrown) {
        log(Level.INFO, message, thrown);
    }

    public void info(String format, Object... args) {
        logf(Level.INFO, format, args);
    }

    public void debug(String message) {
        log(Level.DEBUG, message, null);
    }

    public void debug(String message, Throwable thrown) {
        log(Level.DEBUG, message, thrown);
    }

    public void debug(String format, Object... args) {
        logf(Level.DEBUG, format, args);
    }

    public void debug(Supplier<String> message) {
        if (enabled(Level.DEBUG)) {
            Logging.emit(name, Level.DEBUG, message.get(), null);
        }
    }

    public void trace(String message) {
        log(Level.TRACE, message, null);
    }

    public void trace(String format, Object... args) {
        logf(Level.TRACE, format, args);
    }

    public void trace(Supplier<String> message) {
        if (enabled(Level.TRACE)) {
            Logging.emit(name, Level.TRACE, message.get(), null);
        }
    }

    /**
     * Log at {@code level}, for a caller that computes the level.
     */
    public void log(Level level, String message, Throwable thrown) {
        if (enabled(level)) {
            Logging.emit(name, level, message == null ? "null" : message, thrown);
        }
    }

    /**
     * Warn once per {@code key}, and never again.
     *
     * <p>For the seams that run per pipeline, per frame or per glyph, where a warning that repeats is a warning
     * that is filtered out by eye. <b>The key identifies the call site, not the data</b> — {@code "ConeField/albedo"},
     * not the name of the surface: a data-derived key defeats warn-once and grows memory with it, so the number
     * of keys is capped, and reaching the cap is itself reported.
     *
     * @return whether this call logged
     */
    public boolean warnOnce(String key, String message) {
        if (!Logging.firstTime(name + "/" + key)) {
            return false;
        }
        log(Level.WARN, message, null);
        return true;
    }

    // --- formatting ---------------------------------------------------------------------------------------

    private void logf(Level level, String format, Object[] args) {
        if (!enabled(level)) {
            return;
        }
        Throwable thrown = null;
        int placeholders = count(format);
        // SLF4J's convention, because it is the one everybody already knows: a Throwable left over after the
        // placeholders are filled is the exception, not another argument.
        if (args.length > placeholders && args.length > 0 && args[args.length - 1] instanceof Throwable t) {
            thrown = t;
        }
        Logging.emit(name, level, fill(format, args, placeholders), thrown);
    }

    private static int count(String format) {
        int n = 0;
        for (int i = format.indexOf("{}"); i >= 0; i = format.indexOf("{}", i + 2)) {
            n++;
        }
        return n;
    }

    private static String fill(String format, Object[] args, int placeholders) {
        if (placeholders == 0) {
            return format;
        }
        StringBuilder sb = new StringBuilder(format.length() + 16 * placeholders);
        int from = 0;
        int used = 0;
        for (int at = format.indexOf("{}"); at >= 0; at = format.indexOf("{}", from)) {
            sb.append(format, from, at);
            sb.append(used < args.length ? show(args[used]) : "{}");
            used++;
            from = at + 2;
        }
        return sb.append(format, from, format.length()).toString();
    }

    /** {@code String.valueOf}, except that a {@code toString} that throws cannot take the logger down with it. */
    private static String show(Object arg) {
        try {
            return String.valueOf(arg);
        } catch (RuntimeException e) {
            return "<" + arg.getClass().getName() + ".toString() threw " + e + ">";
        }
    }
}
