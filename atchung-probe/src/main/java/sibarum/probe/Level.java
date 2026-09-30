package sibarum.probe;

import java.util.Locale;

/**
 * How much a record matters, in the order everyone expects: {@code ERROR} is the most severe and the rarest,
 * {@code TRACE} the least and the most frequent.
 *
 * <p>The conventions this stack uses for them, because a level is only useful if two people pick the same one:
 *
 * <ul>
 *   <li>{@link #ERROR} — something failed and the application cannot do what was asked. A person should look.</li>
 *   <li>{@link #WARN} — something went wrong or was dropped and the application carried on, possibly in a worse
 *       state. The class of thing {@code Diagnostics.dropped} reports: a plausible result that is not the one
 *       that was asked for.</li>
 *   <li>{@link #INFO} — the lifecycle, once each: started, configured, window opened, stopped. Readable as a
 *       story of the run. <b>Never per frame and never per event.</b></li>
 *   <li>{@link #DEBUG} — the decisions inside one of those steps: which backend, which path, why a fallback.
 *       Useful to whoever is working on the application; off in a shipped build.</li>
 *   <li>{@link #TRACE} — a record for every step, including the ones that happen every frame. The volume is the
 *       point. Only ever on while somebody is reading along, which is what an automation run is.</li>
 * </ul>
 *
 * <p>{@link #OFF} is a threshold and never a severity: nothing is logged at it, and setting a sink to it turns the
 * sink off.
 */
public enum Level {

    TRACE, DEBUG, INFO, WARN, ERROR, OFF;

    /** The five-character column a line carries, so levels line up in a file. */
    String padded() {
        return switch (this) {
            case TRACE -> "TRACE";
            case DEBUG -> "DEBUG";
            case INFO -> "INFO ";
            case WARN -> "WARN ";
            case ERROR -> "ERROR";
            case OFF -> "OFF  ";
        };
    }

    /** Whether a record at {@code severity} passes a threshold of {@code this}. {@code OFF} passes nothing. */
    public boolean admits(Level severity) {
        return this != OFF && severity != OFF && severity.ordinal() >= ordinal();
    }

    /**
     * The level named by {@code text}, case-insensitively — {@code warning} is accepted for {@code warn}, and
     * {@code none}, {@code false} and {@code 0} for {@code off} — or {@code null} if it names none.
     */
    public static Level parse(String text) {
        if (text == null) {
            return null;
        }
        return switch (text.trim().toLowerCase(Locale.ROOT)) {
            case "trace", "all" -> TRACE;
            case "debug" -> DEBUG;
            case "info" -> INFO;
            case "warn", "warning" -> WARN;
            case "error" -> ERROR;
            case "off", "none", "false", "0" -> OFF;
            default -> null;
        };
    }
}
