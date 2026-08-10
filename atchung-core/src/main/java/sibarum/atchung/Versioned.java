package sibarum.atchung;

/**
 * An immutable snapshot of a {@link State} at one version. Every commit produces a new {@code Versioned}
 * with a monotonically increasing {@link #version()}; the initial state is version {@code 0}.
 *
 * @param value          the immutable state value at this version
 * @param version        the monotonic version number (0 = initial, +1 per commit)
 * @param timestampNanos capture time from {@link System#nanoTime()} — for freshness/history TTL, not wall-clock
 * @param <T>            the state type
 */
public record Versioned<T>(T value, long version, long timestampNanos) {
}
