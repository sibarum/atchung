package sibarum.atchung;

import java.time.Duration;
import java.util.Objects;

/**
 * How long a wait in a {@link Chain} may last: a time, or forever, or until the person answers. Where none is stated,
 * the chain's default applies, and then the tick's (vexelray-framework's {@code docs/plans/chains.md}, rulings 3 to 5).
 */
final class Bound {

    /** No bound: stated with {@code forever()}, and never supplied by a default. */
    static final Bound FOREVER = new Bound(0, "forever");

    /**
     * Bounded by the person's answer, which the framework's dialogs guarantee (closing one unanswered answers too): no
     * time, because a person may take an hour (ruling 4).
     */
    static final Bound PERSON = new Bound(0, "until the person answers");

    final long nanos;
    private final String said;

    private Bound(long nanos, String said) {
        this.nanos = nanos;
        this.said = said;
    }

    static Bound of(Duration time) {
        Objects.requireNonNull(time, "time");
        if (time.isZero() || time.isNegative()) {
            throw new IllegalArgumentException("a bound is some time: " + time);
        }
        return new Bound(time.toNanos(), time.toString());
    }

    /** Whether a timer keeps it. */
    boolean timed() {
        return nanos > 0;
    }

    Duration duration() {
        return Duration.ofNanos(nanos);
    }

    @Override
    public String toString() {
        return said;
    }
}
