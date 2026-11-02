package sibarum.atchung;

import java.time.Duration;

/**
 * Time, as a message: what a {@link Tick} is given so that a {@link Chain}'s waits can be bounded. It publishes a
 * message on a topic after a delay, and runs nothing else.
 *
 * <p>atchung has no clock and takes no thread of its own, so whoever owns the threads supplies one: vexelray-framework
 * hands a tick its {@code Timer}, and a test hands it a {@link HandClock}, moved by hand, so a five-second bound is
 * tested at once. Why time belongs in a chain at all is vexelray-framework's {@code docs/plans/chains.md}, rulings 3 and
 * 14.
 */
@FunctionalInterface
public interface Alarm {

    /**
     * Publish {@code message} on {@code topic} once, {@code delay} from now.
     *
     * @return what stops it, if it has not been published yet
     */
    <T> Cancel after(Duration delay, Topic<T> topic, T message);

    /** A message waiting to be published. */
    @FunctionalInterface
    interface Cancel {
        /** Publish it no more. One already under way may still arrive; whoever hears it checks that it still waits. */
        void cancel();
    }
}
