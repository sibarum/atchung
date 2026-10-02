package sibarum.atchung;

import java.time.Instant;
import java.util.Set;

/**
 * A {@link Backpressure#FAIL} mailbox was full when an event arrived for it: the consumer is not keeping up,
 * and this subscription has said it would rather stop than lose an event.
 *
 * <p>Carries everything needed to act on it without a debugger — which topic, which capacity, how many events
 * that subscription had already delivered, <b>who published the event that did not fit and when, and when the
 * consumer last drained</b> — because the one thing an overflow report must not be is a message that tells you a
 * queue somewhere filled up. The last two are what separate the two causes: a consumer that drained a moment ago
 * and fell behind is a mailbox too small for its traffic, and one that has not drained for seconds, or ever, is
 * a consumer that is stuck.
 *
 * <p>The payload is deliberately not here. It may be large or private, and what a reader needs is where the
 * traffic came from, not a copy of it.
 */
public final class MailboxOverflow extends RuntimeException {

    private static final long serialVersionUID = 2L;

    /** Frames of this package that are the bus itself, skipped when naming the caller that published. */
    private static final Set<String> INTERNAL = Set.of(
            "sibarum.atchung.Atchung", "sibarum.atchung.Atchung$Reg", "sibarum.atchung.PumpedReg",
            "sibarum.atchung.MailboxOverflow");

    private final transient Topic<?> topic;
    private final int capacity;
    private final long delivered;
    private final long publishedMillis;
    private final String publisherThread;
    private final StackTraceElement origin;
    private final long lastDrainMillis;

    /**
     * @param subscribedMillis when the subscription was made, as {@link System#currentTimeMillis()}
     * @param lastDrainMillis  when the consumer last took a batch, or {@code 0} if it never has
     */
    public MailboxOverflow(Topic<?> topic, int capacity, long delivered, long subscribedMillis,
                           long lastDrainMillis) {
        this(topic, capacity, delivered, subscribedMillis, lastDrainMillis, System.currentTimeMillis(),
                Thread.currentThread().getName(), originOf(new Throwable().getStackTrace()));
    }

    private MailboxOverflow(Topic<?> topic, int capacity, long delivered, long subscribedMillis,
                            long lastDrainMillis, long now, String thread, StackTraceElement origin) {
        super("mailbox for topic '" + topic.name() + "' is full at capacity " + capacity + " after "
                + delivered + " delivered event(s): the consumer is not draining fast enough, and this "
                + "subscription is FAIL rather than lossy. Either the consumer has stalled, or this topic "
                + "carries more traffic than its mailbox was sized for. The event was published at "
                + Instant.ofEpochMilli(now) + " by thread '" + thread + "' from "
                + (origin == null ? "an unknown caller" : origin) + ". "
                + (lastDrainMillis == 0
                        ? "The mailbox has never been drained, in the " + seconds(now - subscribedMillis)
                                + " since it was subscribed."
                        : "The mailbox was last drained at " + Instant.ofEpochMilli(lastDrainMillis) + ", "
                                + seconds(now - lastDrainMillis) + " earlier."));
        this.topic = topic;
        this.capacity = capacity;
        this.delivered = delivered;
        this.publishedMillis = now;
        this.publisherThread = thread;
        this.origin = origin;
        this.lastDrainMillis = lastDrainMillis;
    }

    /** The first frame that is not the bus: whoever called {@code publish}. Null if there is none. */
    private static StackTraceElement originOf(StackTraceElement[] stack) {
        for (StackTraceElement frame : stack) {
            if (!INTERNAL.contains(frame.getClassName())) {
                return frame;
            }
        }
        return null;
    }

    private static String seconds(long millis) {
        return String.format("%.1f s", Math.max(0, millis) / 1000.0);
    }

    /** The topic whose mailbox overflowed. */
    public Topic<?> topic() {
        return topic;
    }

    /** The mailbox bound that was reached. */
    public int capacity() {
        return capacity;
    }

    /** How many events this subscription had delivered before the overflow — a stall shows up as a small number. */
    public long delivered() {
        return delivered;
    }

    /** When the event that did not fit was published, as {@link System#currentTimeMillis()}. */
    public long publishedMillis() {
        return publishedMillis;
    }

    /** The name of the thread that published it. */
    public String publisherThread() {
        return publisherThread;
    }

    /** The code that called {@code publish}, or null if it cannot be told. The full stack is this exception's. */
    public StackTraceElement origin() {
        return origin;
    }

    /** When the consumer last took a batch, as {@link System#currentTimeMillis()}, or {@code 0} if it never has. */
    public long lastDrainMillis() {
        return lastDrainMillis;
    }
}
