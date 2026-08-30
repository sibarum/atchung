package sibarum.atchung;

/**
 * A {@link Backpressure#FAIL} mailbox was full when an event arrived for it: the consumer is not keeping up,
 * and this subscription has said it would rather stop than lose an event.
 *
 * <p>Carries everything needed to act on it without a debugger — which topic, which capacity, and how many
 * events that subscription had already delivered — because the one thing an overflow report must not be is a
 * message that tells you a queue somewhere filled up.
 */
public final class MailboxOverflow extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final transient Topic<?> topic;
    private final int capacity;
    private final long delivered;

    public MailboxOverflow(Topic<?> topic, int capacity, long delivered) {
        super("mailbox for topic '" + topic.name() + "' is full at capacity " + capacity + " after "
                + delivered + " delivered event(s): the consumer is not draining fast enough, and this "
                + "subscription is FAIL rather than lossy. Either the consumer has stalled, or this topic "
                + "carries more traffic than its mailbox was sized for.");
        this.topic = topic;
        this.capacity = capacity;
        this.delivered = delivered;
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
}
