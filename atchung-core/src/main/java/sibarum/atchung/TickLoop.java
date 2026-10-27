package sibarum.atchung;

/**
 * A chain of messages longer than a {@link Tick} allows: one message's delivery sent another, whose delivery sent
 * another, past the limit. Raised after the {@link Fatal} policy has run, as {@link MailboxOverflow} is, and for the
 * same reason: a loop on a worker would otherwise be an exception nobody reads and a lane that never settles.
 *
 * <p>A chain that long is not a busy application. A request, its reply and the update they cause are three hops;
 * a loop reaches the limit in microseconds. The message names the topic and the inbox where the limit was crossed,
 * which is somewhere on the loop: follow what that inbox's component sends.
 */
public final class TickLoop extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final transient Topic<?> topic;
    private final String inbox;
    private final int hops;

    TickLoop(Topic<?> topic, String inbox, int hops, int limit) {
        super("a message on topic '" + topic.name() + "' for inbox '" + inbox + "' is " + hops + " hops from the "
                + "message that started its chain, past the tick's limit of " + limit + ": each delivery is sending "
                + "the next, so this is a loop. It runs through '" + inbox + "'; what that component sends, and what "
                + "the subscribers of those topics send back, is the cycle.");
        this.topic = topic;
        this.inbox = inbox;
        this.hops = hops;
    }

    /** The topic of the message that crossed the limit. */
    public Topic<?> topic() {
        return topic;
    }

    /** The inbox it was for. */
    public String inbox() {
        return inbox;
    }

    /** How many hops it was from the message that started its chain. */
    public int hops() {
        return hops;
    }
}
