package sibarum.atchung;

import java.util.Objects;

import sibarum.probe.Cause;

/**
 * How a component in a {@link Tick} sends on one topic. Taken from the component's own {@link Inbox}
 * ({@link Inbox#sender}), because that is how a send knows where it stands:
 *
 * <ul>
 *   <li><b>Inside one of the inbox's deliveries</b>, the send belongs to the round. It goes into the inbox's outbox
 *       and reaches its subscribers when the round ends, in the order {@link Tick} describes.</li>
 *   <li><b>Anywhere else</b> (a component's constructor, a thread that is not delivering to it), it is a message from
 *       outside, and is published as {@link Atchung#publish} would, which stages it for the next tick.</li>
 * </ul>
 *
 * <p>A sender belongs to the component whose inbox made it. Handed to another component, its sends would be filed
 * as that component's and its order would be wrong, which is why a framework builds senders and never shares one.
 *
 * <p>The other kind is {@link #outside}: a sender for a thread that reads from outside the process. It belongs to
 * no inbox, since it is never inside a delivery. And for a test there is {@link #collecting}, which keeps what is sent
 * in a list and has no bus at all.
 */
public final class Sender<T> {

    private final Inbox inbox;
    private final Topic<T> topic;
    /** For an {@link #outside} sender, its bus and the source each send begins a cause of; else null. */
    private final Atchung bus;
    private final Cause.Source source;
    /** For a {@link #collecting} sender, where what it sends goes; else null. */
    private final java.util.List<? super T> collected;

    Sender(Inbox inbox, Topic<T> topic) {
        this.inbox = inbox;
        this.topic = topic;
        this.bus = null;
        this.source = null;
        this.collected = null;
    }

    private Sender(Atchung bus, Topic<T> topic, Cause.Source source) {
        this.inbox = null;
        this.topic = topic;
        this.bus = bus;
        this.source = source;
        this.collected = null;
    }

    private Sender(Topic<T> topic, java.util.List<? super T> collected) {
        this.inbox = null;
        this.topic = topic;
        this.bus = null;
        this.source = null;
        this.collected = collected;
    }

    /**
     * A sender for a thread outside every tick that reads from outside the process: each send begins a new cause of
     * {@code source}, since the message arrives from outside as input does, so whoever traces it back finds where it
     * came from rather than nothing. Otherwise an ordinary publish: a mailbox it overflows fails as any does, and a
     * publisher that can outrun its readers batches what it sends.
     */
    public static <T> Sender<T> outside(Atchung bus, Topic<T> topic, Cause.Source source) {
        return new Sender<>(Objects.requireNonNull(bus, "bus"), Objects.requireNonNull(topic, "topic"),
                Objects.requireNonNull(source, "source"));
    }

    /**
     * A sender for a test, which adds each message to {@code into} on the thread that sends it, at once, with no bus,
     * tick or inbox: what a job, a source or a widget sends is read straight off the list. It is what a test of a part
     * that takes a sender needs and nothing more, since {@code Sender} is no functional interface
     * (Vexplore framework-notes FN-16). Not for application code: it delivers to nobody. A part that sends from
     * another thread wants a list that is safe there ({@code Collections.synchronizedList}).
     *
     * {@snippet :
     * List<Progress> sent = new ArrayList<>();
     * new Copier(Sender.collecting(PROGRESS, sent)).copy(from, to);
     * assertEquals(3, sent.size());
     * }
     */
    public static <T> Sender<T> collecting(Topic<T> topic, java.util.List<? super T> into) {
        return new Sender<>(Objects.requireNonNull(topic, "topic"), Objects.requireNonNull(into, "into"));
    }

    /** The topic this sends on. */
    public Topic<T> topic() {
        return topic;
    }

    /** Send {@code event}: in this round when called from a delivery to this sender's inbox, else as a publish. */
    public void send(T event) {
        Objects.requireNonNull(event, "event");
        if (collected != null) {
            collected.add(event);
        } else         if (source != null) {
            long previous = Cause.enter(source.next());
            try {
                bus.publish(topic, event);
            } finally {
                Cause.exit(previous);
            }
        } else if (inbox.delivering == Thread.currentThread()) {
            inbox.post(topic, event);
        } else {
            inbox.tick.bus.publish(topic, event);
        }
    }

    @Override
    public String toString() {
        return "Sender[" + topic.name() + " from "
                + (collected != null ? "a test" : source != null ? source.name() : inbox.name) + "]";
    }
}
