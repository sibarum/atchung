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
 * <p>The other kind is {@link #waiting}: a sender for a thread that reads from outside the process and is allowed
 * to block. It belongs to no inbox, since it is never inside a delivery.
 */
public final class Sender<T> {

    private final Inbox inbox;
    private final Topic<T> topic;
    /** For a {@link #waiting} sender, its bus and the source each send begins a cause of; else null. */
    private final Atchung bus;
    private final Cause.Source source;

    Sender(Inbox inbox, Topic<T> topic) {
        this.inbox = inbox;
        this.topic = topic;
        this.bus = null;
        this.source = null;
    }

    private Sender(Atchung bus, Topic<T> topic, Cause.Source source) {
        this.inbox = null;
        this.topic = topic;
        this.bus = bus;
        this.source = source;
    }

    /**
     * A sender for a thread outside every tick that reads from outside the process: each send begins a new cause of
     * {@code source}, since the message arrives from outside as input does, and is published by
     * {@link Atchung#publishWaiting}, so a full mailbox makes the sender wait rather than overflow. A send interrupted
     * while waiting is dropped and leaves the interrupt set, for the thread's next blocking read to answer: a thread
     * being stopped has no use for what it was sending. <b>Never use one on a thread that runs ticks.</b>
     */
    public static <T> Sender<T> waiting(Atchung bus, Topic<T> topic, Cause.Source source) {
        return new Sender<>(Objects.requireNonNull(bus, "bus"), Objects.requireNonNull(topic, "topic"),
                Objects.requireNonNull(source, "source"));
    }

    /** The topic this sends on. */
    public Topic<T> topic() {
        return topic;
    }

    /** Send {@code event}: in this round when called from a delivery to this sender's inbox, else as a publish. */
    public void send(T event) {
        Objects.requireNonNull(event, "event");
        if (source != null) {
            long previous = Cause.enter(source.next());
            try {
                bus.publishWaiting(topic, event);
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
        return "Sender[" + topic.name() + " from " + (source != null ? source.name() : inbox.name) + "]";
    }
}
