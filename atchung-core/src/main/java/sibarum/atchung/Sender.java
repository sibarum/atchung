package sibarum.atchung;

import java.util.Objects;

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
 */
public final class Sender<T> {

    private final Inbox inbox;
    private final Topic<T> topic;

    Sender(Inbox inbox, Topic<T> topic) {
        this.inbox = inbox;
        this.topic = topic;
    }

    /** The topic this sends on. */
    public Topic<T> topic() {
        return topic;
    }

    /** Send {@code event}: in this round when called from a delivery to this sender's inbox, else as a publish. */
    public void send(T event) {
        Objects.requireNonNull(event, "event");
        if (inbox.delivering == Thread.currentThread()) {
            inbox.post(topic, event);
        } else {
            inbox.tick.bus.publish(topic, event);
        }
    }

    @Override
    public String toString() {
        return "Sender[" + topic.name() + " from " + inbox.name + "]";
    }
}
