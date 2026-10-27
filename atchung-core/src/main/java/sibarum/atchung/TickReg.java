package sibarum.atchung;

import java.util.HashMap;
import java.util.Set;

/**
 * One subscription of an {@link Inbox}: the bus's routing entry for it. A publish from outside a tick reaches
 * {@link #doDeliver} on the publisher's thread and is staged; a send inside one reaches {@link #enqueue} on the
 * coordinator, at the end of the round that sent it.
 */
final class TickReg<T> extends Atchung.Reg<T> {

    final Inbox inbox;
    final Subscriber<T> subscriber;
    private final int capacity;
    /** The keys this subscription takes, or null for all of them. Only ever set on a state topic. */
    private final Set<?> keys;

    /** On a state topic, the queued message for each key. Coordinator between rounds, the lane during one. */
    final HashMap<Object, Inbox.Entry> cells;
    /** How many edges are queued, against {@link #capacity}. Coordinator between rounds, the lane during one. */
    int edges;

    long delivered;
    final long subscribedMillis = System.currentTimeMillis();
    long lastDeliveredMillis;
    /** Set by closing; the delivery that finds it set skips the message. */
    volatile boolean closed;

    TickReg(Inbox inbox, Topic<T> topic, Subscriber<T> subscriber, int capacity, Set<?> keys) {
        super(topic);
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be >= 1, was " + capacity);
        }
        this.inbox = inbox;
        this.subscriber = subscriber;
        this.capacity = capacity;
        this.keys = keys;
        this.cells = topic.kind() == Topic.Kind.STATE ? new HashMap<>() : null;
    }

    /** A publish from outside the tick, on the publisher's thread: staged for the next one. */
    @Override
    void doDeliver(T event) {
        inbox.tick.stage(this, event, sibarum.probe.Cause.current());
    }

    /** Queue {@code event} for the next round, folding by key on a state topic. Coordinator only. */
    void enqueue(T event, long cause, int hops) {
        if (closed || isPaused()) {
            return;
        }
        if (hops > inbox.tick.maxHops) {
            TickLoop loop = new TickLoop(topic(), inbox.name, hops, inbox.tick.maxHops);
            Atchung.fatal().fault(loop);
            throw loop;
        }
        // This subscription's own topic decides the key: it is the declaration this inbox was built against, and the
        // bus has already refused any declaration of the channel that disagrees with it about the kind.
        Object key = topic().keyOf(event);
        if (keys != null && !keys.contains(key)) {
            return;
        }
        if (key == null && edges >= capacity) {
            MailboxOverflow overflow = new MailboxOverflow(topic(), capacity, delivered, subscribedMillis,
                    lastDeliveredMillis);
            Atchung.fatal().fault(overflow);
            throw overflow;
        }
        inbox.enqueue(this, event, key, cause, hops);
    }
}
