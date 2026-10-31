package sibarum.atchung;

import java.util.HashMap;
import java.util.Set;

import sibarum.probe.Probe;

/**
 * One subscription of an {@link Inbox}: the bus's routing entry for it. A publish from outside a tick reaches
 * {@link #doDeliver} on the publisher's thread and is staged; a send inside one reaches {@link #enqueue} on the
 * coordinator, at the end of the round that sent it.
 */
final class TickReg<T> extends Atchung.Reg<T> {

    final Inbox inbox;
    final Subscriber<T> subscriber;
    private final int capacity;
    /** The keys this subscription takes, or null for all of them. Only ever set on a keyed topic: see Topic.keyed. */
    private final Set<?> keys;
    /** Whether {@link #keys} are scopes ({@link Topic#scoped}) rather than keys. */
    private final boolean byScope;

    /** On a state topic, the queued message for each key. Coordinator between rounds, the lane during one. */
    final HashMap<Object, Inbox.Entry> cells;
    /** How many edges are queued, against {@link #capacity}. Coordinator between rounds, the lane during one. */
    int edges;

    long delivered;
    /** What each {@code receive} row of this subscription says first, {@code <topic> to <inbox>}; null unprofiled. */
    final String receiveRow;
    final long subscribedMillis = System.currentTimeMillis();
    long lastDeliveredMillis;
    /** Set by closing; the delivery that finds it set skips the message. */
    volatile boolean closed;

    TickReg(Inbox inbox, Topic<T> topic, Subscriber<T> subscriber, int capacity, Set<?> keys, boolean byScope) {
        super(topic);
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be >= 1, was " + capacity);
        }
        this.inbox = inbox;
        this.receiveRow = Probe.ON ? topic.name() + " to " + inbox.name : null;
        this.subscriber = subscriber;
        this.capacity = capacity;
        this.keys = keys;
        this.byScope = byScope;
        this.cells = topic.kind() == Topic.Kind.STATE ? new HashMap<>() : null;
    }

    /** Whether this subscription takes {@code event}: every one, or only those whose key or scope it names. */
    boolean accepts(T event) {
        return keys == null || keys.contains(byScope ? topic().scopeOf(event) : topic().filterKeyOf(event));
    }

    /** {@link #accepts}, for a message held untyped beside its registration, as a tick's hand-over holds it. */
    @SuppressWarnings("unchecked")
    boolean acceptsAny(Object event) {
        return accepts((T) event);
    }

    /** A publish from outside the tick, on the publisher's thread: staged for the next one. */
    @Override
    void doDeliver(T event) {
        // Filtered before it is staged as well as when it is queued: a tick that does not run (a closed window's) must
        // not hold what it would only discard.
        if (!accepts(event)) {
            return;
        }
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
        if (!accepts(event)) {
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
