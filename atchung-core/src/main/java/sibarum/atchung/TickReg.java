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
    /** Where this subscription is among its inbox's, counting from zero: how a recording names it ({@link Tick.Recorder}). */
    final int index;
    final Subscriber<T> subscriber;
    private final int capacity;
    /** The keys this subscription takes, or null for all of them. Only ever set on a keyed topic: see Topic.keyed. */
    private final Set<?> keys;
    /** Whether {@link #keys} are scopes ({@link Topic#scoped}) rather than keys. */
    private final boolean byScope;

    /** On a state topic, the queued message for each key. Coordinator between rounds, the lane during one. */
    final HashMap<Object, Inbox.Entry> cells;
    /**
     * How many edges are queued, against {@link #capacity}. Written by the coordinator between rounds and the lane during
     * one; volatile so a {@link Sender#paced paced} sender on another thread can read how much room is left.
     */
    volatile int edges;
    /**
     * How many edges are staged for this subscription and not yet queued: what a paced sender counts beside
     * {@link #edges}. Written under the tick's lock, on staging and on taking what was staged.
     */
    volatile int stagedEdges;
    /** How many paced senders are waiting for room here, so a drain wakes them only when there are any. */
    volatile int waiting;
    /** What a paced sender waits on, and a drain or a close wakes. */
    private final Object room = new Object();

    long delivered;
    /**
     * What each {@code receive} row of this subscription says first, {@code <topic> to <inbox>} and then the mailbox's
     * name when it has one; null unprofiled.
     */
    final String receiveRow;
    final long subscribedMillis = System.currentTimeMillis();
    long lastDeliveredMillis;
    /** Set by closing; the delivery that finds it set skips the message. */
    volatile boolean closed;

    TickReg(Inbox inbox, String mailbox, Topic<T> topic, Subscriber<T> subscriber, int capacity, Set<?> keys,
            boolean byScope) {
        super(topic);
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be >= 1, was " + capacity);
        }
        this.inbox = inbox;
        this.index = inbox.regs.size();
        this.receiveRow = !Probe.ON ? null
                : topic.name() + " to " + inbox.name + (mailbox == null ? "" : " " + mailbox);
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

    /** Whether one more edge fits: what is staged and what is queued, against the capacity. Any thread. */
    boolean hasRoom() {
        return stagedEdges + edges < capacity;
    }

    /**
     * Wait, on the caller's thread, until one more edge fits or this subscription is closed: what a
     * {@link Sender#paced paced} sender does before it publishes. True at once on a state topic, which cannot overflow.
     * False when the wait was interrupted, with the interrupt flag set again.
     */
    boolean awaitRoom() {
        if (cells != null || closed || hasRoom()) {
            return true;
        }
        synchronized (room) {
            waiting++;
            try {
                // Waiting is counted before room is read, and a drain lowers the count before it reads waiting, so one
                // of the two sees the other: either this sees the room, or the drain sees a waiter and wakes it.
                while (!closed && !hasRoom()) {
                    room.wait();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } finally {
                waiting--;
            }
        }
        return true;
    }

    /** Wake every paced sender waiting for room here. After a drain or a close; any thread. */
    void wakeRoom() {
        synchronized (room) {
            room.notifyAll();
        }
    }

    /** A publish from outside the tick, on the publisher's thread: staged for the next one. */
    @Override
    void doDeliver(T event) {
        // Filtered before it is staged as well as when it is queued: a tick that does not run (a closed window's) must
        // not hold what it would only discard.
        if (!accepts(event)) {
            return;
        }
        inbox.tick.stage(this, event, sibarum.probe.Cause.current(), Runs.current());
    }

    /** Queue {@code event} for the next round, folding by key on a state topic. Coordinator only. */
    void enqueue(T event, long cause, long run, int hops) {
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
        inbox.enqueue(this, event, key, cause, run, hops);
    }
}
