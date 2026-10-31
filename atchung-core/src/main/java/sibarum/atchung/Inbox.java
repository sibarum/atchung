package sibarum.atchung;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import sibarum.probe.Cause;
import sibarum.probe.Lane;
import sibarum.probe.Probe;

/**
 * A component's inbox in a {@link Tick}: one queue for every topic the component subscribes to, so its messages
 * arrive in one order whichever topic each came on. Made by {@link Tick#inbox}.
 *
 * <p>A component subscribes here ({@link #subscribe}) and sends through a {@link Sender} it takes from here
 * ({@link #sender}). The sender is what knows whether a send happens inside one of this inbox's deliveries, and so
 * whether it belongs to the round (see {@link Tick}) or is a message from outside, to be staged for the next tick.
 */
public final class Inbox {

    final Tick tick;
    final String name;
    final int lane;
    /** Where this inbox is in the order outboxes are emptied in. */
    final int position;

    /**
     * One queued message: a link in this inbox's arrival-ordered chain. Recycled, so queueing allocates nothing. Its
     * fields are touched by the coordinator between rounds and by this inbox's lane during one, never both at once.
     */
    static final class Entry {
        TickReg<?> reg;
        Object event;
        /** The piece of state the message writes, or null for an edge. */
        Object key;
        long cause;
        int hops;
        Entry prev;
        Entry next;
    }

    Entry head;
    private Entry tail;
    private Entry free;
    private int freeCount;
    /** How many recycled entries are worth keeping: enough for a busy tick, not a burst's worth forever. */
    private static final int POOL = 1024;

    // This inbox's outbox: what its component sent during the round, emptied by the coordinator at the round's end.
    Topic<?>[] outTopics = new Topic<?>[16];
    Object[] outEvents = new Object[16];
    long[] outCauses = new long[16];
    int[] outHops = new int[16];
    int outSize;

    /**
     * The thread delivering to this inbox right now, or null. Written only by that thread, before and after a
     * delivery, so a thread that reads itself here is inside one: any other thread reads null or another thread,
     * however stale its view, and never itself.
     */
    Thread delivering;
    /** The hops of the message being delivered. Meaningful only to {@link #delivering}. */
    private int hops;
    int deliveredThisRound;
    /** What a subscriber threw in this round, for the coordinator to rethrow. */
    Throwable failure;

    /** Every topic subscribed here, in order: what {@link #topics()} answers. */
    private final List<Topic<?>> subscribed = new ArrayList<>();

    Inbox(Tick tick, String name, int lane, int position) {
        this.tick = tick;
        this.name = name;
        this.lane = lane;
        this.position = position;
    }

    /** This inbox's name, as the tick was told it. */
    public String name() {
        return name;
    }

    /**
     * Every topic subscribed to here, in the order subscribed: so whoever owns the inbox can compare what it hears
     * with what it declared.
     */
    public List<Topic<?>> topics() {
        return List.copyOf(subscribed);
    }

    /**
     * Subscribe to {@code topic}. An edge mailbox holds at most {@code capacity} messages; one more is a fault, as a
     * {@link Backpressure#FAIL} mailbox's is. A state subscription holds one message per key and cannot overflow, so
     * {@code capacity} counts only its edges, of which it has none.
     */
    public <T> Subscription subscribe(Topic<T> topic, Subscriber<T> subscriber, int capacity) {
        return subscribe(topic, subscriber, capacity, null);
    }

    /**
     * As {@link #subscribe(Topic, Subscriber, int)}, delivering only messages whose key is in {@code keys}: the piece
     * of state a {@link Topic.Kind#STATE state} message writes, or the key an edge declared
     * ({@link Topic#edge(Class, Topic.Key)}). A message filtered out is never queued, so it costs this inbox nothing
     * and cannot fill it. An edge that declared no key has nothing to filter by, so a filter on one is refused.
     */
    public <T> Subscription subscribe(Topic<T> topic, Subscriber<T> subscriber, int capacity, Set<?> keys) {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(subscriber, "subscriber");
        if (keys != null && !topic.keyed()) {
            throw new IllegalArgumentException("topic '" + topic.name() + "' is an edge that declares no key, so it "
                    + "has nothing to filter by: declare it with Topic.edge(type, key)");
        }
        return register(topic, subscriber, capacity, keys == null ? null : Set.copyOf(keys), false);
    }

    /**
     * As {@link #subscribe(Topic, Subscriber, int)}, delivering only messages in one of {@code scopes}
     * ({@link Topic#scoped}): which part of the application a message is about, apart from the piece of state it writes.
     * A message from another scope is never staged or queued for this inbox, so an inbox that is not run for a while
     * (the widgets of a closed window, on a bus every window shares) holds nothing for it.
     */
    public <T> Subscription subscribeIn(Topic<T> topic, Subscriber<T> subscriber, int capacity, Set<?> scopes) {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(subscriber, "subscriber");
        Objects.requireNonNull(scopes, "scopes");
        if (!topic.hasScope()) {
            throw new IllegalArgumentException("topic '" + topic.name() + "' names no scope, so it has nothing to "
                    + "filter by: declare it with Topic.edge(type, key) or Topic.state(...).scoped(scope)");
        }
        return register(topic, subscriber, capacity, Set.copyOf(scopes), true);
    }

    private <T> Subscription register(Topic<T> topic, Subscriber<T> subscriber, int capacity, Set<?> keys,
                                      boolean byScope) {
        // Two inboxes hearing one channel as two kinds would have one folding what the other treats as edges, which is
        // the channel with no correct policy. Only inboxes are compared: a Pump or an inline subscriber on the same
        // channel chooses its own policy, as it always has, and is neither checked nor refused here.
        Atchung.Reg<?>[] existing = tick.bus.registered(topic);
        if (existing != null) {
            for (Atchung.Reg<?> r : existing) {
                if (r instanceof TickReg<?> t && t.topic().kind() != topic.kind()) {
                    throw new IllegalStateException("topic '" + topic.name() + "' is heard as " + topic.kind()
                            + " here and as " + t.topic().kind() + " by inbox '" + t.inbox.name + "': a channel "
                            + "carries one kind, so every inbox's declaration of it must say the same one");
                }
            }
        }
        TickReg<T> reg = new TickReg<>(this, topic, subscriber, capacity, keys, byScope);
        subscribed.add(topic);
        Subscription busSub = tick.bus.register(reg);
        return new Subscription() {
            @Override
            public boolean isActive() {
                return busSub.isActive();
            }

            @Override
            public void pause() {
                busSub.pause();
            }

            @Override
            public void resume() {
                busSub.resume();
            }

            @Override
            public boolean isPaused() {
                return busSub.isPaused();
            }

            @Override
            public void close() {
                reg.closed = true;
                busSub.close();
            }
        };
    }

    /** What this inbox's component sends {@code topic} through. */
    public <T> Sender<T> sender(Topic<T> topic) {
        return new Sender<>(this, Objects.requireNonNull(topic, "topic"));
    }

    // ------------------------------------------------------------------------------------------- the coordinator

    /** Queue a message for {@code reg} at the back, or in place of the queued write to its key. */
    <T> void enqueue(TickReg<T> reg, T event, Object key, long cause, int hops) {
        if (key != null) {
            Entry queued = reg.cells.get(key);
            if (queued != null) {
                // Unlinked and relinked at the back, as Fold explains: the survivor belongs where the write that
                // survived was, after any edge that arrived between the two.
                unlink(queued);
                link(queued, event, cause, hops);
                return;
            }
        }
        Entry e = obtain();
        e.reg = reg;
        e.key = key;
        link(e, event, cause, hops);
        if (key != null) {
            reg.cells.put(key, e);
        } else {
            reg.edges++;
        }
    }

    private void link(Entry e, Object event, long cause, int hops) {
        e.event = event;
        e.cause = cause;
        e.hops = hops;
        e.prev = tail;
        e.next = null;
        if (tail == null) {
            head = e;
        } else {
            tail.next = e;
        }
        tail = e;
    }

    private void unlink(Entry e) {
        if (e.prev == null) {
            head = e.next;
        } else {
            e.prev.next = e.next;
        }
        if (e.next == null) {
            tail = e.prev;
        } else {
            e.next.prev = e.prev;
        }
        e.prev = null;
        e.next = null;
    }

    private Entry obtain() {
        Entry e = free;
        if (e == null) {
            return new Entry();
        }
        free = e.next;
        freeCount--;
        e.next = null;
        return e;
    }

    private void recycle(Entry e) {
        e.reg = null;
        e.event = null;
        e.key = null;
        e.cause = Cause.NONE;
        e.prev = null;
        e.next = null;
        if (freeCount < POOL) {
            e.next = free;
            free = e;
            freeCount++;
        }
    }

    // ------------------------------------------------------------------------------------------- the lane

    /**
     * Deliver everything queued, in order, on the calling thread. A message queued during this call (there are none:
     * sends go to the outbox) would wait for the next round. If a subscriber throws, the throw is kept for the
     * coordinator and the rest stays queued, so nothing is lost by the failure of one delivery.
     */
    void deliver() {
        if (head == null) {
            return;
        }
        Thread me = Thread.currentThread();
        long lane = Cause.current();
        long now = System.currentTimeMillis();
        delivering = me;
        try {
            Entry e;
            while ((e = head) != null) {
                head = e.next;
                if (head == null) {
                    tail = null;
                } else {
                    head.prev = null;
                }
                TickReg<?> reg = e.reg;
                Object event = e.event;
                hops = e.hops;
                long cause = e.cause;
                if (e.key != null) {
                    reg.cells.remove(e.key);
                } else {
                    reg.edges--;
                }
                recycle(e);
                if (reg.closed) {
                    continue;
                }
                reg.delivered++;
                reg.lastDeliveredMillis = now;
                Cause.enter(cause);
                if (reg.receiveRow != null) {
                    // In the message's cause, so a reader following it sees which inbox it reached, and when.
                    Probe.mark(Lane.BUS, "receive", reg.receiveRow, hops);
                }
                deliverTo(reg, event);
                deliveredThisRound++;
            }
        } catch (RuntimeException | Error t) {
            failure = t;
            if (Probe.ON) {
                Probe.mark(Lane.BUS, "tick failure", name);
            }
        } finally {
            Cause.exit(lane);
            delivering = null;
            hops = 0;
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> void deliverTo(TickReg<T> reg, Object event) {
        reg.subscriber.on((T) event);
    }

    /**
     * The detail each {@code send} row of this inbox begins with, {@code <topic> from <inbox>}, built once a topic
     * while profiling. Its lane's thread only, as {@link #post} is.
     */
    private final HashMap<Topic<?>, String> sendRows = new HashMap<>();

    /**
     * A send from inside one of this inbox's deliveries: into the outbox, for the end of the round. Profiling, it
     * writes a {@code send} row in the cause it is sent in, naming the topic and this inbox, so a reader following
     * that cause sees which component the message left; the hop count after it is the one the message will carry.
     */
    void post(Topic<?> topic, Object event) {
        if (Probe.ON) {
            String row = sendRows.get(topic);
            if (row == null) {
                row = topic.name() + " from " + name;
                sendRows.put(topic, row);
            }
            Probe.mark(Lane.BUS, "send", row, hops + 1);
        }
        int n = outSize;
        if (n == outTopics.length) {
            outTopics = Arrays.copyOf(outTopics, n * 2);
            outEvents = Arrays.copyOf(outEvents, n * 2);
            outCauses = Arrays.copyOf(outCauses, n * 2);
            outHops = Arrays.copyOf(outHops, n * 2);
        }
        outTopics[n] = topic;
        outEvents[n] = event;
        outCauses[n] = Cause.current();
        outHops[n] = hops + 1;
        outSize = n + 1;
    }

    /** Take back what was posted since the outbox held {@code mark} messages: a {@link Ledger}'s step that threw. */
    void discardFrom(int mark) {
        for (int i = mark; i < outSize; i++) {
            outTopics[i] = null;
            outEvents[i] = null;
        }
        outSize = mark;
    }

    /**
     * Work the coordinator does between rounds, when no lane is delivering: what changes several {@link Ledger}s at
     * once, all or nothing, as {@link Undo} does. Queued by a step through its {@link Out}, run after the round and
     * before the outboxes are emptied.
     */
    interface Between {
        void run(Inbox from);
    }

    Between[] between = new Between[4];
    int betweenSize;

    void queueBetween(Between work) {
        if (betweenSize == between.length) {
            between = Arrays.copyOf(between, betweenSize * 2);
        }
        between[betweenSize++] = work;
    }

    /** Take back what was queued since {@code mark}: a step that threw. */
    void discardBetweenFrom(int mark) {
        for (int i = mark; i < betweenSize; i++) {
            between[i] = null;
        }
        betweenSize = mark;
    }

    /** Run what was queued, in order, on the coordinator. @return the first throw, or null */
    Throwable runBetween() {
        Throwable first = null;
        for (int i = 0; i < betweenSize; i++) {
            try {
                between[i].run(this);
            } catch (RuntimeException | Error t) {
                if (first == null) {
                    first = t;
                }
            }
            between[i] = null;
        }
        betweenSize = 0;
        return first;
    }

    @Override
    public String toString() {
        return "Inbox[" + name + ", lane " + lane + "]";
    }
}
