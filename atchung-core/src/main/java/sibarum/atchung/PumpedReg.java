package sibarum.atchung;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import sibarum.probe.Lane;
import sibarum.probe.Probe;

/**
 * A pumped subscriber's bounded mailbox. {@link #deliver(Object)} runs on publisher threads and only
 * enqueues (applying the {@link Backpressure} policy); {@link #drain()} runs on the pump's owner
 * thread and invokes the subscriber for each queued event. This is the thread hand-off that gives
 * pumped subscribers their affinity: events are produced anywhere, consumed where {@code drain()} is called.
 *
 * <p>With a {@link Fold} the mailbox holds <em>cells</em> rather than events: a write supersedes the queued
 * write to the same cell instead of taking a slot beside it, so the queue grows with how much changed and not
 * with how fast the producer ran. The two backings are kept apart deliberately — a channel with no fold uses
 * the same {@link ArrayDeque} it always did, and pays nothing for a feature it is not using.
 */
final class PumpedReg<T> extends Atchung.Reg<T> {

    private final Subscriber<T> subscriber;
    private final int capacity;
    private final Backpressure backpressure;
    private final Fold<T> fold;

    /**
     * Told when something has been queued, so that a thread parked in {@link Pump#drain(long)} can be woken.
     *
     * <p><b>Called after this mailbox's lock has been released, and that is not a tidiness preference.</b> The
     * parked thread holds the pump's monitor and reaches for this mailbox's lock to ask whether anything is
     * waiting. A publisher that signalled while still holding this lock would be taking the two in the
     * opposite order, which is the definition of a lock-ordering deadlock.
     */
    private final Runnable arrival;

    /** The queue, when nothing folds. Null on a folding mailbox. */
    private final ArrayDeque<T> queue;
    /**
     * The queue, when something folds: cell → the latest write to it, in the arrival order of the writes that
     * survived. Null on an ordinary mailbox. A {@link LinkedHashMap} because superseding is remove-then-put,
     * which is O(1) and lands the survivor at the back — see {@link Fold} for why the back is the right place.
     */
    private final LinkedHashMap<Object, T> folded;

    private final Object lock = new Object();
    /** How many events have been handed to the subscriber, for {@link MailboxOverflow}. Guarded by the lock. */
    private long delivered;
    /** When this mailbox was made, for an overflow that has never seen a drain. */
    private final long subscribedMillis = System.currentTimeMillis();
    /**
     * When the consumer last took a batch, or 0 if it never has. Written once per non-empty drain, not per event
     * and not on an empty poll, so it costs nothing on the path a frame loop runs every frame. Guarded by the lock.
     */
    private long lastDrainMillis;

    /** The key of an event that folds with nothing: unique by identity, so every edge keeps its own slot. */
    private static final class Edge {
    }

    /**
     * The probe's names for this mailbox, built once at subscription time and null when not profiling.
     *
     * <p>A mailbox is exactly where a "driver overload" complaint turns into a number: the peak depth says
     * how far behind the consumer fell, the drop count says whether that cost anything, and the batch size
     * says how lumpy the drain is. All three are per subscription, so the label is per subscription too —
     * composing it on the enqueue path, which runs on every publish, would be the profiler paying for itself
     * out of the budget it is measuring.
     */
    private final String depthName;
    private final String dropName;
    private final String batchName;

    PumpedReg(Topic<T> topic, Subscriber<T> subscriber, int capacity, Backpressure backpressure, Fold<T> fold,
              Runnable arrival) {
        super(topic);
        this.arrival = arrival;
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be >= 1, was " + capacity);
        }
        this.subscriber = subscriber;
        this.backpressure = backpressure;
        this.fold = fold;
        this.capacity = backpressure == Backpressure.COALESCE_LATEST ? 1 : capacity;
        int initial = Math.min(this.capacity, 64);
        this.queue = fold == null ? new ArrayDeque<>(initial) : null;
        this.folded = fold == null ? null : new LinkedHashMap<>(initial);
        this.depthName = Probe.ON ? topic.name() + " depth" : null;
        this.dropName = Probe.ON ? topic.name() + " dropped" : null;
        this.batchName = Probe.ON ? topic.name() + " batch" : null;
    }

    @Override
    void doDeliver(T event) {
        boolean queued = false;
        synchronized (lock) {
            Object cell = fold == null ? null : fold.cell(event);
            // A write to a cell that is already queued takes no new slot, so it can never overflow, block, or be
            // dropped for want of room. That is the whole point: a producer writing one cell a million times
            // between drains is not a producer that is outrunning anything.
            boolean supersedes = cell != null && folded.containsKey(cell);
            switch (backpressure) {
                case FAIL -> {
                    if (!supersedes && queued() >= capacity) {
                        // Outside the lock would be tidier and is wrong: the Fatal policy does not return in
                        // the default, so releasing the lock first would leave a publisher that is about to
                        // halt racing a drain that is about to succeed, and the report would name a mailbox
                        // that had just been emptied. The state described has to be the state observed.
                        MailboxOverflow overflow = new MailboxOverflow(topic(), capacity, delivered,
                                subscribedMillis, lastDrainMillis);
                        Atchung.fatal().fault(overflow);
                        throw overflow;
                    }
                    enqueue(event, cell);
                    queued = true;
                }
                case DROP_OLDEST -> {
                    while (!supersedes && queued() >= capacity) {
                        evictOldest();
                        // A lossy policy is a decision, and this is the line that tells you it was taken. The
                        // policy is not the bug; a policy silently firing ten thousand times is.
                        Probe.count(Lane.BUS, dropName);
                    }
                    enqueue(event, cell);
                    queued = true;
                }
                case DROP_NEWEST -> {
                    if (supersedes || queued() < capacity) {
                        enqueue(event, cell);
                        queued = true;
                    } else {
                        Probe.count(Lane.BUS, dropName);
                    }
                }
                case COALESCE_LATEST -> {
                    if (Probe.ON && queued() > 0) {
                        Probe.count(Lane.BUS, dropName, queued());
                    }
                    clearQueue();
                    enqueue(event, cell);
                    queued = true;
                }
                case BLOCK -> {
                    boolean interrupted = false;
                    while (!supersedes && queued() >= capacity) {
                        try {
                            lock.wait();
                        } catch (InterruptedException e) {
                            interrupted = true;
                            break;
                        }
                    }
                    enqueue(event, cell);
                    queued = true;
                    if (interrupted) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
            // The peak of this counter is the number that matters: a mailbox whose depth averaged 2 and once
            // reached its capacity is a mailbox that was one publish away from dropping, or did.
            Probe.count(Lane.BUS, depthName, queued());
        }
        // Outside the lock, deliberately: see the field. Nothing is signalled for an event that was dropped
        // rather than queued — a wake for a mailbox that is still empty is a thread woken to find out it had
        // no reason to be.
        if (queued) {
            arrival.run();
        }
    }

    /** Queue {@code event}, superseding the queued write to {@code cell} if there is one. Caller holds the lock. */
    private void enqueue(T event, Object cell) {
        if (fold == null) {
            queue.addLast(event);
            return;
        }
        Object key = cell == null ? new Edge() : cell;
        // Remove before put: a re-put alone would keep the original insertion position, and the survivor belongs
        // at the position of the write that survived — otherwise it could sit in front of an edge that arrived
        // between the two writes and that the unfolded sequence would have ordered it after.
        folded.remove(key);
        folded.put(key, event);
    }

    /** How many slots are in use. Caller holds the lock. */
    private int queued() {
        return fold == null ? queue.size() : folded.size();
    }

    /** Discard the oldest slot. Caller holds the lock. */
    private void evictOldest() {
        if (fold == null) {
            queue.pollFirst();
            return;
        }
        Iterator<Object> it = folded.keySet().iterator();
        if (it.hasNext()) {
            it.next();
            it.remove();
        }
    }

    /** Caller holds the lock. */
    private void clearQueue() {
        if (fold == null) {
            queue.clear();
        } else {
            folded.clear();
        }
    }

    /**
     * Deliver every currently-queued event to the subscriber, on the calling thread. Events that
     * arrive after the snapshot is taken wait for the next drain. Handlers run outside the lock, so
     * publishers are never blocked by a running handler.
     *
     * @return the number of events delivered
     */
    int drain() {
        List<T> batch;
        synchronized (lock) {
            if (queued() == 0) {
                return 0;
            }
            batch = fold == null ? new ArrayList<>(queue) : new ArrayList<>(folded.values());
            clearQueue();
            delivered += batch.size();
            lastDrainMillis = System.currentTimeMillis();
            lock.notifyAll(); // wake any BLOCK publisher waiting on space
        }
        Probe.count(Lane.BUS, batchName, batch.size());
        for (T event : batch) {
            subscriber.on(event);
        }
        return batch.size();
    }

    /** @return whether any event is currently queued (non-draining). */
    boolean hasQueued() {
        synchronized (lock) {
            return queued() > 0;
        }
    }
}
