package sibarum.atchung;

import java.util.ArrayDeque;
import java.util.HashMap;

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
 *
 * <p><b>A drain allocates nothing in steady state.</b> The GUI drains every pump every frame, so a drain that
 * copied its queue would be garbage made per frame per mailbox for the privilege of handing events over. Instead
 * the queue is <em>double-buffered</em>: a drain swaps the full queue for an empty one under the lock and walks
 * the full one outside it, then hands it back as the next spare. A folding mailbox does the same with its slot
 * chain and recycles its slots, so an edge costs no allocation either.
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

    /** The queue, when nothing folds. Null on a folding mailbox. Guarded by the lock; swapped by each drain. */
    private ArrayDeque<T> queue;
    /**
     * The emptied queue of the last drain, waiting to become the next {@link #queue}; null while a drain is walking
     * it. A drain that finds it null — one run from inside a handler of another, or on a second thread — makes a
     * fresh one rather than share a buffer somebody is still reading. Guarded by the lock.
     */
    private ArrayDeque<T> spare;

    /**
     * One queued write on a folding mailbox: a link in the arrival-ordered chain {@link #head} … {@link #tail}.
     * {@code cell} is null for an edge. Slots are recycled through {@link #free}, so neither an edge nor a
     * supersede allocates.
     */
    private static final class Slot<T> {
        Object cell;
        T event;
        Slot<T> prev;
        Slot<T> next;
    }

    /**
     * The queue, when something folds: the chain of surviving writes in arrival order, and {@link #cells} indexing
     * the ones that name a cell. Superseding unlinks the cell's slot and re-links it at the back, which is O(1) —
     * see {@link Fold} for why the back is the right place. Null on an ordinary mailbox. Guarded by the lock.
     */
    private final HashMap<Object, Slot<T>> cells;
    private Slot<T> head;
    private Slot<T> tail;
    /** How many slots are linked into the chain. Guarded by the lock. */
    private int slots;
    /** Recycled slots, singly linked through {@code next}; at most {@link #poolCap}. Guarded by the lock. */
    private Slot<T> free;
    private int freeCount;
    /** How many recycled slots are worth keeping: enough for a busy frame, not a burst's worth forever. */
    private final int poolCap;

    /**
     * How many {@link Backpressure#BLOCK} publishers are parked on the lock waiting for room. A drain wakes them
     * only when this is non-zero: no other policy ever waits, and a frame loop draining a mailbox nobody is
     * waiting on should not pay for a monitor notification. Guarded by the lock.
     */
    private int waiters;

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

    /**
     * This mailbox's name, as the {@code enqueue} and {@code deliver} rows begin, and null when it has none or when
     * not profiling — so the one null test on the hot path stands for both. The rows say which mailbox a message
     * reached, which the topic rows above cannot: two mailboxes on one topic count into the same names.
     */
    private final String rowName;
    /** The whole detail of this mailbox's {@code drop} row, {@code <mailbox> <policy>}; null exactly when {@link #rowName} is. */
    private final String dropDetail;

    PumpedReg(String mailbox, Topic<T> topic, Subscriber<T> subscriber, int capacity, Backpressure backpressure,
              Fold<T> fold, Runnable arrival) {
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
        this.spare = fold == null ? new ArrayDeque<>(initial) : null;
        this.cells = fold == null ? null : new HashMap<>();
        this.poolCap = Math.min(this.capacity, 1024);
        this.depthName = Probe.ON ? topic.name() + " depth" : null;
        this.dropName = Probe.ON ? topic.name() + " dropped" : null;
        this.batchName = Probe.ON ? topic.name() + " batch" : null;
        this.rowName = Probe.ON ? mailbox : null;
        this.dropDetail = rowName == null ? null : rowName + " " + backpressure.name();
    }

    @Override
    void doDeliver(T event) {
        boolean queued = false;
        synchronized (lock) {
            Object cell = fold == null ? null : fold.cell(event);
            // A write to a cell that is already queued takes no new slot, so it can never overflow, block, or be
            // dropped for want of room. That is the whole point: a producer writing one cell a million times
            // between drains is not a producer that is outrunning anything.
            boolean supersedes = cell != null && cells.containsKey(cell);
            switch (backpressure) {
                case FAIL -> {
                    if (!supersedes && queued() >= capacity) {
                        // Outside the lock would be tidier and is wrong: the Fatal policy does not return in
                        // the default, so releasing the lock first would leave a publisher that is about to
                        // halt racing a drain that is about to succeed, and the report would name a mailbox
                        // that had just been emptied. The state described has to be the state observed.
                        // Before the fault, which in the default does not return: the refusal is the last row
                        // this mailbox writes, and a reader of the file sees which pin it happened at.
                        if (rowName != null) {
                            Probe.mark(Lane.BUS, "drop", dropDetail);
                        }
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
                        if (rowName != null) {
                            Probe.mark(Lane.BUS, "drop", dropDetail);
                        }
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
                        if (rowName != null) {
                            Probe.mark(Lane.BUS, "drop", dropDetail);
                        }
                    }
                }
                case COALESCE_LATEST -> {
                    if (Probe.ON && queued() > 0) {
                        Probe.count(Lane.BUS, dropName, queued());
                        // One row per event the coalesce discards, as the counter above counts them.
                        for (int i = queued(); rowName != null && i > 0; i--) {
                            Probe.mark(Lane.BUS, "drop", dropDetail);
                        }
                    }
                    clearQueue();
                    enqueue(event, cell);
                    queued = true;
                }
                case BLOCK -> {
                    boolean interrupted = false;
                    while (!supersedes && queued() >= capacity) {
                        waiters++;
                        try {
                            lock.wait();
                        } catch (InterruptedException e) {
                            interrupted = true;
                            break;
                        } finally {
                            waiters--;
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
            // Which mailbox the message reached, on the publisher's thread, which says who sent it. Inside the
            // lock with the depth row, so the depth it carries is the depth this publish left.
            if (queued && rowName != null) {
                Probe.mark(Lane.BUS, "enqueue", rowName, queued());
            }
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
        // Looked up again rather than taken from the caller: a BLOCK publisher may have waited since it asked, and
        // another publisher may have queued this cell in the meantime.
        Slot<T> slot = cell == null ? null : cells.get(cell);
        if (slot != null) {
            // Unlink and re-link at the back: the survivor belongs at the position of the write that survived —
            // otherwise it could sit in front of an edge that arrived between the two writes and that the unfolded
            // sequence would have ordered it after.
            unlink(slot);
        } else {
            slot = obtain();
            slot.cell = cell;
            if (cell != null) {
                cells.put(cell, slot);
            }
            slots++;
        }
        slot.event = event;
        slot.prev = tail;
        slot.next = null;
        if (tail == null) {
            head = slot;
        } else {
            tail.next = slot;
        }
        tail = slot;
    }

    /** Take {@code slot} out of the chain without changing the count. Caller holds the lock. */
    private void unlink(Slot<T> slot) {
        if (slot.prev == null) {
            head = slot.next;
        } else {
            slot.prev.next = slot.next;
        }
        if (slot.next == null) {
            tail = slot.prev;
        } else {
            slot.next.prev = slot.prev;
        }
        slot.prev = null;
        slot.next = null;
    }

    /** A blank slot, recycled if one is free. Caller holds the lock. */
    private Slot<T> obtain() {
        Slot<T> slot = free;
        if (slot == null) {
            return new Slot<>();
        }
        free = slot.next;
        freeCount--;
        slot.next = null;
        return slot;
    }

    /** Return one slot, already unlinked from the chain, to the pool. Caller holds the lock. */
    private void recycle(Slot<T> slot) {
        slot.cell = null;
        slot.event = null;
        slot.prev = null;
        slot.next = null;
        if (freeCount < poolCap) {
            slot.next = free;
            free = slot;
            freeCount++;
        }
    }

    /** How many slots are in use. Caller holds the lock. */
    private int queued() {
        return fold == null ? queue.size() : slots;
    }

    /** Discard the oldest slot. Caller holds the lock. */
    private void evictOldest() {
        if (fold == null) {
            queue.pollFirst();
            return;
        }
        Slot<T> oldest = head;
        if (oldest == null) {
            return;
        }
        unlink(oldest);
        if (oldest.cell != null) {
            cells.remove(oldest.cell);
        }
        slots--;
        recycle(oldest);
    }

    /** Caller holds the lock. */
    private void clearQueue() {
        if (fold == null) {
            queue.clear();
            return;
        }
        Slot<T> slot = head;
        head = null;
        tail = null;
        slots = 0;
        cells.clear();
        while (slot != null) {
            Slot<T> next = slot.next;
            recycle(slot);
            slot = next;
        }
    }

    /**
     * Deliver every currently-queued event to the subscriber, on the calling thread. Events that
     * arrive after the batch is taken wait for the next drain. Handlers run outside the lock, so
     * publishers are never blocked by a running handler.
     *
     * <p>The batch is taken by swapping buffers, not by copying them: under the lock the full queue (or, folding,
     * the whole slot chain) is detached and an empty one put in its place, and the detached batch then belongs to
     * this call alone. A handler that publishes queues into the fresh buffer; a handler that drains again takes
     * that fresh buffer as its own batch and, finding no spare, makes one. If a handler throws, the rest of its
     * batch is discarded, as it always was, and the buffer is still recycled.
     *
     * @return the number of events delivered
     */
    int drain() {
        ArrayDeque<T> batch = null;
        Slot<T> first = null;
        int count;
        synchronized (lock) {
            count = queued();
            if (count == 0) {
                return 0;
            }
            if (fold == null) {
                batch = queue;
                ArrayDeque<T> next = spare;
                queue = next != null ? next : new ArrayDeque<>(Math.min(capacity, 64));
                spare = null;
            } else {
                first = head;
                head = null;
                tail = null;
                slots = 0;
                cells.clear();
            }
            delivered += count;
            lastDrainMillis = System.currentTimeMillis();
            if (waiters > 0) {
                lock.notifyAll(); // wake the BLOCK publishers waiting on space; nobody else ever waits here
            }
        }
        Probe.count(Lane.BUS, batchName, count);
        if (rowName != null) {
            Probe.mark(Lane.BUS, "deliver", rowName, count);
        }
        if (fold == null) {
            deliverQueue(batch);
        } else {
            deliverChain(first);
        }
        return count;
    }

    /** Walk a detached queue, then offer it back as the next spare. Runs outside the lock. */
    private void deliverQueue(ArrayDeque<T> batch) {
        try {
            // pollFirst rather than an iterator: an iterator is an allocation per drain, and an ArrayDeque holds
            // no nulls, so null is the end.
            T event;
            while ((event = batch.pollFirst()) != null) {
                subscriber.on(event);
            }
        } finally {
            batch.clear();
            synchronized (lock) {
                if (spare == null) {
                    spare = batch;
                }
            }
        }
    }

    /**
     * Walk a detached slot chain, then return its slots to the pool. Runs outside the lock; the chain is blanked
     * and cut to the pool's size here too, so that the lock is held only for the O(1) splice.
     */
    private void deliverChain(Slot<T> first) {
        try {
            for (Slot<T> slot = first; slot != null; slot = slot.next) {
                subscriber.on(slot.event);
            }
        } finally {
            Slot<T> last = null;
            int kept = 0;
            for (Slot<T> slot = first; slot != null; ) {
                Slot<T> next = slot.next;
                slot.cell = null;
                slot.event = null;
                slot.prev = null;
                if (kept < poolCap) {
                    last = slot;
                    kept++;
                } else {
                    last.next = null;
                    break;
                }
                slot = next;
            }
            if (last != null) {
                synchronized (lock) {
                    if (freeCount < poolCap) {
                        last.next = free;
                        free = first;
                        freeCount += kept;
                    }
                }
            }
        }
    }

    /** @return whether any event is currently queued (non-draining). */
    boolean hasQueued() {
        synchronized (lock) {
            return queued() > 0;
        }
    }
}
