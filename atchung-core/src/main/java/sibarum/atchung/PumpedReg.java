package sibarum.atchung;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/**
 * A pumped subscriber's bounded mailbox. {@link #deliver(Object)} runs on publisher threads and only
 * enqueues (applying the {@link Backpressure} policy); {@link #drain()} runs on the pump's owner
 * thread and invokes the subscriber for each queued event. This is the thread hand-off that gives
 * pumped subscribers their affinity: events are produced anywhere, consumed where {@code drain()} is called.
 */
final class PumpedReg<T> extends Atchung.Reg<T> {

    private final Subscriber<T> subscriber;
    private final int capacity;
    private final Backpressure backpressure;

    private final ArrayDeque<T> queue;
    private final Object lock = new Object();

    PumpedReg(Topic<T> topic, Subscriber<T> subscriber, int capacity, Backpressure backpressure) {
        super(topic);
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be >= 1, was " + capacity);
        }
        this.subscriber = subscriber;
        this.backpressure = backpressure;
        this.capacity = backpressure == Backpressure.COALESCE_LATEST ? 1 : capacity;
        this.queue = new ArrayDeque<>(Math.min(this.capacity, 64));
    }

    @Override
    void doDeliver(T event) {
        synchronized (lock) {
            switch (backpressure) {
                case DROP_OLDEST -> {
                    while (queue.size() >= capacity) {
                        queue.pollFirst();
                    }
                    queue.addLast(event);
                }
                case DROP_NEWEST -> {
                    if (queue.size() < capacity) {
                        queue.addLast(event);
                    }
                }
                case COALESCE_LATEST -> {
                    queue.clear();
                    queue.addLast(event);
                }
                case BLOCK -> {
                    boolean interrupted = false;
                    while (queue.size() >= capacity) {
                        try {
                            lock.wait();
                        } catch (InterruptedException e) {
                            interrupted = true;
                            break;
                        }
                    }
                    queue.addLast(event);
                    if (interrupted) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
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
            if (queue.isEmpty()) {
                return 0;
            }
            batch = new ArrayList<>(queue);
            queue.clear();
            lock.notifyAll(); // wake any BLOCK publisher waiting on space
        }
        for (T event : batch) {
            subscriber.on(event);
        }
        return batch.size();
    }

    /** @return whether any event is currently queued (non-draining). */
    boolean hasQueued() {
        synchronized (lock) {
            return !queue.isEmpty();
        }
    }
}
