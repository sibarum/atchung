package sibarum.atchung;

import sibarum.probe.Cause;

/**
 * The causes of an ordinary mailbox's queued events, kept in step with its {@code ArrayDeque}: every add, eviction,
 * clear and swap of the queue does the same here. A ring of {@code long}s, so storing a cause allocates nothing; it
 * grows as the queue does and is swapped and reused with it, so a drain still allocates nothing in steady state.
 * Not thread-safe: guarded by the mailbox's lock, or owned by the drain that detached it.
 */
final class CauseRing {

    private long[] items;
    private int head;
    private int size;

    CauseRing(int initial) {
        items = new long[Math.max(2, initial)];
    }

    void addLast(long cause) {
        if (size == items.length) {
            long[] grown = new long[items.length * 2];
            for (int i = 0; i < size; i++) {
                grown[i] = items[(head + i) % items.length];
            }
            items = grown;
            head = 0;
        }
        items[(head + size) % items.length] = cause;
        size++;
    }

    /** The oldest cause, removed; {@link Cause#NONE} when there is none, as for an event queued with no cause. */
    long pollFirst() {
        if (size == 0) {
            return Cause.NONE;
        }
        long cause = items[head];
        head = (head + 1) % items.length;
        size--;
        return cause;
    }

    void clear() {
        head = 0;
        size = 0;
    }

    int size() {
        return size;
    }
}
