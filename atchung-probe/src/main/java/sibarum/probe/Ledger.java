package sibarum.probe;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Who is still holding what: the open/close ledger behind the leak half of this facility.
 *
 * <p>A leak in this stack is rarely a leak of Java objects — the collector handles those. It is a swapchain
 * that outlived its window, a descriptor set per frame, a subscription that nothing closed, a render target
 * remade on every resize. All of those are {@code AutoCloseable} things whose {@code close()} is simply never
 * reached, and all of them are invisible to a heap dump because the native handle they own is a {@code long}.
 * So the ledger does not guess: a resource is registered when it is created and struck off when it is closed,
 * and whatever is left at the end is named, counted, and dated.
 *
 * <p>Entries are keyed by <b>identity</b>, not equality. Two render targets of the same size are two
 * resources, and a record type that has value semantics must still be two rows if it was allocated twice.
 *
 * <p><b>The ledger holds a strong reference to everything registered.</b> That is a real cost and a
 * deliberate one — a weak reference would let the very object under investigation vanish before it could be
 * named, and "something leaked but it has been collected" is not a report. It is also why the ledger only
 * exists when profiling is on.
 */
final class Ledger {

    /** One live resource. {@code stack} is null unless {@code probe.stacks} asked for it. */
    record Entry(String kind, Lane lane, long seq, long nanos, String thread, StackTraceElement[] stack) {
    }

    /** Identity key: two distinct allocations are two rows even if the objects compare equal. */
    private record Id(Object ref) {
        @Override
        public boolean equals(Object o) {
            return o instanceof Id other && other.ref == ref;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(ref);
        }
    }

    private final ConcurrentHashMap<Id, Entry> live = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> opened = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> closed = new ConcurrentHashMap<>();
    private final AtomicLong seq = new AtomicLong();
    private final boolean stacks;
    private final long origin = System.nanoTime();

    Ledger(boolean stacks) {
        this.stacks = stacks;
    }

    void opened(Lane lane, String kind, Object what) {
        if (what == null) {
            return;
        }
        opened.computeIfAbsent(kind, k -> new LongAdder()).increment();
        live.put(new Id(what), new Entry(kind, lane, seq.incrementAndGet(), System.nanoTime() - origin,
                Thread.currentThread().getName(), stacks ? trimmed() : null));
    }

    void closed(String kind, Object what) {
        if (what == null) {
            return;
        }
        closed.computeIfAbsent(kind, k -> new LongAdder()).increment();
        live.remove(new Id(what));
    }

    /**
     * The call site that created the resource, with this class's own frames dropped.
     *
     * <p>Capped at 12 frames. The allocation site is always within a few frames of the top, and an uncapped
     * capture on a per-frame resource is the profiler becoming the leak.
     */
    private static StackTraceElement[] trimmed() {
        StackTraceElement[] all = Thread.currentThread().getStackTrace();
        int from = 0;
        while (from < all.length && (all[from].getClassName().startsWith("java.lang.Thread")
                || all[from].getClassName().startsWith("sibarum.probe."))) {
            from++;
        }
        int to = Math.min(all.length, from + 12);
        StackTraceElement[] out = new StackTraceElement[Math.max(0, to - from)];
        System.arraycopy(all, from, out, 0, out.length);
        return out;
    }

    /** Kinds that have been registered at all, so a kind with nothing outstanding still reports 0 live. */
    List<String> kinds() {
        List<String> out = new ArrayList<>(opened.keySet());
        java.util.Collections.sort(out);
        return out;
    }

    long opened(String kind) {
        LongAdder a = opened.get(kind);
        return a == null ? 0 : a.sum();
    }

    long closed(String kind) {
        LongAdder a = closed.get(kind);
        return a == null ? 0 : a.sum();
    }

    /** Outstanding entries, oldest first — the order in which a leak wants to be read. */
    List<Entry> oldest(int limit) {
        List<Entry> out = new ArrayList<>(live.values());
        out.sort(Comparator.comparingLong(Entry::seq));
        return out.size() <= limit ? out : out.subList(0, limit);
    }

    int liveCount() {
        return live.size();
    }
}
