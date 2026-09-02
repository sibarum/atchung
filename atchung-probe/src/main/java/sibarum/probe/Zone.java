package sibarum.probe;

/**
 * An open span, closed by leaving the block that opened it.
 *
 * <pre>{@code
 * try (Zone z = Probe.zone(Lane.FRAME, "frame")) {
 *     ...
 * }
 * }</pre>
 *
 * <p><b>Never allocated per span.</b> One instance exists per (thread, nesting depth) and is handed out
 * again on the next span at that depth, because a profiler that allocates on the path it is measuring
 * changes the allocation rate it was installed to explain. The pool grows to whatever depth a thread has
 * actually reached and then stops.
 *
 * <p>A zone is valid only on the thread that opened it, and only until it is closed. Closing twice, or on
 * another thread, is ignored rather than diagnosed — the alternative is a profiler that throws out of a
 * {@code finally}, and a profiling build that crashes where the real build does not is worthless.
 */
public final class Zone implements AutoCloseable {

    /**
     * One thread's span stack.
     *
     * <p>The child accumulator is what makes self time work: when a span closes it adds its whole elapsed
     * time to its parent's child total, and reports as its own self time whatever its children did not
     * account for. A stack rather than a field because the frame loop nests three deep on a good day.
     */
    static final class Stack {
        Zone[] zones = new Zone[16];
        long[] childNanos = new long[16];
        int depth;

        Zone push(Tally tally, long start) {
            if (depth == zones.length) {
                zones = java.util.Arrays.copyOf(zones, depth * 2);
                childNanos = java.util.Arrays.copyOf(childNanos, depth * 2);
            }
            Zone z = zones[depth];
            if (z == null) {
                z = zones[depth] = new Zone(this, depth);
            }
            childNanos[depth] = 0L;
            z.tally = tally;
            z.start = start;
            z.open = true;
            depth++;
            return z;
        }
    }

    /**
     * The stack is per thread and never removed.
     *
     * <p>Not a leak worth avoiding: the pool is bounded by nesting depth, and this only exists at all in a
     * process that was started with profiling on. Clearing it on thread death would need a reference queue,
     * which is machinery that would then be running in every profiling session to reclaim a few hundred bytes.
     */
    static final ThreadLocal<Stack> STACKS = ThreadLocal.withInitial(Stack::new);

    private final Stack owner;
    private final int slot;
    private Tally tally;
    private long start;
    private boolean open;

    private Zone(Stack owner, int slot) {
        this.owner = owner;
        this.slot = slot;
    }

    /**
     * A zone that records nothing: what {@link Probe#zone} returns when the lane is off, so a call site can
     * be written as an unconditional try-with-resources without a branch of its own.
     */
    static final Zone NONE = new Zone(null, -1);

    @Override
    public void close() {
        if (!open || owner == null || owner.depth != slot + 1) {
            return;
        }
        long elapsed = System.nanoTime() - start;
        open = false;
        owner.depth = slot;
        tally.record(elapsed);
        tally.recordSelf(elapsed - owner.childNanos[slot]);
        if (slot > 0) {
            owner.childNanos[slot - 1] += elapsed;
        }
        Probe.slow(tally, elapsed);
        tally = null;
    }
}
