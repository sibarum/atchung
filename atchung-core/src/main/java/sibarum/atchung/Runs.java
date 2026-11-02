package sibarum.atchung;

/**
 * The chain run a thread is working in, carried with every message as the probe's cause is: what lets a reply find
 * the run whose request it answers.
 *
 * <p>A step running in a run works in that run's id, so every message it sends carries it. A delivery works in the id
 * its message carried, so whatever a component sends while handling it carries the same id on, including a request
 * handed to an async subscriber (a job), whose reply then carries it back. A message from outside any run carries
 * {@link #NONE}. vexelray-framework's {@code docs/plans/chains.md}, ruling 9, is why: a wait takes a message that
 * belongs to its run, or one that belongs to none, and never one that belongs to another run.
 *
 * <p>Thread-confined, like {@code Cause}: a slot per thread, read and written without allocating.
 */
final class Runs {

    /** No run. */
    static final long NONE = 0L;

    private static final ThreadLocal<long[]> CURRENT = ThreadLocal.withInitial(() -> new long[1]);

    private Runs() {
    }

    /** The run this thread is working in, or {@link #NONE}. */
    static long current() {
        return CURRENT.get()[0];
    }

    /** This thread's slot, for a runtime that sets it once per message and would rather not look it up each time. */
    static long[] slot() {
        return CURRENT.get();
    }

    /** Enter {@code run} and return the one this thread was in; always paired with {@link #exit} in a finally. */
    static long enter(long run) {
        long[] slot = CURRENT.get();
        long previous = slot[0];
        slot[0] = run;
        return previous;
    }

    /** Put back the run {@link #enter} returned. */
    static void exit(long previous) {
        CURRENT.get()[0] = previous;
    }
}
