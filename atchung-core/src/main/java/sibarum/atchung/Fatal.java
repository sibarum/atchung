package sibarum.atchung;

/**
 * What the process does when the bus detects a fault it cannot honestly continue past — today, a
 * {@link Backpressure#FAIL} mailbox overflowing.
 *
 * <p><b>Why a throw is not enough on its own.</b> A mailbox is filled on the <em>publisher's</em> thread,
 * whichever thread that is. If the publisher is a frame loop, throwing takes the application down and the
 * stack trace names the topic — exactly what is wanted. If the publisher is a task on a worker pool, the
 * throw is caught by the pool, recorded in a {@code Future} nobody reads, and the process carries on with a
 * queue that has silently lost data. The failure mode a fail-fast policy exists to prevent would return in a
 * new disguise: not a shed event, but a shed <em>exception</em>.
 *
 * <p>So the decision to stop is separated from the mechanism that reports it. The policy runs first and, in
 * the default, does not return; the exception is thrown afterwards for the case where it does.
 *
 * <p>Expressed as a function rather than a flag, and process-wide rather than per-bus: "should this process
 * still be running" is not a question two buses can answer differently.
 */
@FunctionalInterface
public interface Fatal {

    /**
     * Called on the thread that detected the fault, before {@code cause} is thrown. An implementation that
     * intends to stop the process must not return.
     */
    void fault(Throwable cause);

    /**
     * Stop the process now: report {@code cause} on stderr and {@link Runtime#halt} with 70
     * ({@code EX_SOFTWARE}). The default, and the right default — a bus that has begun losing events is
     * producing results that cannot be trusted, and every second it runs on is a second of output somebody
     * may act on.
     *
     * <p>{@code halt} rather than {@code exit}: shutdown hooks are application code, and running application
     * code on a thread that is mid-publish, holding a mailbox lock, with a full queue behind it, is how a
     * crash becomes a hang. There is nothing left to tidy that is worth that risk.
     */
    Fatal HALT = cause -> {
        System.err.println("[atchung] fatal: " + cause.getMessage());
        cause.printStackTrace(System.err);
        System.err.flush();
        Runtime.getRuntime().halt(70);
    };

    /**
     * Do nothing, leaving the exception to propagate to the publisher — for tests, which must be able to
     * assert that an overflow is detected without the assertion killing the JVM that is making it.
     *
     * <p>Not for production. A process that installs this is choosing to let a caught exception turn a
     * detected fault back into a silent one.
     */
    Fatal THROW = cause -> { };
}
