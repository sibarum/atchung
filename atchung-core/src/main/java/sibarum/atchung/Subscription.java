package sibarum.atchung;

/**
 * A live registration. Closing it stops delivery and releases the registration (idempotent).
 *
 * <p>A subscription can also be <b>paused</b> and <b>resumed</b> — the primitive for "stop
 * receiving" without unsubscribing. Pausing is <b>lossy</b>: while paused, matching events are not
 * delivered and are <em>not</em> buffered for later — a paused push subscriber is skipped in the
 * fan-out, a paused pull mailbox is simply not filled. Nothing about pausing implies any semantics
 * (focus, priority, …); those live in application code that decides when to pause.
 */
public interface Subscription extends AutoCloseable {

    /** @return whether this subscription is still registered (has not been closed). */
    boolean isActive();

    /** Stop delivering to this subscriber. Events during the pause are lost, not buffered. */
    void pause();

    /** Resume delivery. Only events published after resuming are delivered. */
    void resume();

    /** @return whether delivery is currently paused. */
    boolean isPaused();

    @Override
    void close();
}
