package sibarum.atchung;

/**
 * Reacts to each new committed version of a {@link State}. Invoked on the committing (producer)
 * thread — keep it fast, or hand off. Registered via {@link State#onCommit(StateListener)}, and
 * pausable/lossy like any subscription: while paused it is not notified, and on resume the consumer
 * reads {@link State#current()} to become coherent again (no missed-transition staleness).
 *
 * @param <T> the state type
 */
@FunctionalInterface
public interface StateListener<T> {

    void onCommit(Versioned<T> snapshot);
}
