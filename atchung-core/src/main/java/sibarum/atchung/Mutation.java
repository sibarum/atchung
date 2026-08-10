package sibarum.atchung;

/**
 * A pure state transition: given the current value and a payload, return the next value. Must not
 * mutate {@code current} (state values are immutable) and must be side-effect-free — a mutation may
 * be retried under contention and, in future, replayed for replication.
 *
 * @param <T> the state type
 * @param <P> the payload type
 */
@FunctionalInterface
public interface Mutation<T, P> {

    T apply(T current, P payload);
}
