package sibarum.atchung;

/**
 * Reacts to events published on a {@link Topic}. The thread an event arrives on depends on how the
 * subscriber was registered: the publisher's thread (inline), an executor thread (async), or the
 * thread that calls {@link Pump#drain()} (pumped).
 *
 * @param <T> the event type
 */
@FunctionalInterface
public interface Subscriber<T> {

    void on(T event);
}
