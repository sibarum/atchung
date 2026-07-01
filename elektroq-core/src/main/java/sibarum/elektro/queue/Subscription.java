package sibarum.elektro.queue;

import sibarum.elektro.queue.message.MessageType;

/**
 * A handle to an {@link Actor} registration, returned by
 * {@link Conduit#subscribe(MessageType, Actor)}.
 *
 * <p>Closing the subscription detaches the actor so it reacts to no further messages.
 * {@link #close()} is idempotent and does not throw, so a subscription is convenient to
 * use in try-with-resources.
 */
public interface Subscription extends AutoCloseable {

    /** The message type this subscription reacts to. */
    MessageType<?> type();

    /** {@code true} until {@link #close()} is called. */
    boolean isActive();

    /** Detaches the actor. Idempotent; never throws. */
    @Override
    void close();
}
