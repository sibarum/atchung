package sibarum.elektro.queue;

import sibarum.elektro.queue.message.MessageType;
import sibarum.elektro.queue.message.PeerId;

/**
 * The context handed to an {@link Actor} for a single reacted message.
 *
 * <p>It exposes who sent the message ({@link #source()}), how it was correlated
 * ({@link #correlationId()}, {@link #isRequest()}), and its type, plus the owning
 * {@link #conduit()}. When the message is a request, {@link #reply} sends a correlated
 * response back to the originating peer without the actor having to build an
 * {@link Action} itself.
 *
 * <p>A context is valid only for the duration of the {@code react} call and must not
 * be retained.
 */
public interface MessageContext {

    /** The peer the message was received from. */
    PeerId source();

    /** Correlation id linking a request to its reply; {@code 0} when uncorrelated. */
    long correlationId();

    /** {@code true} if the sender expects a {@link #reply}. */
    boolean isRequest();

    /** Type of the message being reacted to. */
    MessageType<?> type();

    /** The conduit that delivered this message. */
    Conduit conduit();

    /**
     * Sends {@code reply} back to {@link #source()}, correlated to this message.
     *
     * @throws ElektroException if this message was not a request
     */
    <R> void reply(MessageType<R> replyType, R reply);
}
