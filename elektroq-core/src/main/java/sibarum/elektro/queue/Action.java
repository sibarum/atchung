package sibarum.elektro.queue;

import sibarum.elektro.queue.message.MessageType;
import sibarum.elektro.queue.message.PeerId;

import java.util.concurrent.CompletionStage;

/**
 * A typed emitter &mdash; the producing corner of elektro-Q's triad.
 *
 * <p>An {@code Action} is bound to a single {@link MessageType} and obtained from a
 * {@link Conduit} via {@link Conduit#action(MessageType)}. Emitting encodes the message
 * with the type's codec, frames it, and hands it to the conduit's transport; callers
 * never touch bytes or sockets.
 *
 * <p>Three delivery shapes are offered, cheapest first: fire-and-forget
 * {@link #emit(Object)} / {@link #emit(Object, PeerId)}, and correlated
 * {@link #request(Object, PeerId, MessageType)} for request/response. An action is
 * safe to reuse and to share across threads.
 *
 * @param <T> the message type this action emits
 */
public interface Action<T> {

    /** The message type this action emits. */
    MessageType<T> type();

    /** Fire-and-forget to every connected peer ({@link PeerId#BROADCAST}). */
    void emit(T message);

    /** Fire-and-forget to a single peer. */
    void emit(T message, PeerId destination);

    /**
     * Sends {@code message} to {@code destination} as a request and completes when a
     * correlated reply of {@code replyType} arrives (or completes exceptionally on
     * timeout/transport failure).
     *
     * @param <R> the expected reply type
     */
    <R> CompletionStage<R> request(T message, PeerId destination, MessageType<R> replyType);
}
