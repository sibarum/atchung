package sibarum.elektro.queue;

import sibarum.elektro.queue.message.MessageType;

/**
 * A typed reactor &mdash; the consuming corner of elektro-Q's triad.
 *
 * <p>An {@code Actor} is registered against a {@link MessageType} via
 * {@link Conduit#subscribe(MessageType, Actor)}. When a frame of that type arrives,
 * the conduit decodes it and invokes {@link #react} with the decoded message and a
 * {@link MessageContext} describing its origin. This is a functional interface, so an
 * actor may be a lambda or a Truffle node that translates the event into a DSL call.
 *
 * <p>Threading &mdash; the conduit decides which thread calls {@code react} and whether
 * calls for a given peer are serialised; implementations should treat {@code react} as
 * potentially concurrent unless the conduit documents otherwise, and must not block it.
 *
 * @param <T> the message type this actor reacts to
 */
@FunctionalInterface
public interface Actor<T> {

    void react(T message, MessageContext context);
}
