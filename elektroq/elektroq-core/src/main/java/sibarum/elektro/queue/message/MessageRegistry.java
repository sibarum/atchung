package sibarum.elektro.queue.message;

/**
 * Resolves incoming {@code typeId}s to the {@link MessageType} that can decode them.
 *
 * <p>A registry is populated once at startup by generated registration code &mdash;
 * one {@link #register} call per {@link Message} type &mdash; and is then read on
 * every inbound frame. Registration is reflection-free: generated code holds direct
 * references to the codecs it instantiated, so nothing is scanned or looked up by name.
 *
 * <p>The lookup on {@link #byId(int)} is on the hot receive path and must be fast; the
 * bundled {@code ArrayMessageRegistry} keeps types in an id-indexed table. A registry
 * is effectively immutable after startup and safe to share across threads for reads.
 */
public interface MessageRegistry {

    /**
     * Registers a message type. Throws {@link ElektroException} if its id is already
     * bound to a different type.
     */
    <T> void register(MessageType<T> type);

    /** Returns the type bound to {@code id}, or {@code null} if none is registered. */
    MessageType<?> byId(int id);

    /** {@code true} if a type is registered under {@code id}. */
    boolean contains(int id);

    /** Number of registered types. */
    int size();
}
