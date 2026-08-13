package sibarum.elektro.queue.wire;

/**
 * Encodes and decodes a single message type to and from the wire.
 *
 * <p>Codecs are the reflection-free heart of elektro-Q's serialization. A codec is
 * normally <b>generated at compile time</b> by the elektro-Q annotation processor
 * from a type annotated with {@code sibarum.elektro.queue.message.Message}; the
 * generated code reads and writes each {@code WireField} in declared order. Codecs
 * may also be written by hand against {@link WireWriter} / {@link WireReader} when
 * full control over the byte layout is desired.
 *
 * <p>A codec instance must be <b>stateless and thread-safe</b>: the same instance is
 * shared across all encode/decode calls for its message type.
 *
 * @param <T> the message type this codec handles
 */
public interface Codec<T> {

    /** Writes {@code value} to {@code out} in this codec's wire layout. */
    void encode(T value, WireWriter out);

    /** Reads one value from {@code in}, inverse of {@link #encode}. */
    T decode(WireReader in);
}
