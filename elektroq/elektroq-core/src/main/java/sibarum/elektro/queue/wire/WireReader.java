package sibarum.elektro.queue.wire;

/**
 * A source for decoding primitive values and byte sequences from a message payload.
 *
 * <p>{@code WireReader} is the read half of elektro-Q's serialization contract and
 * is symmetric with {@link WireWriter}: values must be read in the same order they
 * were written, using the matching accessor. Generated {@link Codec} implementations
 * call these methods directly &mdash; no reflection, no intermediate object graph.
 *
 * <p><b>Schema evolution.</b> Codecs support forward/backward compatibility by
 * checking {@link #hasRemaining()} before reading fields added in a later schema
 * version, and by tolerating trailing bytes they do not recognise. See
 * {@code sibarum.elektro.queue.message.WireField#since()}.
 *
 * <p>Implementations are <b>not</b> required to be thread-safe.
 */
public interface WireReader {

    byte getByte();

    boolean getBoolean();

    short getShort();

    int getInt();

    long getLong();

    float getFloat();

    double getDouble();

    /** Reads an unsigned 32-bit LEB128 var-int. */
    int getVarInt();

    /** Reads an unsigned 64-bit LEB128 var-int. */
    long getVarLong();

    /** Reads {@code length} bytes into a freshly allocated array. */
    byte[] getBytes(int length);

    void getBytes(byte[] destination, int offset, int length);

    /** Reads a var-int length prefix followed by that many UTF-8 bytes. */
    String getString();

    /** {@code true} if at least one more byte can be read. */
    boolean hasRemaining();

    /** Number of bytes still available to read. */
    int remaining();
}
