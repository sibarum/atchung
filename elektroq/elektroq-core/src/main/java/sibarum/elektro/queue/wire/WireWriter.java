package sibarum.elektro.queue.wire;

/**
 * A sink for encoding primitive values and byte sequences into a message payload.
 *
 * <p>{@code WireWriter} is the write half of elektro-Q's serialization contract.
 * Generated {@link Codec} implementations call these methods directly; there is no
 * reflection and no intermediate object model, which keeps encoding allocation-free
 * on the hot path and fully compatible with GraalVM native-image.
 *
 * <p>Multi-byte integers are written in <b>big-endian</b> (network) byte order.
 * Variable-length integers use unsigned LEB128; a codec that needs to encode a
 * signed value compactly is expected to apply zig-zag mapping before calling
 * {@link #putVarInt(int)} / {@link #putVarLong(long)}.
 *
 * <p>Implementations are <b>not</b> required to be thread-safe. A writer instance
 * is expected to be confined to the thread performing a single encode.
 */
public interface WireWriter {

    WireWriter putByte(byte value);

    WireWriter putBoolean(boolean value);

    WireWriter putShort(short value);

    WireWriter putInt(int value);

    WireWriter putLong(long value);

    WireWriter putFloat(float value);

    WireWriter putDouble(double value);

    /** Writes an unsigned 32-bit integer as LEB128 (1&ndash;5 bytes). */
    WireWriter putVarInt(int value);

    /** Writes an unsigned 64-bit integer as LEB128 (1&ndash;10 bytes). */
    WireWriter putVarLong(long value);

    WireWriter putBytes(byte[] source);

    WireWriter putBytes(byte[] source, int offset, int length);

    /** Writes a UTF-8 string prefixed with its unsigned byte length as a var-int. */
    WireWriter putString(String value);

    /** Number of bytes written so far. */
    int position();
}
