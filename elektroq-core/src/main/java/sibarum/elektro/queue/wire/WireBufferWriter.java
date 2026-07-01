package sibarum.elektro.queue.wire;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * A {@link WireWriter} that encodes into a growable heap {@code byte[]}.
 *
 * <p>Values are written big-endian; var-ints use unsigned LEB128. The backing array
 * grows geometrically, so a codec can encode a payload of unknown size without
 * pre-measuring it. The result is exposed either as a fresh array ({@link #toByteArray()})
 * or as a zero-copy view over the written region ({@link #toByteBuffer()}), the latter
 * being what a conduit hands to {@link sibarum.elektro.queue.transport.Transport#send}.
 *
 * <p>Not thread-safe; confine one instance to a single encode. {@link #reset()} allows
 * reuse to avoid re-allocating between messages.
 */
public final class WireBufferWriter implements WireWriter {

    private static final int DEFAULT_CAPACITY = 64;

    private byte[] buffer;
    private int position;

    public WireBufferWriter() {
        this(DEFAULT_CAPACITY);
    }

    public WireBufferWriter(int initialCapacity) {
        if (initialCapacity < 0) {
            throw new IllegalArgumentException("initialCapacity < 0: " + initialCapacity);
        }
        this.buffer = new byte[Math.max(initialCapacity, 1)];
    }

    private void ensure(int extra) {
        int required = position + extra;
        if (required > buffer.length) {
            int newLength = buffer.length;
            while (newLength < required) {
                newLength <<= 1;
                if (newLength < 0) { // overflow guard
                    newLength = required;
                    break;
                }
            }
            buffer = Arrays.copyOf(buffer, newLength);
        }
    }

    @Override
    public WireWriter putByte(byte value) {
        ensure(1);
        buffer[position++] = value;
        return this;
    }

    @Override
    public WireWriter putBoolean(boolean value) {
        return putByte((byte) (value ? 1 : 0));
    }

    @Override
    public WireWriter putShort(short value) {
        ensure(2);
        buffer[position++] = (byte) (value >>> 8);
        buffer[position++] = (byte) value;
        return this;
    }

    @Override
    public WireWriter putInt(int value) {
        ensure(4);
        buffer[position++] = (byte) (value >>> 24);
        buffer[position++] = (byte) (value >>> 16);
        buffer[position++] = (byte) (value >>> 8);
        buffer[position++] = (byte) value;
        return this;
    }

    @Override
    public WireWriter putLong(long value) {
        ensure(8);
        buffer[position++] = (byte) (value >>> 56);
        buffer[position++] = (byte) (value >>> 48);
        buffer[position++] = (byte) (value >>> 40);
        buffer[position++] = (byte) (value >>> 32);
        buffer[position++] = (byte) (value >>> 24);
        buffer[position++] = (byte) (value >>> 16);
        buffer[position++] = (byte) (value >>> 8);
        buffer[position++] = (byte) value;
        return this;
    }

    @Override
    public WireWriter putFloat(float value) {
        return putInt(Float.floatToIntBits(value));
    }

    @Override
    public WireWriter putDouble(double value) {
        return putLong(Double.doubleToLongBits(value));
    }

    @Override
    public WireWriter putVarInt(int value) {
        // Unsigned LEB128 over 32 bits.
        int v = value;
        ensure(5);
        while ((v & ~0x7F) != 0) {
            buffer[position++] = (byte) ((v & 0x7F) | 0x80);
            v >>>= 7;
        }
        buffer[position++] = (byte) v;
        return this;
    }

    @Override
    public WireWriter putVarLong(long value) {
        long v = value;
        ensure(10);
        while ((v & ~0x7FL) != 0) {
            buffer[position++] = (byte) ((v & 0x7F) | 0x80);
            v >>>= 7;
        }
        buffer[position++] = (byte) v;
        return this;
    }

    @Override
    public WireWriter putBytes(byte[] source) {
        return putBytes(source, 0, source.length);
    }

    @Override
    public WireWriter putBytes(byte[] source, int offset, int length) {
        if (offset < 0 || length < 0 || offset + length > source.length) {
            throw new IndexOutOfBoundsException(
                    "offset=" + offset + " length=" + length + " capacity=" + source.length);
        }
        ensure(length);
        System.arraycopy(source, offset, buffer, position, length);
        position += length;
        return this;
    }

    @Override
    public WireWriter putString(String value) {
        byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
        putVarInt(utf8.length);
        return putBytes(utf8, 0, utf8.length);
    }

    @Override
    public int position() {
        return position;
    }

    /** Discards written content so the writer can encode a fresh message. */
    public void reset() {
        position = 0;
    }

    /** Returns a copy of the bytes written so far. */
    public byte[] toByteArray() {
        return Arrays.copyOf(buffer, position);
    }

    /** Returns a zero-copy {@link ByteBuffer} view over the written region. */
    public ByteBuffer toByteBuffer() {
        return ByteBuffer.wrap(buffer, 0, position);
    }
}
