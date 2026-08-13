package sibarum.elektro.queue.wire;

import sibarum.elektro.queue.ElektroException;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * A {@link WireReader} over a {@code byte[]} region, the inverse of
 * {@link WireBufferWriter}.
 *
 * <p>Reads are bounds-checked: underrunning the region raises {@link ElektroException}
 * rather than an {@link IndexOutOfBoundsException}, so a malformed or truncated frame
 * surfaces as a protocol error. Not thread-safe.
 */
public final class WireBufferReader implements WireReader {

    private final byte[] buffer;
    private final int limit;
    private int position;

    public WireBufferReader(byte[] source) {
        this(source, 0, source.length);
    }

    public WireBufferReader(byte[] source, int offset, int length) {
        if (offset < 0 || length < 0 || offset + length > source.length) {
            throw new IndexOutOfBoundsException(
                    "offset=" + offset + " length=" + length + " capacity=" + source.length);
        }
        this.buffer = source;
        this.position = offset;
        this.limit = offset + length;
    }

    /** Reads the buffer's remaining bytes into a reader and advances the buffer past them. */
    public static WireBufferReader of(ByteBuffer source) {
        byte[] copy = new byte[source.remaining()];
        source.get(copy);
        return new WireBufferReader(copy);
    }

    private void require(int count) {
        if (position + count > limit) {
            throw new ElektroException(
                    "Wire underflow: need " + count + " byte(s), have " + (limit - position));
        }
    }

    @Override
    public byte getByte() {
        require(1);
        return buffer[position++];
    }

    @Override
    public boolean getBoolean() {
        return getByte() != 0;
    }

    @Override
    public short getShort() {
        require(2);
        return (short) ((buffer[position++] & 0xFF) << 8
                | (buffer[position++] & 0xFF));
    }

    @Override
    public int getInt() {
        require(4);
        return (buffer[position++] & 0xFF) << 24
                | (buffer[position++] & 0xFF) << 16
                | (buffer[position++] & 0xFF) << 8
                | (buffer[position++] & 0xFF);
    }

    @Override
    public long getLong() {
        require(8);
        long result = 0;
        for (int i = 0; i < 8; i++) {
            result = (result << 8) | (buffer[position++] & 0xFFL);
        }
        return result;
    }

    @Override
    public float getFloat() {
        return Float.intBitsToFloat(getInt());
    }

    @Override
    public double getDouble() {
        return Double.longBitsToDouble(getLong());
    }

    @Override
    public int getVarInt() {
        int result = 0;
        int shift = 0;
        while (shift < 35) {
            byte b = getByte();
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
        }
        throw new ElektroException("VarInt exceeds 32 bits");
    }

    @Override
    public long getVarLong() {
        long result = 0;
        int shift = 0;
        while (shift < 70) {
            byte b = getByte();
            result |= (b & 0x7FL) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
        }
        throw new ElektroException("VarLong exceeds 64 bits");
    }

    @Override
    public byte[] getBytes(int length) {
        if (length < 0) {
            throw new ElektroException("Negative length: " + length);
        }
        require(length);
        byte[] out = new byte[length];
        System.arraycopy(buffer, position, out, 0, length);
        position += length;
        return out;
    }

    @Override
    public void getBytes(byte[] destination, int offset, int length) {
        if (offset < 0 || length < 0 || offset + length > destination.length) {
            throw new IndexOutOfBoundsException(
                    "offset=" + offset + " length=" + length + " capacity=" + destination.length);
        }
        require(length);
        System.arraycopy(buffer, position, destination, offset, length);
        position += length;
    }

    @Override
    public String getString() {
        int length = getVarInt();
        require(length);
        String s = new String(buffer, position, length, StandardCharsets.UTF_8);
        position += length;
        return s;
    }

    @Override
    public boolean hasRemaining() {
        return position < limit;
    }

    @Override
    public int remaining() {
        return limit - position;
    }
}
