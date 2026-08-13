package sibarum.elektro.queue.wire;

import org.junit.jupiter.api.Test;
import sibarum.elektro.queue.ElektroException;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WireRoundTripTest {

    @Test
    void primitivesRoundTrip() {
        WireBufferWriter w = new WireBufferWriter();
        w.putByte((byte) -7)
         .putBoolean(true)
         .putShort((short) 30000)
         .putInt(Integer.MIN_VALUE)
         .putLong(Long.MAX_VALUE)
         .putFloat(3.5f)
         .putDouble(-2.25d)
         .putBoolean(false);

        WireBufferReader r = new WireBufferReader(w.toByteArray());
        assertEquals((byte) -7, r.getByte());
        assertTrue(r.getBoolean());
        assertEquals((short) 30000, r.getShort());
        assertEquals(Integer.MIN_VALUE, r.getInt());
        assertEquals(Long.MAX_VALUE, r.getLong());
        assertEquals(3.5f, r.getFloat());
        assertEquals(-2.25d, r.getDouble());
        assertFalse(r.getBoolean());
        assertFalse(r.hasRemaining());
    }

    @Test
    void varIntsRoundTripAcrossBoundaries() {
        int[] ints = {0, 1, 127, 128, 300, -1, Integer.MAX_VALUE, Integer.MIN_VALUE};
        long[] longs = {0L, 127L, 128L, 1L << 35, -1L, Long.MAX_VALUE, Long.MIN_VALUE};

        WireBufferWriter w = new WireBufferWriter();
        for (int v : ints) {
            w.putVarInt(v);
        }
        for (long v : longs) {
            w.putVarLong(v);
        }

        WireBufferReader r = new WireBufferReader(w.toByteArray());
        for (int v : ints) {
            assertEquals(v, r.getVarInt(), "varint " + v);
        }
        for (long v : longs) {
            assertEquals(v, r.getVarLong(), "varlong " + v);
        }
    }

    @Test
    void varIntUsesMinimalBytes() {
        assertEquals(1, varIntLength(0));
        assertEquals(1, varIntLength(127));
        assertEquals(2, varIntLength(128));
        assertEquals(5, varIntLength(-1)); // negatives always span the full 5 bytes
    }

    private static int varIntLength(int value) {
        WireBufferWriter w = new WireBufferWriter();
        w.putVarInt(value);
        return w.position();
    }

    @Test
    void stringsAndBytesRoundTrip() {
        byte[] blob = "the quick brown fox".getBytes(StandardCharsets.UTF_8);
        WireBufferWriter w = new WireBufferWriter();
        w.putString("héllo — wörld")
         .putVarInt(blob.length)
         .putBytes(blob);

        WireBufferReader r = new WireBufferReader(w.toByteArray());
        assertEquals("héllo — wörld", r.getString());
        int len = r.getVarInt();
        assertArrayEquals(blob, r.getBytes(len));
    }

    @Test
    void emptyStringRoundTrips() {
        WireBufferWriter w = new WireBufferWriter();
        w.putString("");
        WireBufferReader r = new WireBufferReader(w.toByteArray());
        assertEquals("", r.getString());
    }

    @Test
    void underflowRaisesElektroException() {
        WireBufferReader r = new WireBufferReader(new byte[]{0x01});
        assertThrows(ElektroException.class, r::getInt);
    }

    @Test
    void toByteBufferSharesWrittenRegion() {
        WireBufferWriter w = new WireBufferWriter(2);
        w.putInt(0x0A0B0C0D);
        assertEquals(4, w.toByteBuffer().remaining());
    }
}
