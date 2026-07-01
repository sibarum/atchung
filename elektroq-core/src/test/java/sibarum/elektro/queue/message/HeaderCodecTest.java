package sibarum.elektro.queue.message;

import org.junit.jupiter.api.Test;
import sibarum.elektro.queue.ElektroException;
import sibarum.elektro.queue.wire.WireBufferReader;
import sibarum.elektro.queue.wire.WireBufferWriter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HeaderCodecTest {

    @Test
    void roundTripsAndMatchesFixedSize() {
        MessageHeader header = new MessageHeader(42, 3, Flags.REQUEST, 0xCAFEBABEL, 1024);

        WireBufferWriter w = new WireBufferWriter();
        HeaderCodec.INSTANCE.encode(header, w);
        assertEquals(Protocol.HEADER_BYTES, w.position(), "encoded header must be the advertised fixed size");

        MessageHeader decoded = HeaderCodec.INSTANCE.decode(new WireBufferReader(w.toByteArray()));
        assertEquals(header, decoded);
        assertTrue(decoded.isRequest());
    }

    @Test
    void rejectsBadMagic() {
        byte[] frame = new byte[Protocol.HEADER_BYTES];
        frame[0] = 0x00;
        frame[1] = 0x00; // not MAGIC
        assertThrows(ElektroException.class,
                () -> HeaderCodec.INSTANCE.decode(new WireBufferReader(frame)));
    }

    @Test
    void rejectsUnsupportedVersion() {
        WireBufferWriter w = new WireBufferWriter();
        w.putShort(Protocol.MAGIC)
         .putByte((byte) (Protocol.VERSION + 1))
         .putByte(Flags.NONE)
         .putInt(1).putInt(1).putLong(0).putInt(0);
        assertThrows(ElektroException.class,
                () -> HeaderCodec.INSTANCE.decode(new WireBufferReader(w.toByteArray())));
    }
}
