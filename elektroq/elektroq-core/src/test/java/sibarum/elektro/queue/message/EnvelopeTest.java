package sibarum.elektro.queue.message;

import org.junit.jupiter.api.Test;
import sibarum.elektro.queue.wire.Codec;
import sibarum.elektro.queue.wire.WireBufferReader;
import sibarum.elektro.queue.wire.WireBufferWriter;
import sibarum.elektro.queue.wire.WireReader;
import sibarum.elektro.queue.wire.WireWriter;
import sibarum.intrabuf.Compat;
import sibarum.intrabuf.HeaderCodec;
import sibarum.intrabuf.IbReader;
import sibarum.intrabuf.IbWriter;
import sibarum.intrabuf.SchemaInfo;
import sibarum.intrabuf.annotation.IntrabufMessage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EnvelopeTest {

    record Note(String text) { }

    @IntrabufMessage(name = "test.Reading", id = 900)
    record Reading(String sensor, double value) { }

    private static final MessageType<Note> NOTE = new MessageType<>(1, 3, "Note", Note.class, new Codec<>() {
        @Override public void encode(Note v, WireWriter out) { out.putString(v.text()); }
        @Override public Note decode(WireReader in) { return new Note(in.getString()); }
    });

    private static byte[] write(Envelope e) {
        IbWriter w = new IbWriter();
        EnvelopeIntrabuf.CODEC.write(w, e);
        return w.toByteArray();
    }

    @Test
    void roundTripsWithFlagsAndCorrelation() {
        byte flags = (byte) (Flags.REQUEST | Flags.COMPRESSED);
        Envelope e = Envelope.of(NOTE.schema(), flags, 0xCAFEBABEL);
        HeaderCodec.Frame<Envelope> f = EnvelopeIntrabuf.CODEC.readFrame(new IbReader(write(e)));
        assertEquals(e, f.header());
        assertEquals(flags, f.header().flags());
        assertEquals(Protocol.MAGIC, f.header().magic());
        assertEquals(1, f.facts().schemaId());
    }

    @Test
    void aOneWayEmitIsTenBytes() {
        // Version 1's fixed header was 24.
        assertEquals(10, write(Envelope.of(NOTE.schema(), Flags.NONE, 0)).length);
    }

    @Test
    void messageRecordsMapOntoFullCompatibility() {
        SchemaInfo s = NOTE.schema();
        assertEquals(1, s.major());
        assertEquals(2, s.minor(), "schemaVersion 3 is minor 2");
        assertEquals(Compat.FULL, s.compat());
        assertNull(Envelope.of(s, Flags.NONE, 0).fingerprint(), "release FULL schemas carry no fingerprint");
    }

    @Test
    void intrabufTypesCarryTheirOwnSchema() {
        MessageType<Reading> reading = MessageType.of(EnvelopeTest_ReadingIntrabuf.CODEC, Reading.class);
        assertEquals(900, reading.id());
        assertEquals(EnvelopeTest_ReadingIntrabuf.SCHEMA, reading.schema());
        Envelope e = Envelope.of(reading.schema(), Flags.NONE, 0);
        assertEquals(EnvelopeTest_ReadingIntrabuf.FINGERPRINT, e.fingerprint(), "STRICT schemas carry their fingerprint");
        assertEquals(Compat.STRICT, e.compat());
        // Built twice, it is the same descriptor, so registering it from two places is harmless.
        assertEquals(reading, MessageType.of(EnvelopeTest_ReadingIntrabuf.CODEC, Reading.class));
    }

    @Test
    void intrabufCodecAlsoWorksAsAPlainCodec() {
        MessageType<Reading> reading = MessageType.of(EnvelopeTest_ReadingIntrabuf.CODEC, Reading.class);
        WireBufferWriter w = new WireBufferWriter();
        reading.codec().encode(new Reading("t1", 21.5), w);
        assertEquals(new Reading("t1", 21.5), reading.codec().decode(new WireBufferReader(w.toByteArray())));
    }

    @Test
    void aCauseIsEightBytesWhenThereIsOneAndNothingWhenThereIsNot() {
        long cause = 0x3f2a1_4_07_0000_04f2L;   // any nonzero long: the envelope does not interpret it
        Envelope with = Envelope.of(NOTE.schema(), Flags.NONE, 0, cause);
        Envelope without = Envelope.of(NOTE.schema(), Flags.NONE, 0);

        assertEquals(cause, EnvelopeIntrabuf.CODEC.readFrame(new IbReader(write(with))).header().causeId());
        assertNull(without.cause());
        assertEquals(0L, without.causeId());
        assertTrue(without.causes(), "every frame says its sender reads causes, whether or not it brings one");
        assertEquals(10, write(without).length, "saying so is a bit, not a byte");
        assertEquals(18, write(with).length);
    }

    /**
     * Version 2 as it was before causes, written out by hand from the generated codec of that time, so the tests
     * below are against a peer that predates the field and not against this build's idea of one.
     */
    static final class Before {
        static byte[] write(int typeId, byte flags, long correlationId) {
            SchemaInfo s = NOTE.schema();
            IbWriter w = new IbWriter();
            long bits = (flags & Flags.REQUEST) != 0 ? 0x4 : 0;
            bits |= (flags & Flags.REPLY) != 0 ? 0x8 : 0;
            w.writeBits(bits, 1);
            w.writeInt16(Protocol.MAGIC);
            w.writeInt8((byte) Protocol.VERSION);
            w.writeInt8((byte) sibarum.intrabuf.Header.of(s).wireVersion());
            w.writeInt32(typeId);
            w.writeInt16((short) s.major());
            w.writeInt16((short) s.minor());
            w.writeEnum(s.compat());
            w.writeInt64(correlationId);
            return w.toByteArray();
        }

        /** Reads the header as that codec did, leaving {@code r} where it believed the payload began. */
        static int readTypeId(IbReader r) {
            int at = r.beginBits(1);
            long bits = r.bitsWord(at, 1, 0);
            r.readInt16();
            r.readInt8();
            r.readInt8();
            int typeId = r.readInt32();
            r.readInt16();
            r.readInt16();
            r.readEnum(Compat.values().length, "Compat");
            if ((bits & 0x2) != 0) {
                r.readFixed32();
            }
            r.readInt64();
            return typeId;
        }
    }

    @Test
    void aPeerFromBeforeCausesReadsAFrameWithoutOneExactly() {
        byte[] header = write(Envelope.of(NOTE.schema(), Flags.REQUEST, 42));
        IbReader r = new IbReader(header);
        assertEquals(1, Before.readTypeId(r));
        assertEquals(header.length, r.position(), "the payload begins where it always did: the new bit is ignored");
    }

    @Test
    void aFrameFromBeforeCausesIsReadAsOneWithNone() {
        Envelope e = EnvelopeIntrabuf.CODEC.read(new IbReader(Before.write(1, Flags.REQUEST, 42)));
        assertEquals(42, e.correlationId());
        assertTrue(e.request());
        assertFalse(e.causes(), "its sender is not one to send a cause to");
        assertNull(e.cause());
    }

    @Test
    void theHelloIsATypeNoRegistryHas() {
        Envelope hello = Envelope.hello();
        assertEquals(Protocol.HELLO_TYPE, hello.typeId());
        assertTrue(hello.causes());
        assertEquals(Protocol.HELLO_TYPE, Before.readTypeId(new IbReader(write(hello))));
        assertNull(new ArrayMessageRegistry().byId(Protocol.HELLO_TYPE), "so a peer from before drops it");
        MessageType<Note> squatter = new MessageType<>(Protocol.HELLO_TYPE, 1, "Squatter", Note.class, NOTE.codec());
        assertThrows(IllegalArgumentException.class, () -> new ArrayMessageRegistry().register(squatter));
    }

    @Test
    void flagsDefaultOff() {
        Envelope e = Envelope.of(NOTE.schema(), Flags.NONE, 7);
        assertFalse(e.request() || e.reply() || e.fragment() || e.compressed());
        assertTrue(e.correlationId() == 7);
    }
}
