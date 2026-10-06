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
    void flagsDefaultOff() {
        Envelope e = Envelope.of(NOTE.schema(), Flags.NONE, 7);
        assertFalse(e.request() || e.reply() || e.fragment() || e.compressed());
        assertTrue(e.correlationId() == 7);
    }
}
