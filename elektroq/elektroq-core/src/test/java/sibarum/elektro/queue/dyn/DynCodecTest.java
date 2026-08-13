package sibarum.elektro.queue.dyn;

import org.junit.jupiter.api.Test;
import sibarum.elektro.queue.wire.WireBufferReader;
import sibarum.elektro.queue.wire.WireBufferWriter;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class DynCodecTest {

    private static DynValue roundTrip(DynValue value) {
        WireBufferWriter w = new WireBufferWriter();
        DynCodec.INSTANCE.encode(value, w);
        WireBufferReader r = new WireBufferReader(w.toByteArray());
        DynValue decoded = DynCodec.INSTANCE.decode(r);
        assertFalse(r.hasRemaining(), "codec left trailing bytes for " + value);
        return decoded;
    }

    @Test
    void scalarsRoundTrip() {
        assertEquals(DynValue.Null.INSTANCE, roundTrip(DynValue.Null.INSTANCE));
        assertEquals(new DynValue.Bool(true), roundTrip(new DynValue.Bool(true)));
        assertEquals(new DynValue.Bool(false), roundTrip(new DynValue.Bool(false)));
        assertEquals(new DynValue.Chr(0x1F600), roundTrip(new DynValue.Chr(0x1F600)));
        assertEquals(new DynValue.Str("héllo 世界"), roundTrip(new DynValue.Str("héllo 世界")));
        assertEquals(new DynValue.Str(""), roundTrip(new DynValue.Str("")));
    }

    @Test
    void signedIntegersRoundTripBothSigns() {
        long[] cases = {0, 1, -1, 127, -128, 1_000_000, -1_000_000, Long.MAX_VALUE, Long.MIN_VALUE};
        for (long c : cases) {
            assertEquals(new DynValue.I64(c), roundTrip(new DynValue.I64(c)), "for " + c);
        }
    }

    @Test
    void bigDecimalIsExactIncludingScale() {
        String[] decimals = {"0", "0.00", "-3.14159", "105.0", "1E+10", "123456789.987654321"};
        for (String d : decimals) {
            BigDecimal original = new BigDecimal(d);
            DynValue.Dec encoded = new DynValue.Dec(original.toString());
            DynValue decoded = roundTrip(encoded);
            BigDecimal recovered = new BigDecimal(((DynValue.Dec) decoded).canonical());
            // Exact equality, not compareTo: scale must survive too (0 != 0.00 under equals()).
            assertEquals(original, recovered, "for " + d);
            assertEquals(original.scale(), recovered.scale(), "scale for " + d);
        }
    }

    @Test
    void bytesRoundTrip() {
        byte[] blob = {0, 1, 2, -1, -128, 127};
        DynValue decoded = roundTrip(new DynValue.Bytes(blob));
        assertEquals(new DynValue.Bytes(blob), decoded);
    }

    @Test
    void emptySeqAndStruct() {
        assertEquals(new DynValue.Seq(List.of()), roundTrip(new DynValue.Seq(List.of())));
        assertEquals(new DynValue.Struct("Empty", Map.of()),
                roundTrip(new DynValue.Struct("Empty", new LinkedHashMap<>())));
    }

    @Test
    void nestedStructWithTupleAndFieldOrderPreserved() {
        Map<String, DynValue> inner = new LinkedHashMap<>();
        inner.put("x", new DynValue.I64(3));
        inner.put("y", new DynValue.I64(4));

        Map<String, DynValue> outer = new LinkedHashMap<>();
        outer.put("label", new DynValue.Str("origin"));
        outer.put("point", new DynValue.Struct("_anonymous/Vec", inner));
        outer.put("tags", new DynValue.Seq(List.of(new DynValue.Str("a"), DynValue.Null.INSTANCE)));
        outer.put("ratio", new DynValue.Dec("0.50"));

        DynValue.Struct original = new DynValue.Struct("_anonymous/Node", outer);
        DynValue decoded = roundTrip(original);

        assertEquals(original, decoded);
        // Field iteration order is the wire order and must be preserved.
        DynValue.Struct s = assertInstanceOf(DynValue.Struct.class, decoded);
        assertEquals(List.of("label", "point", "tags", "ratio"), List.copyOf(s.fields().keySet()));
    }
}
