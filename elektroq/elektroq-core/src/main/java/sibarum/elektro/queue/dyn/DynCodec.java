package sibarum.elektro.queue.dyn;

import sibarum.elektro.queue.wire.Codec;
import sibarum.elektro.queue.wire.WireReader;
import sibarum.elektro.queue.wire.WireWriter;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The hand-written {@link Codec} for {@link DynValue} &mdash; the self-describing counterpart to the
 * generated, fixed-schema codecs.
 *
 * <p>Where a generated codec knows every field up front and writes them positionally, this codec
 * emits a <b>one-byte kind tag</b> ahead of each node and recurses into sequences and structs, so a
 * decoder reconstructs the value with no prior knowledge of its shape. All of it goes through the
 * ordinary {@link WireWriter}/{@link WireReader} primitives &mdash; no reflection, no intermediate
 * object graph &mdash; keeping the native-image story identical to the rest of elektro-Q.
 *
 * <p>Encoding choices: signed 64-bit integers are zig-zag mapped before an LEB128 var-long (small
 * magnitudes, positive or negative, stay compact); code points, lengths, and counts are unsigned
 * var-ints; decimals ride as their canonical string. The codec is stateless and thread-safe.
 */
public final class DynCodec implements Codec<DynValue> {

    /** The shared, stateless instance. */
    public static final DynCodec INSTANCE = new DynCodec();

    // Kind tags. Stable on the wire; append-only.
    private static final byte T_NULL = 0;
    private static final byte T_BOOL = 1;
    private static final byte T_I64 = 2;
    private static final byte T_DEC = 3;
    private static final byte T_CHR = 4;
    private static final byte T_STR = 5;
    private static final byte T_BYTES = 6;
    private static final byte T_SEQ = 7;
    private static final byte T_STRUCT = 8;

    private DynCodec() {}

    @Override
    public void encode(DynValue value, WireWriter out) {
        switch (value) {
            case DynValue.Null ignored -> out.putByte(T_NULL);
            case DynValue.Bool b -> out.putByte(T_BOOL).putBoolean(b.value());
            case DynValue.I64 i -> out.putByte(T_I64).putVarLong(zigZag(i.value()));
            case DynValue.Dec d -> out.putByte(T_DEC).putString(d.canonical());
            case DynValue.Chr c -> out.putByte(T_CHR).putVarInt(c.codePoint());
            case DynValue.Str s -> out.putByte(T_STR).putString(s.value());
            case DynValue.Bytes by -> {
                out.putByte(T_BYTES).putVarInt(by.value().length);
                out.putBytes(by.value());
            }
            case DynValue.Seq seq -> {
                out.putByte(T_SEQ).putVarInt(seq.elements().size());
                for (DynValue element : seq.elements()) {
                    encode(element, out);
                }
            }
            case DynValue.Struct struct -> {
                out.putByte(T_STRUCT).putString(struct.typeName());
                out.putVarInt(struct.fields().size());
                for (Map.Entry<String, DynValue> field : struct.fields().entrySet()) {
                    out.putString(field.getKey());
                    encode(field.getValue(), out);
                }
            }
        }
    }

    @Override
    public DynValue decode(WireReader in) {
        byte tag = in.getByte();
        return switch (tag) {
            case T_NULL -> DynValue.Null.INSTANCE;
            case T_BOOL -> new DynValue.Bool(in.getBoolean());
            case T_I64 -> new DynValue.I64(unZigZag(in.getVarLong()));
            case T_DEC -> new DynValue.Dec(in.getString());
            case T_CHR -> new DynValue.Chr(in.getVarInt());
            case T_STR -> new DynValue.Str(in.getString());
            case T_BYTES -> new DynValue.Bytes(in.getBytes(in.getVarInt()));
            case T_SEQ -> {
                int count = in.getVarInt();
                List<DynValue> elements = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    elements.add(decode(in));
                }
                yield new DynValue.Seq(elements);
            }
            case T_STRUCT -> {
                String typeName = in.getString();
                int count = in.getVarInt();
                Map<String, DynValue> fields = new LinkedHashMap<>();
                for (int i = 0; i < count; i++) {
                    String name = in.getString();
                    fields.put(name, decode(in));
                }
                yield new DynValue.Struct(typeName, fields);
            }
            default -> throw new IllegalStateException("Unknown DynValue tag: " + tag);
        };
    }

    /** Maps a signed long to an unsigned one so small magnitudes of either sign stay compact. */
    private static long zigZag(long v) {
        return (v << 1) ^ (v >> 63);
    }

    private static long unZigZag(long u) {
        return (u >>> 1) ^ -(u & 1L);
    }
}
