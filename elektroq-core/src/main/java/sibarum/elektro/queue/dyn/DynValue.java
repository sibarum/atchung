package sibarum.elektro.queue.dyn;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A neutral, self-describing runtime value &mdash; elektro-Q's representation of data whose
 * shape is <b>not</b> known at compile time.
 *
 * <p>The rest of elektro-Q routes and (de)serializes {@link sibarum.elektro.queue.message.Message}
 * records through generated, integer-id-keyed codecs; that is the fast path and it needs the type
 * fixed at compile time. A {@code DynValue} is the escape hatch for callers &mdash; a dynamic
 * language runtime, a scripting bridge, a debug channel &mdash; that must carry <em>open-ended</em>
 * structured values over the same wire. It is a small, closed algebra of the shapes such a runtime
 * produces; {@link DynCodec} serialises it with a tag-per-node scheme, so it stays reflection-free
 * and needs no native-image reachability metadata, exactly like the generated codecs.
 *
 * <p>A whole family of logical types travels under a single elektro-Q {@code MessageType}
 * ({@link DynMessages#DYN}); the logical identity lives in {@link Struct#typeName()}, and a consumer
 * dispatches on it. This keeps elektro-Q's "one integer id per wire type" contract intact while
 * letting an unbounded set of runtime types share one conduit.
 *
 * <p>Instances are immutable (a {@link Seq} or {@link Struct} wraps the caller's collection without
 * copying, so callers must not mutate what they pass in).
 */
public sealed interface DynValue
        permits DynValue.Null, DynValue.Bool, DynValue.I64, DynValue.Dec,
                DynValue.Chr, DynValue.Str, DynValue.Bytes, DynValue.Seq, DynValue.Struct {

    /** Absence of a value &mdash; the neutral counterpart to a runtime's null/unit/omission value. */
    record Null() implements DynValue {
        public static final Null INSTANCE = new Null();
    }

    /** A boolean. */
    record Bool(boolean value) implements DynValue {}

    /** A 64-bit signed integer. */
    record I64(long value) implements DynValue {}

    /**
     * An arbitrary-precision decimal, carried as its canonical {@code String} form (never a
     * lossy {@code double}). Round-trips a {@code BigDecimal} exactly, including scale.
     */
    record Dec(String canonical) implements DynValue {
        public Dec {
            Objects.requireNonNull(canonical, "canonical");
        }
    }

    /** A single Unicode code point. */
    record Chr(int codePoint) implements DynValue {}

    /** A UTF-8 string. */
    record Str(String value) implements DynValue {
        public Str {
            Objects.requireNonNull(value, "value");
        }
    }

    /** An opaque byte blob. Equality is by content, so a decoded copy equals the original. */
    record Bytes(byte[] value) implements DynValue {
        public Bytes {
            Objects.requireNonNull(value, "value");
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Bytes other && java.util.Arrays.equals(value, other.value);
        }

        @Override
        public int hashCode() {
            return java.util.Arrays.hashCode(value);
        }

        @Override
        public String toString() {
            return "Bytes[length=" + value.length + "]";
        }
    }

    /** An ordered sequence &mdash; a tuple, list, or (materialised) stream. */
    record Seq(List<DynValue> elements) implements DynValue {
        public Seq {
            Objects.requireNonNull(elements, "elements");
        }
    }

    /**
     * A named record: a logical type name plus its fields in declaration order. The
     * {@code fields} map's iteration order is the wire order, so pass an order-preserving map
     * (e.g. {@code LinkedHashMap}).
     */
    record Struct(String typeName, Map<String, DynValue> fields) implements DynValue {
        public Struct {
            Objects.requireNonNull(typeName, "typeName");
            Objects.requireNonNull(fields, "fields");
        }
    }
}
