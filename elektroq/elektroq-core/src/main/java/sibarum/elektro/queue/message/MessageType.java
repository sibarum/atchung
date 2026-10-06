package sibarum.elektro.queue.message;

import sibarum.elektro.queue.wire.Codec;
import sibarum.intrabuf.Compat;
import sibarum.intrabuf.MessageCodec;
import sibarum.intrabuf.SchemaInfo;

import java.util.Objects;

/**
 * Runtime descriptor binding a message's stable {@code id} to its codec and metadata.
 *
 * <p>One {@code MessageType} exists per message type. Instances are produced by generated code &mdash; elektro-Q's
 * own for {@link Message} records, or {@link #of} for intrabuf {@code @IntrabufMessage} records &mdash; and handed
 * to a {@link MessageRegistry}; the emit and react paths route purely on the integer {@link #id()}, never on
 * {@link #javaType()}. The {@code Class} is retained only for compile-safe API signatures and diagnostics, so no
 * reflective dispatch is required and native-image needs no extra configuration.
 *
 * @param <T> the message type described
 */
public record MessageType<T>(int id, int schemaVersion, String name, Class<T> javaType, Codec<T> codec) {

    public MessageType {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(javaType, "javaType");
        Objects.requireNonNull(codec, "codec");
        if (schemaVersion < 1) {
            throw new IllegalArgumentException("schemaVersion must be >= 1, was " + schemaVersion);
        }
    }

    /**
     * A message type backed by an intrabuf codec. Its id is the schema's id, so declare the record with an explicit
     * {@code @IntrabufMessage(id = ...)} from the same id space as your {@link Message} ids. {@code schemaVersion}
     * is informational ({@code minor + 1}); the envelope carries the schema's real major, minor, mode and
     * fingerprint, and intrabuf decides compatibility from them.
     */
    public static <T> MessageType<T> of(MessageCodec<T> codec, Class<T> javaType) {
        SchemaInfo s = codec.schema();
        return new MessageType<>(s.id(), s.minor() + 1, s.name(), javaType, new IntrabufCodec<>(codec));
    }

    /**
     * The schema facts frames of this type carry in their {@link Envelope}. For an intrabuf-backed type, its own.
     * For a {@link Message} record, the closest equivalent of elektro-Q's evolution rules: major 1, minor
     * {@code schemaVersion - 1}, FULL compatibility (newer readers default missing trailing fields, older readers
     * ignore extra ones), no fingerprint.
     */
    public SchemaInfo schema() {
        if (codec instanceof IntrabufCodec<T> ic) {
            return ic.messageCodec().schema();
        }
        return new SchemaInfo(name, javaType.getName(), id, 1, schemaVersion - 1, Compat.FULL, 0, false, "elektroq");
    }
}
