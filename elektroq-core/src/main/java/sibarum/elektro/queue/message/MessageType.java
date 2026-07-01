package sibarum.elektro.queue.message;

import sibarum.elektro.queue.wire.Codec;

import java.util.Objects;

/**
 * Runtime descriptor binding a message's stable {@code id} to its codec and metadata.
 *
 * <p>One {@code MessageType} exists per {@link Message}-annotated type. Instances are
 * produced by generated code and handed to a {@link MessageRegistry}; the emit and
 * react paths route purely on the integer {@link #id()}, never on {@link #javaType()}.
 * The {@code Class} is retained only for compile-safe API signatures and diagnostics,
 * so no reflective dispatch is required and native-image needs no extra configuration.
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
}
