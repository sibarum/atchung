package sibarum.elektro.queue.example;

import sibarum.intrabuf.Compat;
import sibarum.intrabuf.annotation.Field;
import sibarum.intrabuf.annotation.IntrabufMessage;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * An intrabuf message riding elektro-Q: types elektro-Q's own codegen does not support (collections, maps,
 * {@code Optional}), BACKWARD compatibility, and an id from the same space as the {@code @Message} records.
 * Registered with {@code MessageType.of(TelemetryIntrabuf.CODEC, Telemetry.class)}.
 */
@IntrabufMessage(name = "example.Telemetry", id = 100, compat = Compat.BACKWARD)
public record Telemetry(
        @Field(nonNull = true) String node,
        @Field(nonNull = true) List<Double> samples,
        @Field(nonNull = true) Map<String, Long> counters,
        Optional<String> note) {
}
