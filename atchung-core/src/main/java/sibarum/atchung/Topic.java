package sibarum.atchung;

import java.util.Objects;

/**
 * A typed channel identity. Publishers and subscribers rendezvous on equal topics, so a topic is
 * both a routing key and the compile-time proof that an event matches its handlers.
 *
 * <p>Equality is by {@code name} + {@code payloadType}: {@code Topic.of("input", InputEvent.class)}
 * built in two places refer to the same channel. The {@code payloadType} is carried for routing
 * identity and diagnostics — Atchung never reflects over it.
 *
 * @param name        a stable channel name
 * @param payloadType the event type carried on this topic
 * @param <T>         the event type
 */
public record Topic<T>(String name, Class<T> payloadType) {

    public Topic {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(payloadType, "payloadType");
    }

    public static <T> Topic<T> of(String name, Class<T> payloadType) {
        return new Topic<>(name, payloadType);
    }
}
