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
 * <h2>State or edge</h2>
 *
 * <p>A topic also says which of two loss classes it carries, because every subscriber has to agree on it.
 * {@link Backpressure}'s javadoc gives the reason: a channel carrying both <em>"cannot be given a correct policy"</em>.
 *
 * <ul>
 *   <li>An <b>edge</b> ({@link #edge}, and {@link #of}) is something that happened: a keystroke, a command, the
 *       answer to a question. A {@link Tick} delivers every one, in order, and never drops one.</li>
 *   <li>A <b>state</b> topic ({@link #state}) is what is true now: a title, a flag, a progress value. Each message
 *       names a {@link Key}, and a {@link Tick} delivers only the last write per key since its subscriber last
 *       ran — the condition {@link Fold} spells out holds by construction there, since nothing can read a
 *       component's state between two of its deliveries.</li>
 * </ul>
 *
 * <p>The kind is not part of equality. Two declarations of one channel that disagree about its kind are a bug,
 * and keeping them equal is what lets the bus see both and refuse the second ({@link Atchung} does, when the second
 * subscribes), rather than quietly treating them as two channels. A {@link Pump} and an inline or async
 * subscription ignore the kind, as they always have: they choose their policy per subscription.
 *
 * @param <T> the event type
 */
public final class Topic<T> {

    /** The loss class a topic carries. */
    public enum Kind {
        /** Something that happened: every message is delivered, in order. */
        EDGE,
        /** What is true now: within a tick, the last write per key wins. */
        STATE
    }

    /**
     * Which piece of state a message on a {@link Kind#STATE} topic writes: any value with sensible
     * {@code equals}/{@code hashCode}, never null. A topic that is one piece of state as a whole (a window's size)
     * names a constant.
     *
     * <p>Called on the thread that hands the message to a tick, once per message per subscriber: keep it cheap and
     * pure. A record accessor is the usual shape: {@code Topic.state("tab.title", Title.class, Title::id)}.
     */
    @FunctionalInterface
    public interface Key<T> {
        Object of(T event);
    }

    /** The key of a topic that is one piece of state as a whole. */
    private static final Object WHOLE = new Object() {
        @Override
        public String toString() {
            return "whole topic";
        }
    };

    private final String name;
    private final Class<T> payloadType;
    private final Kind kind;
    private final Key<T> key;

    private Topic(String name, Class<T> payloadType, Kind kind, Key<T> key) {
        this.name = Objects.requireNonNull(name, "name");
        this.payloadType = Objects.requireNonNull(payloadType, "payloadType");
        this.kind = kind;
        this.key = key;
    }

    /** An {@link Kind#EDGE edge} topic: what most channels are, and the kind that loses nothing. */
    public static <T> Topic<T> of(String name, Class<T> payloadType) {
        return edge(name, payloadType);
    }

    /** An {@link Kind#EDGE edge} topic: every message is delivered, in order. */
    public static <T> Topic<T> edge(String name, Class<T> payloadType) {
        return new Topic<>(name, payloadType, Kind.EDGE, null);
    }

    /** A {@link Kind#STATE state} topic whose messages each write the piece of state {@code key} names. */
    public static <T> Topic<T> state(String name, Class<T> payloadType, Key<T> key) {
        return new Topic<>(name, payloadType, Kind.STATE, Objects.requireNonNull(key, "key"));
    }

    /** A {@link Kind#STATE state} topic that is one piece of state as a whole: only its latest message matters. */
    public static <T> Topic<T> state(String name, Class<T> payloadType) {
        return new Topic<>(name, payloadType, Kind.STATE, e -> WHOLE);
    }

    /** The channel's stable name. */
    public String name() {
        return name;
    }

    /** The type of event this channel carries. */
    public Class<T> payloadType() {
        return payloadType;
    }

    /** Whether this channel carries state or edges. Not part of equality: see the class javadoc. */
    public Kind kind() {
        return kind;
    }

    /**
     * The piece of state {@code event} writes, on a {@link Kind#STATE} topic; null on an edge.
     *
     * @throws IllegalStateException when a state topic's key names nothing for {@code event}, because a write to no
     *                               piece of state cannot be folded with anything and is not a state message
     */
    Object keyOf(T event) {
        if (key == null) {
            return null;
        }
        Object k = key.of(event);
        if (k == null) {
            throw new IllegalStateException("state topic '" + name + "' has no key for " + event
                    + ": a state message must name the piece of state it writes");
        }
        return k;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Topic<?> t && name.equals(t.name) && payloadType.equals(t.payloadType);
    }

    @Override
    public int hashCode() {
        return 31 * name.hashCode() + payloadType.hashCode();
    }

    @Override
    public String toString() {
        return "Topic[" + name + ", " + payloadType.getName() + (kind == Kind.STATE ? ", state]" : "]");
    }
}
