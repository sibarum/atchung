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
 * <p>The kind is not part of equality, so two declarations of one channel stay one channel. Only a {@link Tick} reads
 * the kind, and an {@link Inbox} refuses to subscribe to a channel that another inbox already hears as the other kind.
 * Everything that existed before ticks ignores it: a {@link Pump}, an inline and an async subscription choose their
 * policy per subscription, exactly as they always have, and subscribing one never fails over a kind.
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
    /** What a scoped subscription filters by ({@link #scoped}), or null for a topic with no scope. */
    private final Key<T> scope;

    private Topic(String name, Class<T> payloadType, Kind kind, Key<T> key, Key<T> scope) {
        this.name = Objects.requireNonNull(name, "name");
        this.payloadType = Objects.requireNonNull(payloadType, "payloadType");
        this.kind = kind;
        this.key = key;
        this.scope = scope;
    }

    /** An {@link Kind#EDGE edge} topic: what most channels are, and the kind that loses nothing. */
    public static <T> Topic<T> of(String name, Class<T> payloadType) {
        return edge(name, payloadType);
    }

    /** An {@link Kind#EDGE edge} topic: every message is delivered, in order. */
    public static <T> Topic<T> edge(String name, Class<T> payloadType) {
        return new Topic<>(name, payloadType, Kind.EDGE, null, null);
    }

    /** A {@link Kind#STATE state} topic whose messages each write the piece of state {@code key} names. */
    public static <T> Topic<T> state(String name, Class<T> payloadType, Key<T> key) {
        return new Topic<>(name, payloadType, Kind.STATE, Objects.requireNonNull(key, "key"), null);
    }

    /** A {@link Kind#STATE state} topic that is one piece of state as a whole: only its latest message matters. */
    public static <T> Topic<T> state(String name, Class<T> payloadType) {
        return new Topic<>(name, payloadType, Kind.STATE, e -> WHOLE, null);
    }

    /**
     * An {@link Kind#EDGE edge} topic named by its payload: the channel <em>is</em> the type, so it is declared once,
     * beside the type, and found from it. Its name is the payload's canonical name
     * ({@code dev.example.Messages.Open}), which no two channels share, so a spelling can never name the wrong one.
     */
    public static <T> Topic<T> edge(Class<T> payloadType) {
        return edge(nameOf(payloadType), payloadType);
    }

    /**
     * An {@link Kind#EDGE edge} topic whose messages each name a {@code key} to be filtered by, and nothing more: every
     * message is still delivered, in order, and none folds with another. What a subscription that hears only some
     * keys ({@link Inbox#subscribe(Topic, Subscriber, int, java.util.Set)}) is offered, so a message it would discard
     * is never queued for it: the widgets of one window, on a bus every window shares, hear only their own window's.
     */
    public static <T> Topic<T> edge(String name, Class<T> payloadType, Key<T> key) {
        Objects.requireNonNull(key, "key");
        return new Topic<>(name, payloadType, Kind.EDGE, key, key);
    }

    /** {@link #edge(String, Class, Key)}, named by its payload as {@link #edge(Class)} is. */
    public static <T> Topic<T> edge(Class<T> payloadType, Key<T> key) {
        return edge(nameOf(payloadType), payloadType, key);
    }

    /** A {@link Kind#STATE state} topic named by its payload, as {@link #edge(Class)} is, whose messages each write {@code key}. */
    public static <T> Topic<T> state(Class<T> payloadType, Key<T> key) {
        return state(nameOf(payloadType), payloadType, key);
    }

    /** A {@link Kind#STATE state} topic named by its payload, as {@link #edge(Class)} is, that is one piece of state as a whole. */
    public static <T> Topic<T> state(Class<T> payloadType) {
        return state(nameOf(payloadType), payloadType);
    }

    /** The name a topic named by its payload carries: the canonical name, or the binary name of a type with none. */
    private static String nameOf(Class<?> payloadType) {
        Objects.requireNonNull(payloadType, "payloadType");
        String canonical = payloadType.getCanonicalName();
        return canonical != null ? canonical : payloadType.getName();
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
        if (kind == Kind.EDGE || key == null) {
            return null;
        }
        Object k = key.of(event);
        if (k == null) {
            throw new IllegalStateException("state topic '" + name + "' has no key for " + event
                    + ": a state message must name the piece of state it writes");
        }
        return k;
    }

    /**
     * What a subscription's key filter reads {@code event} by: the piece of state it writes on a state topic, the key
     * an edge topic declared ({@link #edge(Class, Key)}), or null on an edge that declared none, which no filter
     * passes.
     */
    Object filterKeyOf(T event) {
        return key == null ? null : key.of(event);
    }

    /**
     * The key {@code event} names, as a key filter reads it ({@link #filterKeyOf}): the piece of state on a state topic,
     * the declared key on a keyed edge, and null on an edge that declared none. For whoever keeps something per key
     * outside an inbox, as a framework's source keeps one run per key of its requests.
     */
    public Object keyFor(T event) {
        return filterKeyOf(event);
    }

    /**
     * Whether a subscription may filter this channel by key: every state topic, and an edge that declared a key.
     */
    public boolean keyed() {
        return key != null;
    }

    /**
     * This topic, with messages that also name a {@code scope}: which part of the application a message is about,
     * apart from the piece of state it writes. An inbox that hears only some scopes
     * ({@link Inbox#subscribeIn}) is never offered a message from another, so one that is not run for a while (a
     * closed window's) holds nothing for it. An edge's declared key ({@link #edge(Class, Key)}) is already its scope; a
     * state topic, whose key is the piece of state, declares one here. Same channel: equality ignores it, as it
     * ignores the kind.
     */
    public Topic<T> scoped(Key<T> scope) {
        return new Topic<>(name, payloadType, kind, key, Objects.requireNonNull(scope, "scope"));
    }

    /** Whether this topic names a scope for each message: see {@link #scoped}. */
    public boolean hasScope() {
        return scope != null;
    }

    /** The scope {@code event} is in, or null on a topic with none, which no scope filter passes. */
    Object scopeOf(T event) {
        return scope == null ? null : scope.of(event);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Topic<?> t && name.equals(t.name) && payloadType.equals(t.payloadType);
    }

    @Override
    public int hashCode() {
        return 31 * name.hashCode() + payloadType.hashCode();
    }

    /**
     * As the record this class was until 1.4 printed itself, {@code Topic[name=..., payloadType=...]}, so a log or a
     * test that reads it reads the same thing. A state topic, which did not exist before, adds its kind at the end.
     */
    @Override
    public String toString() {
        return "Topic[name=" + name + ", payloadType=" + payloadType + (kind == Kind.STATE ? ", kind=STATE]" : "]");
    }
}
