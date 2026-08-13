package sibarum.atchung.elektroq;

import sibarum.atchung.Atchung;
import sibarum.atchung.Topic;
import sibarum.elektro.queue.Action;
import sibarum.elektro.queue.Conduit;
import sibarum.elektro.queue.message.MessageType;
import sibarum.elektro.queue.message.PeerId;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Connects an in-VM Atchung! {@link Atchung} bus to an elektro-Q {@link Conduit}, so an event
 * published on a {@link Topic} can cross the wire and an event arriving from a peer is re-published
 * onto the local bus. This is the seam that makes Atchung!'s local fabric network-transparent
 * <em>opt-in</em>: the core bus never pays for it, and only the topics you bridge leave the process.
 *
 * <p><b>Author events once.</b> A bridged topic's payload type <em>is</em> its elektro-Q message
 * type &mdash; declare the event as a {@code @Message} record, build a {@link Topic} of it for local
 * fan-out and use the generated {@link MessageType} for the wire. The bridge asserts the two agree.
 *
 * <p><b>Directions.</b> {@link #bridge} wires both ways; {@link #outbound} forwards local publishes
 * onto the wire; {@link #inbound} re-publishes wire messages locally. Outbound uses <b>inline</b>
 * delivery (the publisher's thread) so forwarding adds no hand-off latency.
 *
 * <p><b>Loop prevention.</b> Re-publishing an inbound message would otherwise be seen by the
 * outbound subscriber and echoed straight back. The bridge suppresses exactly that: while it
 * re-publishes topic {@code A}, an outbound emit <em>of {@code A} on the same thread</em> is
 * dropped. Cascades to <em>other</em> bridged topics still flow out — only the immediate echo is
 * cut. A bridge therefore does not relay: a message from one peer is delivered locally, not fanned
 * back to the others.
 *
 * <p>Not thread-safe to configure concurrently; wire up all topics before traffic flows. Once
 * configured, delivery in both directions is safe under concurrent publishing.
 */
public final class ElektroBridge implements AutoCloseable {

    private final Atchung bus;
    private final Conduit conduit;
    private final CopyOnWriteArrayList<Runnable> closers = new CopyOnWriteArrayList<>();

    /**
     * Topics currently being re-published from the wire on this thread. Scoped per-topic (not a
     * plain flag) so a re-publish of {@code A} cannot suppress a legitimate outbound emit of {@code B}
     * triggered by a local reaction to {@code A}.
     */
    private final ThreadLocal<Deque<Topic<?>>> republishing = ThreadLocal.withInitial(ArrayDeque::new);

    public ElektroBridge(Atchung bus, Conduit conduit) {
        this.bus = Objects.requireNonNull(bus, "bus");
        this.conduit = Objects.requireNonNull(conduit, "conduit");
    }

    /** Bridge {@code topic} both ways: local publishes go out (broadcast), wire messages come in. */
    public <T> ElektroBridge bridge(Topic<T> topic, MessageType<T> type) {
        outbound(topic, type, PeerId.BROADCAST);
        inbound(topic, type);
        return this;
    }

    /** Forward local publishes of {@code topic} onto the wire, broadcast to every connected peer. */
    public <T> ElektroBridge outbound(Topic<T> topic, MessageType<T> type) {
        return outbound(topic, type, PeerId.BROADCAST);
    }

    /** Forward local publishes of {@code topic} onto the wire, sent to {@code destination}. */
    public <T> ElektroBridge outbound(Topic<T> topic, MessageType<T> type, PeerId destination) {
        requireSameType(topic, type);
        Objects.requireNonNull(destination, "destination");
        Action<T> action = conduit.action(type);
        var sub = bus.subscribe(topic, event -> {
            if (republishing.get().contains(topic)) {
                return; // this event just arrived from the wire — don't send it straight back
            }
            action.emit(event, destination);
        });
        closers.add(sub::close);
        return this;
    }

    /** Re-publish wire messages of {@code type} onto the local bus under {@code topic}. */
    public <T> ElektroBridge inbound(Topic<T> topic, MessageType<T> type) {
        requireSameType(topic, type);
        var sub = conduit.subscribe(type, (message, ctx) -> {
            Deque<Topic<?>> stack = republishing.get();
            stack.push(topic);
            try {
                bus.publish(topic, message);
            } finally {
                stack.pop();
            }
        });
        closers.add(sub::close);
        return this;
    }

    /** Detach every bridged direction. Idempotent; does not close the bus or the conduit. */
    @Override
    public void close() {
        for (Runnable closer : closers) {
            closer.run();
        }
        closers.clear();
    }

    private static void requireSameType(Topic<?> topic, MessageType<?> type) {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(type, "type");
        if (!topic.payloadType().equals(type.javaType())) {
            throw new IllegalArgumentException("topic payload " + topic.payloadType().getName()
                    + " does not match message type " + type.javaType().getName());
        }
    }
}
