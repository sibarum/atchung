package sibarum.elektro.queue;

import sibarum.elektro.queue.message.Flags;
import sibarum.elektro.queue.message.HeaderCodec;
import sibarum.elektro.queue.message.MessageHeader;
import sibarum.elektro.queue.message.MessageRegistry;
import sibarum.elektro.queue.message.MessageType;
import sibarum.elektro.queue.message.PeerId;
import sibarum.elektro.queue.message.Protocol;
import sibarum.elektro.queue.transport.FrameListener;
import sibarum.elektro.queue.transport.PeerListener;
import sibarum.elektro.queue.transport.Transport;
import sibarum.elektro.queue.wire.WireBufferReader;
import sibarum.elektro.queue.wire.WireBufferWriter;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Default {@link Conduit}: binds a {@link Transport} to a {@link MessageRegistry} and
 * the generated codecs, turning opaque frames into typed emit/react/request-reply.
 *
 * <p>It is transport-agnostic &mdash; it speaks only the {@link Transport} SPI &mdash;
 * so the same conduit drives TCP today and any future medium unchanged. Outbound, it
 * encodes a payload, prepends a {@link MessageHeader}, and hands the frame to the
 * transport. Inbound, it decodes the header, looks the type up by id, decodes the
 * payload, and either completes a pending {@code request} (on a {@code REPLY}) or
 * dispatches to the {@link Actor}s subscribed to that type.
 *
 * <p>Actors are invoked on the transport's receive thread, which for the bundled TCP
 * transport is a per-connection virtual thread; this preserves per-peer ordering.
 * Actors must therefore not block indefinitely.
 */
public final class DefaultConduit implements Conduit, FrameListener, PeerListener {

    private final String name;
    private final Transport transport;
    private final MessageRegistry registry;

    private final AtomicReference<ConduitState> state = new AtomicReference<>(ConduitState.NEW);
    private final Set<PeerId> peers = ConcurrentHashMap.newKeySet();
    private final Map<Integer, List<Actor<?>>> actors = new ConcurrentHashMap<>();
    private final Map<Long, Pending> pending = new ConcurrentHashMap<>();
    private final AtomicLong correlation = new AtomicLong(1);
    private final Duration requestTimeout;

    /** Creates a conduit whose requests never time out on their own. */
    public DefaultConduit(String name, Transport transport, MessageRegistry registry) {
        this(name, transport, registry, null);
    }

    /**
     * Creates a conduit that fails any outstanding {@code request} after
     * {@code requestTimeout} elapses without a reply. Pass {@code null} for no timeout.
     */
    public DefaultConduit(String name, Transport transport, MessageRegistry registry, Duration requestTimeout) {
        this.name = Objects.requireNonNull(name, "name");
        this.transport = Objects.requireNonNull(transport, "transport");
        this.registry = Objects.requireNonNull(registry, "registry");
        if (requestTimeout != null && (requestTimeout.isZero() || requestTimeout.isNegative())) {
            throw new IllegalArgumentException("requestTimeout must be positive, was " + requestTimeout);
        }
        this.requestTimeout = requestTimeout;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public ConduitState state() {
        return state.get();
    }

    @Override
    public MessageRegistry registry() {
        return registry;
    }

    @Override
    public CompletionStage<Void> start() {
        if (!state.compareAndSet(ConduitState.NEW, ConduitState.STARTING)) {
            throw new ElektroException("Conduit '" + name + "' already started");
        }
        transport.onFrame(this);
        transport.onPeer(this);
        return transport.start().handle((ignored, error) -> {
            if (error != null) {
                state.set(ConduitState.FAILED);
                throw new CompletionException(new ElektroException("Conduit '" + name + "' failed to start", error));
            }
            state.set(ConduitState.OPEN);
            return null;
        });
    }

    @Override
    public <T> Action<T> action(MessageType<T> type) {
        ensureUsable();
        return new EmittingAction<>(this, type);
    }

    @Override
    public <T> Subscription subscribe(MessageType<T> type, Actor<T> actor) {
        ensureUsable();
        actors.computeIfAbsent(type.id(), k -> new CopyOnWriteArrayList<>()).add(actor);
        return new ActorSubscription(type, actor);
    }

    @Override
    public Set<PeerId> peers() {
        return Set.copyOf(peers);
    }

    @Override
    public void close() {
        ConduitState prev = state.getAndSet(ConduitState.CLOSING);
        if (prev == ConduitState.CLOSED || prev == ConduitState.CLOSING) {
            return;
        }
        try {
            transport.close();
        } finally {
            actors.clear();
            peers.clear();
            ElektroException closed = new ElektroException("Conduit '" + name + "' closed");
            pending.values().forEach(waiter -> waiter.future().completeExceptionally(closed));
            pending.clear();
            state.set(ConduitState.CLOSED);
        }
    }

    // --- transport callbacks ----------------------------------------------------

    @Override
    public void onFrame(PeerId source, ByteBuffer frame) {
        WireBufferReader reader = WireBufferReader.of(frame);
        MessageHeader header = HeaderCodec.INSTANCE.decode(reader);
        MessageType<?> type = registry.byId(header.typeId());
        if (type == null) {
            return; // unknown type id: drop rather than guess
        }
        Object payload = type.codec().decode(reader);

        if (header.isReply()) {
            Pending waiter = pending.remove(header.correlationId());
            if (waiter != null) {
                waiter.future().complete(payload);
            }
            return;
        }

        List<Actor<?>> subscribers = actors.get(header.typeId());
        if (subscribers == null || subscribers.isEmpty()) {
            return;
        }
        MessageContext context = new IncomingContext(this, source, header, type);
        for (Actor<?> actor : subscribers) {
            @SuppressWarnings("unchecked")
            Actor<Object> typed = (Actor<Object>) actor;
            typed.react(payload, context);
        }
    }

    @Override
    public void onConnected(PeerId peer) {
        peers.add(peer);
    }

    @Override
    public void onDisconnected(PeerId peer) {
        peers.remove(peer);
        ElektroException cause =
                new ElektroException("Peer " + peer.handle() + " disconnected before replying");
        pending.forEach((id, waiter) -> {
            if (waiter.destination().equals(peer)) {
                waiter.future().completeExceptionally(cause);
            }
        });
    }

    // --- emit / request internals -----------------------------------------------

    <T> void sendFrame(PeerId destination, MessageType<T> type, T message, byte flags, long correlationId) {
        WireBufferWriter payloadWriter = new WireBufferWriter();
        type.codec().encode(message, payloadWriter);
        byte[] payload = payloadWriter.toByteArray();

        MessageHeader header = new MessageHeader(
                type.id(), type.schemaVersion(), flags, correlationId, payload.length);

        WireBufferWriter frameWriter = new WireBufferWriter(Protocol.HEADER_BYTES + payload.length);
        HeaderCodec.INSTANCE.encode(header, frameWriter);
        frameWriter.putBytes(payload);
        transport.send(destination, frameWriter.toByteBuffer());
    }

    <T, R> CompletionStage<R> request(MessageType<T> type, T message, PeerId destination, MessageType<R> replyType) {
        long id = correlation.getAndIncrement();
        CompletableFuture<Object> future = new CompletableFuture<>();
        pending.put(id, new Pending(destination, future));
        // Drop the pending entry on any completion: reply, timeout, disconnect, or send failure.
        future.whenComplete((result, error) -> pending.remove(id));
        if (requestTimeout != null) {
            future.orTimeout(requestTimeout.toNanos(), TimeUnit.NANOSECONDS);
        }
        try {
            sendFrame(destination, type, message, Flags.REQUEST, id);
        } catch (RuntimeException e) {
            future.completeExceptionally(e);
        }
        @SuppressWarnings("unchecked")
        CompletionStage<R> typed = (CompletionStage<R>) (CompletionStage<?>) future;
        return typed;
    }

    private void ensureUsable() {
        ConduitState current = state.get();
        if (current == ConduitState.CLOSING || current == ConduitState.CLOSED || current == ConduitState.FAILED) {
            throw new ElektroException("Conduit '" + name + "' is " + current);
        }
    }

    // --- supporting types -------------------------------------------------------

    /** An in-flight request: the peer it went to, and the future awaiting its reply. */
    private record Pending(PeerId destination, CompletableFuture<Object> future) {
    }

    private record EmittingAction<T>(DefaultConduit conduit, MessageType<T> type) implements Action<T> {

        @Override
        public void emit(T message) {
            conduit.sendFrame(PeerId.BROADCAST, type, message, Flags.NONE, 0L);
        }

        @Override
        public void emit(T message, PeerId destination) {
            conduit.sendFrame(destination, type, message, Flags.NONE, 0L);
        }

        @Override
        public <R> CompletionStage<R> request(T message, PeerId destination, MessageType<R> replyType) {
            return conduit.request(type, message, destination, replyType);
        }
    }

    private record IncomingContext(DefaultConduit conduit, PeerId source, MessageHeader header, MessageType<?> type)
            implements MessageContext {

        @Override
        public long correlationId() {
            return header.correlationId();
        }

        @Override
        public boolean isRequest() {
            return header.isRequest();
        }

        @Override
        public <R> void reply(MessageType<R> replyType, R reply) {
            if (!header.isRequest()) {
                throw new ElektroException("Cannot reply to a message that is not a request");
            }
            conduit.sendFrame(source, replyType, reply, Flags.REPLY, header.correlationId());
        }
    }

    private final class ActorSubscription implements Subscription {

        private final MessageType<?> type;
        private final Actor<?> actor;
        private volatile boolean active = true;

        ActorSubscription(MessageType<?> type, Actor<?> actor) {
            this.type = type;
            this.actor = actor;
        }

        @Override
        public MessageType<?> type() {
            return type;
        }

        @Override
        public boolean isActive() {
            return active;
        }

        @Override
        public void close() {
            if (!active) {
                return;
            }
            active = false;
            List<Actor<?>> subscribers = actors.get(type.id());
            if (subscribers != null) {
                subscribers.remove(actor);
            }
        }
    }
}
