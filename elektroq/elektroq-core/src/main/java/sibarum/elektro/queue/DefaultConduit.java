package sibarum.elektro.queue;

import sibarum.elektro.queue.message.Envelope;
import sibarum.elektro.queue.message.EnvelopeIntrabuf;
import sibarum.elektro.queue.message.Flags;
import sibarum.elektro.queue.message.IntrabufCodec;
import sibarum.elektro.queue.message.MessageRegistry;
import sibarum.elektro.queue.message.MessageType;
import sibarum.elektro.queue.message.PeerId;
import sibarum.elektro.queue.message.Protocol;
import sibarum.elektro.queue.transport.FrameListener;
import sibarum.elektro.queue.transport.PeerListener;
import sibarum.elektro.queue.transport.Transport;
import sibarum.elektro.queue.wire.WireBufferReader;
import sibarum.elektro.queue.wire.WireBufferWriter;
import sibarum.intrabuf.Compatibility;
import sibarum.intrabuf.Header;
import sibarum.intrabuf.HeaderCodec;
import sibarum.intrabuf.IbReader;
import sibarum.intrabuf.IbWriter;
import sibarum.intrabuf.IntrabufException;
import sibarum.probe.Cause;

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
 * writes an {@link Envelope} carrying the type's schema facts, then the payload, and
 * hands the frame to the transport. Inbound, it decodes the envelope, looks the type
 * up by id, has intrabuf check the envelope's facts against the type's schema, decodes
 * the payload, and either completes a pending {@code request} (on a {@code REPLY}) or
 * dispatches to the {@link Actor}s subscribed to that type.
 *
 * <p><b>Causes cross with the frames.</b> A frame is sent with the {@link Cause} its sender was in, and its actors run
 * inside that cause, as does whatever completes a request's future, so a chain that crosses processes stays one
 * chain. Only to a peer that has said it reads causes, by its hello at connect or by any frame of its own; anything
 * else may be a peer from before them (see {@link Envelope}). A frame that brings no cause leaves the receiving
 * thread in the one it was in.
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
    /** The peers that have said they read causes: only these are sent one. */
    private final Set<PeerId> causePeers = ConcurrentHashMap.newKeySet();
    private final Map<Integer, List<Actor<?>>> actors = new ConcurrentHashMap<>();
    private final Map<Long, Pending> pending = new ConcurrentHashMap<>();
    private final AtomicLong correlation = new AtomicLong(1);
    private final Duration requestTimeout;
    private final List<PeerListener> peerListeners = new CopyOnWriteArrayList<>();

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
    public void addPeerListener(PeerListener listener) {
        peerListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    @Override
    public void removePeerListener(PeerListener listener) {
        peerListeners.remove(listener);
    }

    /** Tell every peer listener, absorbing a fault in one so it neither refuses the peer nor silences the rest. */
    private void announce(PeerId peer, boolean connected) {
        for (PeerListener listener : peerListeners) {
            try {
                if (connected) {
                    listener.onConnected(peer);
                } else {
                    listener.onDisconnected(peer);
                }
            } catch (RuntimeException e) {
                // stderr, as the transports report: this module has no logger, and a fault nobody hears about is
                // the failure this reporting exists to prevent.
                System.err.println("[elektroq] conduit '" + name + "': a peer listener failed on peer "
                        + peer.handle() + (connected ? " connecting" : " disconnecting") + ": " + e);
            }
        }
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
        byte[] bytes;
        int offset;
        int length = frame.remaining();
        if (frame.hasArray()) {
            bytes = frame.array();
            offset = frame.arrayOffset() + frame.position();
        } else {
            bytes = new byte[length];
            frame.duplicate().get(bytes);
            offset = 0;
        }
        IbReader reader = new IbReader(bytes, offset, length);
        HeaderCodec.Frame<Envelope> envelopeFrame;
        try {
            envelopeFrame = EnvelopeIntrabuf.CODEC.readFrame(reader);
        } catch (IntrabufException e) {
            throw new ElektroException("Malformed frame envelope: " + e.getMessage(), e);
        }
        Envelope header = envelopeFrame.header();
        if (header.magic() != Protocol.MAGIC) {
            throw new ElektroException("Bad frame magic: 0x" + Integer.toHexString(header.magic() & 0xFFFF));
        }
        if ((header.protocolVersion() & 0xFF) != Protocol.VERSION) {
            throw new ElektroException("Unsupported protocol version: " + (header.protocolVersion() & 0xFF));
        }
        if (header.causes() && !causePeers.contains(source)) {
            causePeers.add(source);
        }
        if (header.typeId() == Protocol.HELLO_TYPE) {
            return;
        }
        if (header.cause() == null) {
            receive(source, header, envelopeFrame, reader, bytes, offset);
            return;
        }
        long previous = Cause.enter(header.cause());
        try {
            receive(source, header, envelopeFrame, reader, bytes, offset);
        } finally {
            Cause.exit(previous);
        }
    }

    /** Decodes and delivers a frame whose envelope has been read and checked, inside whatever cause it brought. */
    private void receive(PeerId source, Envelope header, HeaderCodec.Frame<Envelope> envelopeFrame, IbReader reader,
                         byte[] bytes, int offset) {
        MessageType<?> type = registry.byId(header.typeId());
        if (type == null) {
            return; // unknown type id: drop rather than guess
        }
        Object payload = decodePayload(type, envelopeFrame.facts(), reader, bytes, offset);

        if (header.reply()) {
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
        hello(peer);
        announce(peer, true);
    }

    /**
     * Tell a new peer this conduit reads causes, so it can send them before it has had anything else from here. A
     * failure is reported and absorbed: the peer is still connected, and learns the same from the next frame.
     */
    private void hello(PeerId peer) {
        try {
            IbWriter frame = new IbWriter(16);
            EnvelopeIntrabuf.CODEC.write(frame, Envelope.hello());
            transport.send(peer, ByteBuffer.wrap(frame.array(), 0, frame.position()));
        } catch (RuntimeException e) {
            System.err.println("[elektroq] conduit '" + name + "': the hello to peer " + peer.handle()
                    + " was not sent: " + e);
        }
    }

    @Override
    public void onDisconnected(PeerId peer) {
        peers.remove(peer);
        causePeers.remove(peer);
        ElektroException cause =
                new ElektroException("Peer " + peer.handle() + " disconnected before replying");
        pending.forEach((id, waiter) -> {
            if (waiter.destination().equals(peer)) {
                waiter.future().completeExceptionally(cause);
            }
        });
        announce(peer, false);
    }

    // --- emit / request internals -----------------------------------------------

    /**
     * Decodes a payload once the envelope's facts are in hand. An intrabuf-backed type is checked and decoded by
     * intrabuf, against whichever version of its schema the sender had. A {@code @Message} type is checked against
     * its synthesized schema (same id, major and mode) and then decoded by its own codec, which applies elektro-Q's
     * trailing-field rules.
     */
    private static Object decodePayload(MessageType<?> type, Header facts, IbReader reader, byte[] bytes, int offset) {
        try {
            if (type.codec() instanceof IntrabufCodec<?> intrabuf) {
                return intrabuf.messageCodec().decodeBody(facts, reader);
            }
            Compatibility.check(type.schema(), facts);
        } catch (IntrabufException e) {
            throw new ElektroException(type.name() + ": " + e.getMessage(), e);
        }
        int start = offset + reader.position();
        return type.codec().decode(new WireBufferReader(bytes, start, reader.remaining()));
    }

    <T> void sendFrame(PeerId destination, MessageType<T> type, T message, byte flags, long correlationId) {
        IbWriter frame = new IbWriter(64);
        long cause = Cause.current();
        if (cause != Cause.NONE && !readsCauses(destination)) {
            cause = Cause.NONE;
        }
        try {
            EnvelopeIntrabuf.CODEC.write(frame, Envelope.of(type.schema(), flags, correlationId, cause));
            if (type.codec() instanceof IntrabufCodec<T> intrabuf) {
                intrabuf.messageCodec().encodeBody(message, frame);
            } else {
                WireBufferWriter payload = new WireBufferWriter();
                type.codec().encode(message, payload);
                frame.writeRaw(payload.toByteArray());
            }
        } catch (IntrabufException e) {
            throw new ElektroException(type.name() + ": " + e.getMessage(), e);
        }
        transport.send(destination, ByteBuffer.wrap(frame.array(), 0, frame.position()));
    }

    /** Whether {@code destination} may be sent a cause: it has said it reads them, or for a broadcast, every peer has. */
    private boolean readsCauses(PeerId destination) {
        if (destination == null || destination.isBroadcast()) {
            return !peers.isEmpty() && causePeers.containsAll(peers);
        }
        return causePeers.contains(destination);
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

    private record IncomingContext(DefaultConduit conduit, PeerId source, Envelope header, MessageType<?> type)
            implements MessageContext {

        @Override
        public long correlationId() {
            return header.correlationId();
        }

        @Override
        public boolean isRequest() {
            return header.request();
        }

        @Override
        public <R> void reply(MessageType<R> replyType, R reply) {
            if (!header.request()) {
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
