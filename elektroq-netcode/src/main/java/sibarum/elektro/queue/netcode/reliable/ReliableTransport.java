package sibarum.elektro.queue.netcode.reliable;

import sibarum.elektro.queue.ElektroException;
import sibarum.elektro.queue.message.PeerId;
import sibarum.elektro.queue.netcode.channel.ChannelReceiver;
import sibarum.elektro.queue.netcode.channel.ChannelSender;
import sibarum.elektro.queue.netcode.channel.DeliveryMode;
import sibarum.elektro.queue.netcode.channel.MessageCodec;
import sibarum.elektro.queue.netcode.channel.NetMessage;
import sibarum.elektro.queue.transport.FrameListener;
import sibarum.elektro.queue.transport.PeerListener;
import sibarum.elektro.queue.transport.Transport;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A connection + reliability layer over a raw datagram {@link Transport} (docs/netcode-design.md,
 * layers 1&ndash;2). It turns elektro-Q's connectionless UDP into a session with liveness and
 * measured quality, while staying a {@code Transport} itself &mdash; so a {@code DefaultConduit}
 * sits on top of it exactly as it does on TCP.
 *
 * <p>It adds three things over the raw transport:
 * <ul>
 *   <li><b>A handshake.</b> A client sends {@code CONNECT}; the server replies {@code ACCEPT}. Only
 *       after that does a peer surface upward via {@link PeerListener#onConnected}. The client
 *       retransmits {@code CONNECT} until accepted, so the handshake survives loss.</li>
 *   <li><b>Liveness.</b> A periodic tick sends a {@code KEEPALIVE} when idle (which also carries
 *       fresh acknowledgements), and a peer that goes silent past the timeout is torn down with
 *       {@link PeerListener#onDisconnected} &mdash; the disconnect signal raw UDP lacks.</li>
 *   <li><b>Sequencing + acks.</b> Every packet is stamped by a per-peer {@link ReliableEndpoint},
 *       so {@link #rttMillis} and {@link #packetLoss} are live per connection.</li>
 * </ul>
 *
 * <p>This layer does not yet reorder or retransmit application data (that is the channels layer);
 * it establishes the session and the ack/RTT substrate those guarantees are built on. Peer identity
 * is the underlying transport's {@link PeerId}; NAT-rebinding connection ids come later.
 */
public final class ReliableTransport implements Transport {

    /** Protocol id stamped in every packet; rejects foreign or mismatched datagrams. */
    public static final int PROTOCOL_ID = 0x454C4B51; // "ELKQ"

    private static final byte[] EMPTY = new byte[0];
    private static final long DEFAULT_KEEPALIVE_MILLIS = 150;
    private static final long DEFAULT_TIMEOUT_MILLIS = 5_000;

    /** Channel used by the plain {@link Transport#send} path (reliable-ordered, TCP-like default). */
    private static final int DEFAULT_CHANNEL = 0;
    /** Byte budget for a single packet's message container, kept well under a typical MTU. */
    private static final int PACKET_BUDGET = 1100;

    /** Channel-aware receive callback. When set, it replaces the plain {@link FrameListener}. */
    public interface NetcodeListener {
        void onMessage(PeerId peer, int channelId, byte[] payload);
    }

    private static final PeerListener NO_PEER_LISTENER = new PeerListener() {
        @Override public void onConnected(PeerId peer) { }
        @Override public void onDisconnected(PeerId peer) { }
    };

    /** Which side drives the handshake. */
    public enum Role { CLIENT, SERVER }

    private final Transport delegate;
    private final Role role;
    private final long keepaliveNanos;
    private final long timeoutNanos;

    private final ConcurrentHashMap<Long, Session> sessions = new ConcurrentHashMap<>();
    private final ScheduledExecutorService ticker =
            Executors.newSingleThreadScheduledExecutor(r -> Thread.ofVirtual().unstarted(r));

    private volatile FrameListener frameListener = (source, frame) -> { };
    private volatile PeerListener peerListener = NO_PEER_LISTENER;
    private volatile NetcodeListener netcodeListener;
    private volatile boolean closed;

    private ReliableTransport(Transport delegate, Role role, long keepaliveMillis, long timeoutMillis) {
        this.delegate = delegate;
        this.role = role;
        this.keepaliveNanos = keepaliveMillis * 1_000_000L;
        this.timeoutNanos = timeoutMillis * 1_000_000L;
    }

    /** A client (handshake initiator) over {@code delegate}, with default keepalive/timeout. */
    public static ReliableTransport client(Transport delegate) {
        return new ReliableTransport(delegate, Role.CLIENT, DEFAULT_KEEPALIVE_MILLIS, DEFAULT_TIMEOUT_MILLIS);
    }

    /** A server (handshake responder) over {@code delegate}, with default keepalive/timeout. */
    public static ReliableTransport server(Transport delegate) {
        return new ReliableTransport(delegate, Role.SERVER, DEFAULT_KEEPALIVE_MILLIS, DEFAULT_TIMEOUT_MILLIS);
    }

    /** A transport with explicit role and timing (for tests / tuning). */
    public static ReliableTransport of(Transport delegate, Role role, long keepaliveMillis, long timeoutMillis) {
        return new ReliableTransport(delegate, role, keepaliveMillis, timeoutMillis);
    }

    /** Smoothed round-trip time to {@code peer} in ms, or 0 if unknown. */
    public int rttMillis(PeerId peer) {
        Session s = sessions.get(peer.handle());
        return s == null ? 0 : s.endpoint.rttMillis();
    }

    /** Smoothed packet-loss fraction to {@code peer} in [0,1], or 0 if unknown. */
    public double packetLoss(PeerId peer) {
        Session s = sessions.get(peer.handle());
        return s == null ? 0 : s.endpoint.packetLoss();
    }

    @Override
    public CompletionStage<Void> start() {
        delegate.onFrame(this::onRawFrame);
        delegate.onPeer(new PeerListener() {
            @Override public void onConnected(PeerId peer) { onRawPeerConnected(peer); }
            @Override public void onDisconnected(PeerId peer) { teardown(peer, true); }
        });
        CompletionStage<Void> started = delegate.start();
        long period = keepaliveNanos / 1_000_000L;
        ticker.scheduleAtFixedRate(this::tick, period, period, TimeUnit.MILLISECONDS);
        return started;
    }

    /** Plain SPI send: reliable-ordered on the default channel (TCP-like), so a conduit rides it unchanged. */
    @Override
    public void send(PeerId destination, ByteBuffer frame) {
        send(destination, DEFAULT_CHANNEL, DeliveryMode.RELIABLE_ORDERED, frame);
    }

    /**
     * Sends {@code frame} on {@code channelId} with the given {@code mode}. Independent channels do
     * not block one another; the mode picks the delivery guarantee (docs/netcode-design.md).
     */
    public void send(PeerId destination, int channelId, DeliveryMode mode, ByteBuffer frame) {
        byte[] payload = drain(frame);
        if (destination == null || destination.isBroadcast()) {
            for (Session s : sessions.values()) {
                if (s.established) {
                    enqueue(s, channelId, mode, payload);
                    flush(s);
                }
            }
        } else {
            Session s = sessions.get(destination.handle());
            if (s == null || !s.established) {
                throw new ElektroException("No established peer with id " + destination.handle());
            }
            enqueue(s, channelId, mode, payload);
            flush(s);
        }
    }

    /** Installs a channel-aware receive callback; when set it replaces the plain frame listener. */
    public void onNetcodeMessage(NetcodeListener listener) {
        this.netcodeListener = listener;
    }

    @Override
    public void onFrame(FrameListener listener) {
        this.frameListener = listener != null ? listener : (source, frame) -> { };
    }

    @Override
    public void onPeer(PeerListener listener) {
        this.peerListener = listener != null ? listener : NO_PEER_LISTENER;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (Session s : sessions.values()) {
            if (s.established) {
                try {
                    sendPacket(s, PacketType.DISCONNECT, EMPTY);
                } catch (RuntimeException ignored) {
                    // best-effort teardown notice
                }
            }
        }
        ticker.shutdownNow();
        sessions.clear();
        delegate.close();
    }

    // --- internals --------------------------------------------------------------

    private void onRawPeerConnected(PeerId peer) {
        // The client learns its server the moment the raw transport reports it; kick off the
        // handshake. The server waits for the CONNECT packet instead (handled in onRawFrame).
        if (role == Role.CLIENT) {
            Session s = sessions.computeIfAbsent(peer.handle(), h -> new Session(peer));
            sendPacket(s, PacketType.CONNECT, EMPTY);
        }
    }

    private void onRawFrame(PeerId source, ByteBuffer buffer) {
        byte[] bytes = drain(buffer);
        Packet packet = PacketCodec.decode(bytes, PROTOCOL_ID);
        if (packet == null) {
            return; // not ours: foreign or corrupt datagram, dropped before creating any state
        }
        Session s = sessions.computeIfAbsent(source.handle(), h -> new Session(source));
        synchronized (s) {
            s.endpoint.process(bytes); // absorb acks / update RTT + loss
            s.lastReceivedNanos = System.nanoTime();
        }

        switch (packet.type()) {
            case PacketType.CONNECT -> {
                if (role == Role.SERVER) {
                    boolean first = !s.established;
                    s.established = true;
                    sendPacket(s, PacketType.ACCEPT, EMPTY); // (re)confirm; client may have retried
                    if (first) {
                        peerListener.onConnected(source);
                    }
                }
            }
            case PacketType.ACCEPT -> {
                if (role == Role.CLIENT && !s.established) {
                    s.established = true;
                    peerListener.onConnected(source);
                }
            }
            case PacketType.DISCONNECT -> teardown(source, true);
            case PacketType.KEEPALIVE -> { /* acks already absorbed above */ }
            case PacketType.DATA -> {
                if (s.established) {
                    List<NetMessage> delivered = s.receiver.accept(MessageCodec.decode(packet.payload()));
                    NetcodeListener nc = netcodeListener;
                    for (NetMessage m : delivered) {
                        if (nc != null) {
                            nc.onMessage(source, m.channelId(), m.payload());
                        } else {
                            frameListener.onFrame(source, ByteBuffer.wrap(m.payload()));
                        }
                    }
                }
            }
            default -> { /* unknown type: ignore */ }
        }
    }

    private void tick() {
        long now = System.nanoTime();
        for (Session s : sessions.values()) {
            if (now - s.lastReceivedNanos > timeoutNanos) {
                teardown(s.peer, true); // gone silent past the timeout
            } else if (!s.established && role == Role.CLIENT) {
                if (now - s.lastSentNanos > keepaliveNanos) {
                    sendPacket(s, PacketType.CONNECT, EMPTY); // retransmit handshake until accepted
                }
            } else if (s.established) {
                flush(s); // (re)send any queued or overdue reliable messages
                if (System.nanoTime() - s.lastSentNanos > keepaliveNanos) {
                    sendPacket(s, PacketType.KEEPALIVE, EMPTY); // stay alive + carry fresh acks
                }
            }
        }
    }

    private void enqueue(Session s, int channelId, DeliveryMode mode, byte[] payload) {
        synchronized (s) {
            s.sender.enqueue(channelId, mode, payload);
        }
    }

    /** Packs one packet's worth of due messages (new + overdue reliable) and sends it, if any. */
    private void flush(Session s) {
        byte[] datagram = null;
        synchronized (s) {
            if (!s.established) {
                return;
            }
            long now = System.nanoTime();
            int seq = s.endpoint.peekNextSequence();
            List<NetMessage> messages = s.sender.pack(seq, now, rtoNanos(s), PACKET_BUDGET);
            if (!messages.isEmpty()) {
                datagram = s.endpoint.stamp(PacketType.DATA, MessageCodec.encode(messages));
                s.lastSentNanos = now;
            }
        }
        if (datagram != null) {
            delegate.send(s.peer, ByteBuffer.wrap(datagram));
        }
    }

    private long rtoNanos(Session s) {
        long rttMillis = s.endpoint.rttMillis();
        return Math.max(100, rttMillis * 2) * 1_000_000L;
    }

    private void sendPacket(Session s, int type, byte[] payload) {
        byte[] datagram;
        synchronized (s) {
            datagram = s.endpoint.stamp(type, payload);
            s.lastSentNanos = System.nanoTime();
        }
        delegate.send(s.peer, ByteBuffer.wrap(datagram));
    }

    private void teardown(PeerId peer, boolean notify) {
        Session s = sessions.remove(peer.handle());
        if (s != null && s.established && notify) {
            peerListener.onDisconnected(peer);
        }
    }

    private static byte[] drain(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    /** Per-peer connection state: its endpoint, channel send/receive state, and liveness timestamps. */
    private final class Session {
        final PeerId peer;
        final ReliableEndpoint endpoint = new ReliableEndpoint(PROTOCOL_ID);
        final ChannelSender sender = new ChannelSender();
        final ChannelReceiver receiver = new ChannelReceiver();
        volatile boolean established;
        volatile long lastReceivedNanos = System.nanoTime();
        volatile long lastSentNanos = 0;

        Session(PeerId peer) {
            this.peer = peer;
            // A packet ack retires the reliable messages it carried (invoked under this session's
            // lock, from endpoint.process in onRawFrame).
            this.endpoint.setPacketListener(sender::acked);
        }
    }
}
