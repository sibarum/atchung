package sibarum.elektro.queue.transport.udp;

import sibarum.elektro.queue.ElektroException;
import sibarum.elektro.queue.message.PeerId;
import sibarum.elektro.queue.transport.FrameListener;
import sibarum.elektro.queue.transport.PeerListener;
import sibarum.elektro.queue.transport.Transport;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A raw, unreliable UDP {@link Transport} &mdash; layer 0 of elektro-Q's netcode stack
 * (docs/netcode-design.md).
 *
 * <p>It moves each framed buffer as <b>one datagram</b>: UDP already delimits messages, so unlike the
 * TCP transport there is no length prefix. Delivery is best-effort &mdash; datagrams may be lost,
 * reordered, or duplicated, and a frame must fit the path MTU (larger messages await the
 * fragmentation layer). This layer adds <b>no</b> reliability, sequencing, or connection state; those
 * are higher layers built on top. On its own it is exactly "cross-network unreliable-unordered," the
 * cheapest useful transport and the base every other netcode guarantee is layered over.
 *
 * <p>Because UDP is connectionless, peer identity is synthesised: a {@link #connecting} transport
 * knows its single remote up front (a peer at {@link #start()}); a {@link #listening} transport
 * discovers a peer the first time a datagram arrives from a new address, assigning it a
 * {@link PeerId} and reporting {@link PeerListener#onConnected}. There is no disconnect signal at this
 * layer (that needs keepalive/timeout, a higher layer). One virtual thread runs the blocking receive
 * loop, so callbacks are serialised there, mirroring the TCP transport.
 */
public final class UdpTransport implements Transport {

    /** Max UDP payload; sized to hold any single datagram this transport could receive. */
    private static final int MAX_DATAGRAM_BYTES = 65_535;

    private static final PeerListener NO_PEER_LISTENER = new PeerListener() {
        @Override public void onConnected(PeerId peer) { }
        @Override public void onDisconnected(PeerId peer) { }
    };

    private enum Mode { LISTENING, CONNECTING }

    private final Mode mode;
    private final InetSocketAddress bindAddress;   // listening: where to bind; connecting: ephemeral
    private final InetSocketAddress remoteAddress;  // connecting only

    private final ConcurrentHashMap<SocketAddress, PeerId> peersByAddress = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, SocketAddress> addressByPeer = new ConcurrentHashMap<>();
    private final AtomicLong nextPeerId = new AtomicLong(1); // 0 is reserved for BROADCAST
    private final Object sendLock = new Object();

    // Phase-1 metrics: raw counters. RTT / loss arrive with the sequencing layer.
    private final AtomicLong packetsSent = new AtomicLong();
    private final AtomicLong bytesSent = new AtomicLong();
    private final AtomicLong packetsReceived = new AtomicLong();
    private final AtomicLong bytesReceived = new AtomicLong();

    private volatile FrameListener frameListener = (source, frame) -> { };
    private volatile PeerListener peerListener = NO_PEER_LISTENER;
    private volatile DatagramSocket socket;
    private volatile Thread receiveThread;
    private volatile boolean closed;

    private UdpTransport(Mode mode, InetSocketAddress bindAddress, InetSocketAddress remoteAddress) {
        this.mode = mode;
        this.bindAddress = bindAddress;
        this.remoteAddress = remoteAddress;
    }

    /** A transport bound to {@code port} that discovers peers as their datagrams arrive (0 = ephemeral). */
    public static UdpTransport listening(int port) {
        return new UdpTransport(Mode.LISTENING, new InetSocketAddress(port), null);
    }

    /** A transport bound to a specific address, discovering peers as they arrive. */
    public static UdpTransport listening(InetSocketAddress bindAddress) {
        return new UdpTransport(Mode.LISTENING, bindAddress, null);
    }

    /** A transport that sends to {@code host:port}, which becomes its single peer at {@link #start()}. */
    public static UdpTransport connecting(String host, int port) {
        return new UdpTransport(Mode.CONNECTING, new InetSocketAddress(0), new InetSocketAddress(host, port));
    }

    /** The actual bound local port; only valid after {@link #start()}. */
    public int boundPort() {
        DatagramSocket s = socket;
        if (s == null) {
            throw new ElektroException("UDP transport is not bound");
        }
        return s.getLocalPort();
    }

    /** A point-in-time snapshot of raw traffic counters. */
    public UdpStats stats() {
        return new UdpStats(packetsSent.get(), bytesSent.get(), packetsReceived.get(), bytesReceived.get());
    }

    @Override
    public CompletionStage<Void> start() {
        CompletableFuture<Void> started = new CompletableFuture<>();
        try {
            DatagramSocket s = new DatagramSocket(null);
            s.setReuseAddress(true);
            s.bind(mode == Mode.LISTENING ? bindAddress : new InetSocketAddress(0));
            this.socket = s;
            if (mode == Mode.CONNECTING) {
                // The remote is known up front: register it as the sole peer immediately.
                registerPeer(remoteAddress);
            }
            Thread t = Thread.ofVirtual().name("elektroq-udp-recv").unstarted(this::receiveLoop);
            this.receiveThread = t;
            t.start();
            started.complete(null);
        } catch (IOException e) {
            started.completeExceptionally(
                    new ElektroException("UDP transport failed to start: " + e.getMessage(), e));
        }
        return started;
    }

    @Override
    public void send(PeerId destination, ByteBuffer frame) {
        byte[] bytes = drain(frame);
        if (destination == null || destination.isBroadcast()) {
            for (SocketAddress address : addressByPeer.values()) {
                sendTo(address, bytes);
            }
        } else {
            SocketAddress address = addressByPeer.get(destination.handle());
            if (address == null) {
                throw new ElektroException("No known peer with id " + destination.handle());
            }
            sendTo(address, bytes);
        }
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
        DatagramSocket s = socket;
        if (s != null) {
            s.close(); // unblocks the receive loop
        }
        Thread t = receiveThread;
        if (t != null) {
            t.interrupt();
        }
        peersByAddress.clear();
        addressByPeer.clear();
    }

    // --- internals --------------------------------------------------------------

    private void receiveLoop() {
        byte[] buffer = new byte[MAX_DATAGRAM_BYTES];
        DatagramSocket s = socket;
        while (!closed) {
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            try {
                s.receive(packet);
            } catch (IOException e) {
                if (!closed) {
                    // transient receive error; the socket is still open, keep going
                    continue;
                }
                return; // socket closed under us
            }
            packetsReceived.incrementAndGet();
            bytesReceived.addAndGet(packet.getLength());

            PeerId peer = registerPeer((InetSocketAddress) packet.getSocketAddress());
            byte[] frame = new byte[packet.getLength()];
            System.arraycopy(packet.getData(), packet.getOffset(), frame, 0, packet.getLength());
            frameListener.onFrame(peer, ByteBuffer.wrap(frame));
        }
    }

    /** Returns the peer for {@code address}, assigning and announcing one if it is new. */
    private PeerId registerPeer(SocketAddress address) {
        PeerId existing = peersByAddress.get(address);
        if (existing != null) {
            return existing;
        }
        PeerId assigned = new PeerId(nextPeerId.getAndIncrement());
        PeerId race = peersByAddress.putIfAbsent(address, assigned);
        if (race != null) {
            return race; // another thread won; use theirs
        }
        addressByPeer.put(assigned.handle(), address);
        peerListener.onConnected(assigned);
        return assigned;
    }

    private void sendTo(SocketAddress address, byte[] bytes) {
        DatagramSocket s = socket;
        if (s == null || closed) {
            throw new ElektroException("UDP transport is not open");
        }
        try {
            synchronized (sendLock) {
                s.send(new DatagramPacket(bytes, bytes.length, address));
            }
            packetsSent.incrementAndGet();
            bytesSent.addAndGet(bytes.length);
        } catch (IOException e) {
            throw new ElektroException("UDP send to " + address + " failed: " + e.getMessage(), e);
        }
    }

    private static byte[] drain(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }
}
