package sibarum.elektro.queue.transport.local;

import sibarum.elektro.queue.ElektroException;
import sibarum.elektro.queue.message.PeerId;
import sibarum.elektro.queue.transport.FrameListener;
import sibarum.elektro.queue.transport.PeerListener;
import sibarum.elektro.queue.transport.Transport;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An in-VM {@link Transport}: it moves framed buffers between conduits <b>in the same JVM</b> over
 * bounded queues on virtual threads, with no sockets.
 *
 * <p>It mirrors the TCP transport's shape so the layer above cannot tell them apart. A
 * {@link #listening} transport registers a named endpoint in a process-wide {@link LocalSwitch};
 * a {@link #connecting} transport dials that name on {@link #start()}, and the switch wires the two
 * together into a bidirectional pair. Each side assigns its <em>own</em> {@link PeerId} for the
 * other, exactly as TCP assigns a handle per accepted socket.
 *
 * <p>Every transport drains its inbound frames on a single dedicated virtual thread, so per-peer
 * ordering is preserved and callbacks never run on the sender's thread &mdash; the same thread
 * boundary a socket would impose. The inbound queue is bounded, so a slow receiver applies natural
 * backpressure to senders (a full queue blocks {@link #send}). Frames are copied out of the
 * caller's {@link ByteBuffer} on send, so no buffer is shared across the boundary.
 */
public final class LocalTransport implements Transport {

    private static final int DEFAULT_INBOX_CAPACITY = 1024;

    private static final PeerListener NO_PEER_LISTENER = new PeerListener() {
        @Override public void onConnected(PeerId peer) { }
        @Override public void onDisconnected(PeerId peer) { }
    };

    private enum Mode { LISTENING, CONNECTING }

    private final Mode mode;
    private final String endpoint;
    private final int inboxCapacity;

    private final ExecutorService threads = Executors.newVirtualThreadPerTaskExecutor();
    private final BlockingQueue<Inbound> inbox;
    private final ConcurrentHashMap<Long, Peer> connections = new ConcurrentHashMap<>();
    private final AtomicLong nextPeerId = new AtomicLong(1); // 0 is reserved for BROADCAST

    private volatile FrameListener frameListener = (source, frame) -> { };
    private volatile PeerListener peerListener = NO_PEER_LISTENER;
    private volatile boolean closed;

    private LocalTransport(Mode mode, String endpoint, int inboxCapacity) {
        this.mode = mode;
        this.endpoint = endpoint;
        this.inboxCapacity = inboxCapacity;
        this.inbox = new ArrayBlockingQueue<>(inboxCapacity);
    }

    /** A server transport that publishes {@code endpoint} for local clients to dial. */
    public static LocalTransport listening(String endpoint) {
        return new LocalTransport(Mode.LISTENING, requireEndpoint(endpoint), DEFAULT_INBOX_CAPACITY);
    }

    /** A server transport with an explicit inbound-queue capacity (backpressure bound). */
    public static LocalTransport listening(String endpoint, int inboxCapacity) {
        return new LocalTransport(Mode.LISTENING, requireEndpoint(endpoint), positive(inboxCapacity));
    }

    /** A client transport that dials the local {@code endpoint} on {@link #start()}. */
    public static LocalTransport connecting(String endpoint) {
        return new LocalTransport(Mode.CONNECTING, requireEndpoint(endpoint), DEFAULT_INBOX_CAPACITY);
    }

    /** A client transport with an explicit inbound-queue capacity (backpressure bound). */
    public static LocalTransport connecting(String endpoint, int inboxCapacity) {
        return new LocalTransport(Mode.CONNECTING, requireEndpoint(endpoint), positive(inboxCapacity));
    }

    @Override
    public CompletionStage<Void> start() {
        CompletableFuture<Void> started = new CompletableFuture<>();
        try {
            threads.submit(this::deliverLoop);
            if (mode == Mode.LISTENING) {
                LocalSwitch.INSTANCE.bind(endpoint, this);
            } else {
                LocalTransport server = LocalSwitch.INSTANCE.dial(endpoint);
                if (server == null) {
                    throw new ElektroException("No local endpoint bound at '" + endpoint + "'");
                }
                pair(server, this);
            }
            started.complete(null);
        } catch (RuntimeException e) {
            started.completeExceptionally(e instanceof ElektroException ee ? ee
                    : new ElektroException("Local transport failed to start: " + e.getMessage(), e));
        }
        return started;
    }

    @Override
    public void send(PeerId destination, ByteBuffer frame) {
        byte[] bytes = drain(frame);
        if (destination == null || destination.isBroadcast()) {
            for (Peer peer : connections.values()) {
                peer.deliver(bytes);
            }
        } else {
            Peer peer = connections.get(destination.handle());
            if (peer == null) {
                throw new ElektroException("No connected peer with id " + destination.handle());
            }
            peer.deliver(bytes);
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
        if (mode == Mode.LISTENING) {
            LocalSwitch.INSTANCE.unbind(endpoint, this);
        }
        // Tear down every link from this side; the counterpart is notified symmetrically.
        for (Peer peer : connections.values()) {
            disconnect(peer.localId.handle());
            peer.remote.owner.disconnect(peer.remote.sourceId.handle());
        }
        connections.clear();
        threads.shutdownNow();
    }

    // --- internals --------------------------------------------------------------

    /** Wires a fresh bidirectional link between a bound server and a dialing client. */
    private static void pair(LocalTransport server, LocalTransport client) {
        PeerId serverSideId = new PeerId(server.nextPeerId.getAndIncrement()); // server's id for client
        PeerId clientSideId = new PeerId(client.nextPeerId.getAndIncrement()); // client's id for server

        Peer serverPeer = new Peer(serverSideId, new RemoteEnd(client, clientSideId));
        Peer clientPeer = new Peer(clientSideId, new RemoteEnd(server, serverSideId));

        server.connections.put(serverSideId.handle(), serverPeer);
        client.connections.put(clientSideId.handle(), clientPeer);

        server.peerListener.onConnected(serverSideId);
        client.peerListener.onConnected(clientSideId);
    }

    private void deliverLoop() {
        try {
            while (!closed) {
                Inbound in = inbox.take();
                frameListener.onFrame(in.source, ByteBuffer.wrap(in.frame));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Enqueues an inbound frame for this transport's deliver loop; blocks if the inbox is full. */
    private void receive(PeerId source, byte[] frame) {
        if (closed) {
            return;
        }
        try {
            inbox.put(new Inbound(source, frame));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ElektroException("Interrupted delivering to '" + endpoint + "'", e);
        }
    }

    private void disconnect(long peerHandle) {
        Peer peer = connections.remove(peerHandle);
        if (peer != null) {
            peerListener.onDisconnected(peer.localId);
        }
    }

    private static byte[] drain(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    private static String requireEndpoint(String endpoint) {
        if (endpoint == null || endpoint.isBlank()) {
            throw new IllegalArgumentException("endpoint must be non-blank");
        }
        return endpoint;
    }

    private static int positive(int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("inboxCapacity must be >= 1, was " + capacity);
        }
        return capacity;
    }

    /** One end of a link as seen from this transport: this side's id + how to reach the other. */
    private static final class Peer {
        final PeerId localId;
        final RemoteEnd remote;

        Peer(PeerId localId, RemoteEnd remote) {
            this.localId = localId;
            this.remote = remote;
        }

        void deliver(byte[] frame) {
            remote.owner.receive(remote.sourceId, frame);
        }
    }

    /** The counterpart transport plus the {@link PeerId} it uses to refer to <em>this</em> side. */
    private record RemoteEnd(LocalTransport owner, PeerId sourceId) {}

    private record Inbound(PeerId source, byte[] frame) {}
}
