package sibarum.elektro.queue.transport.uds;

import sibarum.elektro.queue.ElektroException;
import sibarum.elektro.queue.message.PeerId;
import sibarum.elektro.queue.transport.FrameListener;
import sibarum.elektro.queue.transport.PeerListener;
import sibarum.elektro.queue.transport.Transport;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A Unix-domain-socket {@link Transport}: the fast same-machine path. It speaks the same discrete
 * frames as the TCP transport, but over an {@code AF_UNIX} socket, so it skips the entire TCP/IP
 * stack (no loopback routing, no checksums, no Nagle) while remaining an ordinary reliable, ordered
 * byte stream. On this machine it is a drop-in for {@link sibarum.elektro.queue.transport.tcp} with
 * lower latency; it does not cross machines.
 *
 * <p>Framing mirrors TCP exactly: each opaque frame is written as a 4-byte big-endian length
 * followed by the frame bytes, so the transport stays oblivious to the {@code MessageHeader} it
 * carries. Each connection is served by a dedicated virtual thread running a blocking read loop;
 * writes are serialized per connection. All callbacks run on those connection threads.
 *
 * <p>The endpoint is a filesystem {@link Path} (the socket file). A {@link #listening} transport
 * binds and, on {@link #close()}, deletes that file; a {@link #connecting} transport dials it.
 *
 * <p>Availability: Unix domain sockets require JDK 16+ and an OS with {@code AF_UNIX} support
 * (Linux, macOS, and Windows 10/11). Pure NIO — no reflection, native-image clean.
 */
public final class UdsTransport implements Transport {

    /** Upper bound on a single frame, guarding against a corrupt or hostile length prefix. */
    private static final int MAX_FRAME_BYTES = 64 * 1024 * 1024;

    private static final PeerListener NO_PEER_LISTENER = new PeerListener() {
        @Override public void onConnected(PeerId peer) { }
        @Override public void onDisconnected(PeerId peer) { }
    };

    private enum Mode { LISTENING, CONNECTING }

    private final Mode mode;
    private final Path socketPath;
    private final ExecutorService threads = Executors.newVirtualThreadPerTaskExecutor();
    private final ConcurrentHashMap<Long, Connection> connections = new ConcurrentHashMap<>();
    private final AtomicLong nextPeerId = new AtomicLong(1); // 0 is reserved for BROADCAST

    private volatile FrameListener frameListener = (source, frame) -> { };
    private volatile PeerListener peerListener = NO_PEER_LISTENER;
    private volatile ServerSocketChannel serverChannel;
    private volatile boolean closed;

    private UdsTransport(Mode mode, Path socketPath) {
        this.mode = mode;
        this.socketPath = socketPath;
    }

    /** A server transport that binds the socket file at {@code socketPath}. */
    public static UdsTransport listening(Path socketPath) {
        return new UdsTransport(Mode.LISTENING, requirePath(socketPath));
    }

    /** A client transport that dials the socket file at {@code socketPath} on {@link #start()}. */
    public static UdsTransport connecting(Path socketPath) {
        return new UdsTransport(Mode.CONNECTING, requirePath(socketPath));
    }

    /** The bound socket path; valid on a listening transport once {@link #start()} has completed. */
    public Path boundPath() {
        if (serverChannel == null) {
            throw new ElektroException("Transport is not a bound listener");
        }
        return socketPath;
    }

    @Override
    public CompletionStage<Void> start() {
        CompletableFuture<Void> started = new CompletableFuture<>();
        try {
            UnixDomainSocketAddress address = UnixDomainSocketAddress.of(socketPath);
            if (mode == Mode.LISTENING) {
                ServerSocketChannel channel = ServerSocketChannel.open(StandardProtocolFamily.UNIX);
                channel.bind(address);
                this.serverChannel = channel;
                threads.submit(this::acceptLoop);
            } else {
                SocketChannel channel = SocketChannel.open(StandardProtocolFamily.UNIX);
                channel.connect(address);
                register(channel);
            }
            started.complete(null);
        } catch (IOException e) {
            started.completeExceptionally(
                    new ElektroException("UDS transport failed to start: " + e.getMessage(), e));
        }
        return started;
    }

    @Override
    public void send(PeerId destination, ByteBuffer frame) {
        if (destination == null || destination.isBroadcast()) {
            byte[] bytes = drain(frame);
            for (Connection connection : connections.values()) {
                connection.write(bytes);
            }
        } else {
            Connection connection = connections.get(destination.handle());
            if (connection == null) {
                throw new ElektroException("No connected peer with id " + destination.handle());
            }
            connection.write(drain(frame));
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
        ServerSocketChannel channel = serverChannel;
        if (channel != null) {
            closeQuietly(channel);
        }
        for (Connection connection : connections.values()) {
            connection.closeQuietly();
        }
        connections.clear();
        threads.shutdownNow();
        if (mode == Mode.LISTENING) {
            try {
                Files.deleteIfExists(socketPath); // don't leave a stale socket file behind
            } catch (IOException ignored) {
                // best effort
            }
        }
    }

    // --- internals --------------------------------------------------------------

    /**
     * Accepts until the transport is closed.
     *
     * <p>A listener that faults while the transport still believes it is open is the one failure here that
     * nothing downstream can infer: the socket file is still on disk, {@code state()} still says open, and the
     * refusal lands at whichever client dials next. It is reported rather than returned from quietly.
     */
    private void acceptLoop() {
        ServerSocketChannel channel = serverChannel;
        while (!closed) {
            try {
                register(channel.accept());
            } catch (IOException e) {
                if (!closed) {
                    fault("listener on " + socketPath + " faulted; no further connections will be accepted", e);
                }
                return;
            } catch (RuntimeException e) {
                // register() runs peerListener.onConnected inline, so application code shares this thread.
                // Letting it through would end the accept loop for good — and end it inside a submitted task,
                // where the throwable goes into a Future nobody reads. One peer's registration failing is not
                // a reason to stop serving the rest.
                fault("registering an accepted connection failed", e);
            }
        }
    }

    /**
     * Takes a connected channel into service: registered, announced, and then read from.
     *
     * <p>The announcement is application code, and a throw from it used to leave this peer registered and
     * never served: nothing submits its read loop, so no {@code readLoop} finally-block ever runs to strike it
     * off. It would sit in the map holding a channel nobody reads, a broadcast would write into it, and the
     * first one to fill the peer's receive buffer would block {@link #send} for every other peer behind that
     * connection's write lock. So the registration is undone before the fault leaves here — a peer the
     * application refused to accept is not a peer. {@code onDisconnected} is deliberately not called:
     * {@code onConnected} never returned, so there is no connection the application believes in.
     */
    private void register(SocketChannel channel) throws IOException {
        Connection connection = new Connection(new PeerId(nextPeerId.getAndIncrement()), channel);
        connections.put(connection.peer.handle(), connection);
        try {
            peerListener.onConnected(connection.peer);
        } catch (RuntimeException e) {
            connections.remove(connection.peer.handle());
            connection.closeQuietly();
            throw e;
        }
        threads.submit(() -> readLoop(connection));
    }

    private void readLoop(Connection connection) {
        try {
            DataInputStream in = connection.in;
            while (!closed) {
                int length = in.readInt();
                if (length < 0 || length > MAX_FRAME_BYTES) {
                    throw new ElektroException("Illegal frame length: " + length);
                }
                byte[] frame = new byte[length];
                in.readFully(frame);
                dispatch(connection, frame);
            }
        } catch (EOFException eof) {
            // Peer closed the connection cleanly.
        } catch (IOException e) {
            if (!closed) {
                fault("connection to peer " + connection.peer.handle() + " faulted", e);
            }
        } catch (ElektroException e) {
            // A framing fault: the length prefix did not describe a frame, so the stream position is no longer
            // trustworthy and this connection cannot be resynchronised. Unlike a listener fault it is terminal.
            fault("framing error on peer " + connection.peer.handle() + "; dropping the connection", e);
        } finally {
            disconnect(connection);
        }
    }

    /**
     * Hands one frame to the listener, absorbing a fault in it.
     *
     * <p>The frame has already been consumed in full by the time this runs — framing is the length prefix and
     * nothing else, decided here and independent of what the payload turns out to be — so the stream is still
     * in sync and a listener that throws has not cost us the connection. Reported and skipped, therefore,
     * rather than escaping into {@code threads.submit} where it would be filed in an unread {@code Future} and
     * surface only as a peer that disconnected for no stated reason.
     */
    private void dispatch(Connection connection, byte[] frame) {
        try {
            frameListener.onFrame(connection.peer, ByteBuffer.wrap(frame));
        } catch (RuntimeException e) {
            fault("frame listener failed on a " + frame.length + "-byte frame from peer "
                    + connection.peer.handle() + "; frame dropped", e);
        }
    }

    /**
     * The last-resort report for a fault on a thread with no caller to throw to.
     *
     * <p>stderr because this module has no probe and no logger, and a fault nobody hears about is the failure
     * this reporting exists to prevent — see {@code Fatal} in atchung-core for the same argument at more
     * length. A transport that grows a diagnostic seam should route these through it instead.
     */
    private static void fault(String what, Throwable cause) {
        System.err.println("[elektroq] uds: " + what + ": " + cause);
        cause.printStackTrace(System.err);
    }

    private void disconnect(Connection connection) {
        if (connections.remove(connection.peer.handle()) != null) {
            connection.closeQuietly();
            peerListener.onDisconnected(connection.peer);
        }
    }

    private static byte[] drain(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    private static void closeQuietly(ServerSocketChannel channel) {
        try {
            channel.close();
        } catch (IOException ignored) {
            // best effort
        }
    }

    private static Path requirePath(Path socketPath) {
        if (socketPath == null) {
            throw new IllegalArgumentException("socketPath must not be null");
        }
        return socketPath;
    }

    /** One peer connection: a channel, its buffered streams, and a per-connection write lock. */
    private final class Connection {

        private final PeerId peer;
        private final SocketChannel channel;
        private final DataInputStream in;
        private final DataOutputStream out;
        private final Object writeLock = new Object();

        Connection(PeerId peer, SocketChannel channel) throws IOException {
            this.peer = peer;
            this.channel = channel;
            this.in = new DataInputStream(new BufferedInputStream(Channels.newInputStream(channel)));
            this.out = new DataOutputStream(new BufferedOutputStream(Channels.newOutputStream(channel)));
        }

        void write(byte[] frame) {
            synchronized (writeLock) {
                try {
                    out.writeInt(frame.length);
                    out.write(frame);
                    out.flush();
                } catch (IOException e) {
                    closeQuietly();
                    throw new ElektroException(
                            "Failed to send to peer " + peer.handle() + ": " + e.getMessage(), e);
                }
            }
        }

        void closeQuietly() {
            try {
                channel.close();
            } catch (IOException ignored) {
                // best effort
            }
        }
    }
}
