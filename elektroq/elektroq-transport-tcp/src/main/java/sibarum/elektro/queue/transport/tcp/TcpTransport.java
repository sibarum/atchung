package sibarum.elektro.queue.transport.tcp;

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
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A TCP {@link Transport} built on blocking sockets and virtual threads.
 *
 * <p>Because TCP is a byte stream while the SPI moves discrete frames, this transport
 * adds its own length delimiting: each opaque frame is written as a 4-byte big-endian
 * length followed by the frame bytes. This keeps the transport oblivious to the
 * elektro-Q {@code MessageHeader} it is carrying &mdash; framing here is independent of
 * message contents.
 *
 * <p>A transport is created in one of two modes: {@link #listening} accepts many peer
 * connections; {@link #connecting} dials a single remote. Each connection is served by
 * a dedicated virtual thread running a blocking read loop; writes are serialized per
 * connection. All callbacks ({@link FrameListener}, {@link PeerListener}) run on those
 * connection threads.
 */
public final class TcpTransport implements Transport {

    /** Upper bound on a single frame, guarding against a corrupt or hostile length prefix. */
    private static final int MAX_FRAME_BYTES = 64 * 1024 * 1024;

    private static final PeerListener NO_PEER_LISTENER = new PeerListener() {
        @Override public void onConnected(PeerId peer) { }
        @Override public void onDisconnected(PeerId peer) { }
    };

    private enum Mode { LISTENING, CONNECTING }

    private final Mode mode;
    private final InetSocketAddress address;
    private final ExecutorService threads = Executors.newVirtualThreadPerTaskExecutor();
    private final ConcurrentHashMap<Long, Connection> connections = new ConcurrentHashMap<>();
    private final AtomicLong nextPeerId = new AtomicLong(1); // 0 is reserved for BROADCAST

    private volatile FrameListener frameListener = (source, frame) -> { };
    private volatile PeerListener peerListener = NO_PEER_LISTENER;
    private volatile ServerSocket serverSocket;
    private volatile boolean closed;

    private TcpTransport(Mode mode, InetSocketAddress address) {
        this.mode = mode;
        this.address = address;
    }

    /** A server transport bound to {@code port} (0 selects an ephemeral port). */
    public static TcpTransport listening(int port) {
        return new TcpTransport(Mode.LISTENING, new InetSocketAddress(port));
    }

    /** A server transport bound to a specific address. */
    public static TcpTransport listening(InetSocketAddress bindAddress) {
        return new TcpTransport(Mode.LISTENING, bindAddress);
    }

    /** A client transport that dials {@code host:port} on {@link #start()}. */
    public static TcpTransport connecting(String host, int port) {
        return new TcpTransport(Mode.CONNECTING, new InetSocketAddress(host, port));
    }

    /** The actual bound port of a listening transport; only valid after {@link #start()}. */
    public int boundPort() {
        ServerSocket socket = serverSocket;
        if (socket == null) {
            throw new ElektroException("Transport is not a bound listener");
        }
        return socket.getLocalPort();
    }

    @Override
    public CompletionStage<Void> start() {
        CompletableFuture<Void> started = new CompletableFuture<>();
        try {
            if (mode == Mode.LISTENING) {
                ServerSocket socket = new ServerSocket();
                socket.setReuseAddress(true);
                socket.bind(address);
                this.serverSocket = socket;
                threads.submit(this::acceptLoop);
            } else {
                Socket socket = new Socket();
                socket.connect(address);
                register(socket);
            }
            started.complete(null);
        } catch (IOException e) {
            started.completeExceptionally(
                    new ElektroException("TCP transport failed to start: " + e.getMessage(), e));
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
        ServerSocket socket = serverSocket;
        if (socket != null) {
            closeQuietly(socket);
        }
        for (Connection connection : connections.values()) {
            connection.closeQuietly();
        }
        connections.clear();
        threads.shutdownNow();
    }

    // --- internals --------------------------------------------------------------

    /**
     * Accepts until the transport is closed.
     *
     * <p>A listener that faults while the transport still believes it is open is the one failure here that
     * nothing downstream can infer: {@code state()} still says open, {@link #boundPort()} still answers, and
     * the refusal lands at whichever client dials next. It is reported rather than returned from quietly.
     */
    private void acceptLoop() {
        ServerSocket socket = serverSocket;
        while (!closed) {
            try {
                register(socket.accept());
            } catch (IOException e) {
                if (!closed) {
                    fault("listener on " + address + " faulted; no further connections will be accepted", e);
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
     * Takes a connected socket into service: registered, announced, and then read from.
     *
     * <p>The announcement is application code, and a throw from it used to leave this peer registered and
     * never served: nothing submits its read loop, so no {@code readLoop} finally-block ever runs to strike it
     * off. It would sit in the map holding a socket nobody reads, a broadcast would write into it, and the first
     * one to fill the peer's receive buffer would block {@link #send} for every other peer behind that
     * connection's write lock. So the registration is undone before the fault leaves here — a peer the
     * application refused to accept is not a peer. {@code onDisconnected} is deliberately not called:
     * {@code onConnected} never returned, so there is no connection the application believes in.
     */
    private void register(Socket socket) throws IOException {
        socket.setTcpNoDelay(true);
        Connection connection = new Connection(new PeerId(nextPeerId.getAndIncrement()), socket);
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
        System.err.println("[elektroq] tcp: " + what + ": " + cause);
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

    private static void closeQuietly(ServerSocket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
            // best effort
        }
    }

    /** One peer connection: a socket, its buffered streams, and a per-connection write lock. */
    private final class Connection {

        private final PeerId peer;
        private final Socket socket;
        private final DataInputStream in;
        private final DataOutputStream out;
        private final Object writeLock = new Object();

        Connection(PeerId peer, Socket socket) throws IOException {
            this.peer = peer;
            this.socket = socket;
            this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        }

        void write(byte[] frame) {
            synchronized (writeLock) {
                try {
                    out.writeInt(frame.length);
                    out.write(frame);
                    out.flush();
                } catch (IOException e) {
                    closeQuietly();
                    throw new ElektroException("Failed to send to peer " + peer.handle() + ": " + e.getMessage(), e);
                }
            }
        }

        void closeQuietly() {
            try {
                socket.close();
            } catch (IOException ignored) {
                // best effort
            }
        }
    }
}
