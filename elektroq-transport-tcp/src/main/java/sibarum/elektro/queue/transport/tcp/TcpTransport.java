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

    private void acceptLoop() {
        ServerSocket socket = serverSocket;
        while (!closed) {
            try {
                register(socket.accept());
            } catch (IOException e) {
                if (!closed) {
                    // The listener was closed or faulted; stop accepting.
                }
                return;
            }
        }
    }

    private void register(Socket socket) throws IOException {
        socket.setTcpNoDelay(true);
        Connection connection = new Connection(new PeerId(nextPeerId.getAndIncrement()), socket);
        connections.put(connection.peer.handle(), connection);
        peerListener.onConnected(connection.peer);
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
                frameListener.onFrame(connection.peer, ByteBuffer.wrap(frame));
            }
        } catch (EOFException eof) {
            // Peer closed the connection cleanly.
        } catch (IOException | ElektroException e) {
            // Connection faulted; fall through to disconnect.
        } finally {
            disconnect(connection);
        }
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
