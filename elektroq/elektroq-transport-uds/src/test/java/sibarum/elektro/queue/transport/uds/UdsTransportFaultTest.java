package sibarum.elektro.queue.transport.uds;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import sibarum.elektro.queue.message.PeerId;
import sibarum.elektro.queue.transport.PeerListener;

import java.io.IOException;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What the read loop does when something on it fails, over a real {@code AF_UNIX} socket.
 *
 * <p>The mirror of {@code TcpTransportFaultTest}, and mirrored on purpose: the two transports carry the same
 * framing and the same callback contract, so a fault that costs a frame on one must not cost a connection on
 * the other. They share no code, which is exactly why they cannot share one set of tests.
 *
 * <p>Each prints a stack trace to stderr on the way past. That is the point of them; the noise is the fix.
 */
class UdsTransportFaultTest {

    private Path socketDir;
    private Path socket;
    private UdsTransport server;
    private UdsTransport client;

    private Path socketPath() throws IOException {
        socketDir = Files.createTempDirectory("eq");   // short path — AF_UNIX paths are length-limited
        socket = socketDir.resolve("s");
        return socket;
    }

    @AfterEach
    void tearDown() throws IOException {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.close();
        }
        if (socket != null) {
            Files.deleteIfExists(socket);
        }
        if (socketDir != null) {
            Files.deleteIfExists(socketDir);
        }
    }

    /**
     * A frame listener that throws costs its own frame and nothing else.
     *
     * <p>Framing is the length prefix, decided by the transport and independent of the payload, and the frame
     * is consumed in full before the listener runs — so the stream is still in sync afterwards and the
     * connection has no reason to die. Before the fix the first frame killed the connection and the second
     * never arrived.
     */
    @Test
    void listenerFaultDoesNotKillTheConnection() throws Exception {
        List<String> seen = new CopyOnWriteArrayList<>();
        CountDownLatch second = new CountDownLatch(1);
        Path path = socketPath();

        server = UdsTransport.listening(path);
        server.onFrame((source, frame) -> {
            byte[] bytes = new byte[frame.remaining()];
            frame.get(bytes);
            String text = new String(bytes, StandardCharsets.UTF_8);
            seen.add(text);
            if (text.equals("boom")) {
                throw new IllegalStateException("listener fault, deliberately");
            }
            second.countDown();
        });
        server.start().toCompletableFuture().get(5, TimeUnit.SECONDS);

        client = UdsTransport.connecting(path);
        client.start().toCompletableFuture().get(5, TimeUnit.SECONDS);

        client.send(PeerId.BROADCAST, ByteBuffer.wrap("boom".getBytes(StandardCharsets.UTF_8)));
        client.send(PeerId.BROADCAST, ByteBuffer.wrap("after".getBytes(StandardCharsets.UTF_8)));

        assertTrue(second.await(5, TimeUnit.SECONDS),
                "the frame after a throwing listener should still be delivered");
        assertEquals(List.of("boom", "after"), seen);
    }

    /**
     * A framing fault is terminal, and still is.
     *
     * <p>An illegal length prefix means the stream position no longer describes a frame boundary, so there is
     * nothing to resynchronise to and the connection has to go. The peer listener hears about it, which is what
     * separates this from the case above.
     */
    @Test
    void framingFaultDisconnectsThePeer() throws Exception {
        CountDownLatch connected = new CountDownLatch(1);
        CountDownLatch disconnected = new CountDownLatch(1);
        Path path = socketPath();

        server = UdsTransport.listening(path);
        server.onPeer(new PeerListener() {
            @Override public void onConnected(PeerId peer) {
                connected.countDown();
            }

            @Override public void onDisconnected(PeerId peer) {
                disconnected.countDown();
            }
        });
        server.start().toCompletableFuture().get(5, TimeUnit.SECONDS);

        // A raw channel, so the length prefix can be a lie the framing layer has to catch.
        try (SocketChannel raw = SocketChannel.open(UnixDomainSocketAddress.of(path))) {
            assertTrue(connected.await(5, TimeUnit.SECONDS), "the server should have accepted the channel");
            ByteBuffer lie = ByteBuffer.allocate(4).putInt(Integer.MAX_VALUE);
            lie.flip();
            raw.write(lie);
            assertTrue(disconnected.await(5, TimeUnit.SECONDS),
                    "an illegal frame length should disconnect the peer, and say so");
        }
    }

    /**
     * A peer the application refuses is not left registered.
     *
     * <p>{@code onConnected} is application code on the accept thread, and the loop surviving a throw from it
     * is only an improvement if the half-registered peer goes with it. Otherwise every refusal leaves a channel
     * nobody reads in the connection map — and since nothing ever submits that connection's read loop, no
     * {@code finally} ever strikes it off. A broadcast then writes into it until the peer's receive buffer
     * fills and blocks {@code send} for everybody else.
     */
    @Test
    void aRefusedPeerIsNotLeftRegistered() throws Exception {
        AtomicInteger accepted = new AtomicInteger();
        CountDownLatch served = new CountDownLatch(1);
        Path path = socketPath();

        server = UdsTransport.listening(path);
        server.onPeer(new PeerListener() {
            @Override public void onConnected(PeerId peer) {
                if (accepted.incrementAndGet() == 1) {
                    throw new IllegalStateException("refusing the first peer, deliberately");
                }
            }

            @Override public void onDisconnected(PeerId peer) { }
        });
        server.onFrame((source, frame) -> served.countDown());
        server.start().toCompletableFuture().get(5, TimeUnit.SECONDS);

        try (SocketChannel raw = SocketChannel.open(UnixDomainSocketAddress.of(path))) {
            // End of stream, not a hang: the server dropped the channel it could not take into service. A
            // blocking channel read has no timeout of its own, so the bound here is the assertion's.
            int read = assertTimeoutPreemptively(Duration.ofSeconds(5),
                    () -> raw.read(ByteBuffer.allocate(1)),
                    "a refused peer's channel should be closed, not left half-open");
            assertEquals(-1, read, "a refused peer's channel should be closed");
        }

        // And the listener is still serving, which is the other half of the same fix.
        client = UdsTransport.connecting(path);
        client.start().toCompletableFuture().get(5, TimeUnit.SECONDS);
        client.send(PeerId.BROADCAST, ByteBuffer.wrap("after".getBytes(StandardCharsets.UTF_8)));

        assertTrue(served.await(5, TimeUnit.SECONDS), "the peer after a refused one should be served");
    }
}
