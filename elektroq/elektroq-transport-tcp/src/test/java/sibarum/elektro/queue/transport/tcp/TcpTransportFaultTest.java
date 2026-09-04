package sibarum.elektro.queue.transport.tcp;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import sibarum.elektro.queue.message.PeerId;
import sibarum.elektro.queue.transport.PeerListener;

import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What the read loop does when something on it fails.
 *
 * <p>These cases used to be silent: a listener that threw took the connection down with it and the
 * throwable went into the {@code Future} of a submitted task nobody reads, so the only symptom was a peer
 * that disconnected for no stated reason. The tests pin the faults apart — which cost a frame, which cost a
 * connection, and which cost neither.
 *
 * <p>Each prints a stack trace to stderr on the way past. That is the point of them; the noise is the fix.
 */
class TcpTransportFaultTest {

    private TcpTransport server;
    private TcpTransport client;

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.close();
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

        server = TcpTransport.listening(0);
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

        client = TcpTransport.connecting("localhost", server.boundPort());
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

        server = TcpTransport.listening(0);
        server.onPeer(new PeerListener() {
            @Override public void onConnected(PeerId peer) {
                connected.countDown();
            }

            @Override public void onDisconnected(PeerId peer) {
                disconnected.countDown();
            }
        });
        server.start().toCompletableFuture().get(5, TimeUnit.SECONDS);

        // A raw socket, so the length prefix can be a lie the framing layer has to catch.
        try (Socket raw = new Socket("localhost", server.boundPort())) {
            assertTrue(connected.await(5, TimeUnit.SECONDS), "the server should have accepted the socket");
            DataOutputStream out = new DataOutputStream(raw.getOutputStream());
            out.writeInt(Integer.MAX_VALUE);
            out.flush();
            assertTrue(disconnected.await(5, TimeUnit.SECONDS),
                    "an illegal frame length should disconnect the peer, and say so");
        }
    }

    /**
     * A peer the application refuses is not left registered.
     *
     * <p>{@code onConnected} is application code on the accept thread, and the loop surviving a throw from it
     * is only an improvement if the half-registered peer goes with it. Otherwise every refusal leaves a socket
     * nobody reads in the connection map — and since nothing ever submits that connection's read loop, no
     * {@code finally} ever strikes it off. A broadcast then writes into it until the peer's receive buffer
     * fills and blocks {@code send} for everybody else.
     */
    @Test
    void aRefusedPeerIsNotLeftRegistered() throws Exception {
        AtomicInteger accepted = new AtomicInteger();
        CountDownLatch served = new CountDownLatch(1);

        server = TcpTransport.listening(0);
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

        try (Socket refused = new Socket("localhost", server.boundPort())) {
            refused.setSoTimeout(5000);
            // End of stream, not a hang: the server dropped the socket it could not take into service.
            assertEquals(-1, refused.getInputStream().read(), "a refused peer's socket should be closed");
        }

        // And the listener is still serving, which is the other half of the same fix.
        client = TcpTransport.connecting("localhost", server.boundPort());
        client.start().toCompletableFuture().get(5, TimeUnit.SECONDS);
        client.send(PeerId.BROADCAST, ByteBuffer.wrap("after".getBytes(StandardCharsets.UTF_8)));

        assertTrue(served.await(5, TimeUnit.SECONDS), "the peer after a refused one should be served");
    }
}
