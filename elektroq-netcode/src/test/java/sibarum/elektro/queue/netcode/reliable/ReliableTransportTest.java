package sibarum.elektro.queue.netcode.reliable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import sibarum.elektro.queue.message.PeerId;
import sibarum.elektro.queue.netcode.sim.SimParams;
import sibarum.elektro.queue.netcode.sim.SimTransport;
import sibarum.elektro.queue.transport.local.LocalTransport;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for the connection + reliability layer over an impaired in-VM link: the
 * handshake connects and yields a live RTT, application data flows once connected, and a link that
 * goes silent is torn down by the timeout.
 */
class ReliableTransportTest {

    private static final AtomicInteger ENDPOINTS = new AtomicInteger();

    private ReliableTransport client;
    private ReliableTransport server;
    private SimTransport clientSim;
    private SimTransport serverSim;

    /** Wires a client+server pair over SimTransport(LocalTransport) with a fixed one-way latency. */
    private void connect(long latencyMillis, long keepaliveMillis, long timeoutMillis,
                         CountDownLatch clientConnected, CountDownLatch serverConnected,
                         CountDownLatch clientDisconnected, AtomicReference<PeerId> clientPeer,
                         java.util.function.BiConsumer<PeerId, byte[]> serverOnFrame) throws Exception {
        String endpoint = "rel-ep-" + ENDPOINTS.incrementAndGet();

        serverSim = new SimTransport(LocalTransport.listening(endpoint), new SimParams(latencyMillis, 0, 0, 0, 1));
        server = ReliableTransport.of(serverSim, ReliableTransport.Role.SERVER, keepaliveMillis, timeoutMillis);
        server.onFrame((src, frame) -> {
            byte[] b = new byte[frame.remaining()];
            frame.get(b);
            serverOnFrame.accept(src, b);
        });
        server.onPeer(new sibarum.elektro.queue.transport.PeerListener() {
            @Override public void onConnected(PeerId peer) { serverConnected.countDown(); }
            @Override public void onDisconnected(PeerId peer) { }
        });
        server.start().toCompletableFuture().get(5, TimeUnit.SECONDS);

        clientSim = new SimTransport(LocalTransport.connecting(endpoint), new SimParams(latencyMillis, 0, 0, 0, 2));
        client = ReliableTransport.of(clientSim, ReliableTransport.Role.CLIENT, keepaliveMillis, timeoutMillis);
        client.onPeer(new sibarum.elektro.queue.transport.PeerListener() {
            @Override public void onConnected(PeerId peer) { clientPeer.set(peer); clientConnected.countDown(); }
            @Override public void onDisconnected(PeerId peer) { clientDisconnected.countDown(); }
        });
        client.start().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void tearDown() {
        if (client != null) client.close();
        if (server != null) server.close();
    }

    @Test
    void handshakeConnectsAndMeasuresRtt() throws Exception {
        CountDownLatch clientUp = new CountDownLatch(1);
        CountDownLatch serverUp = new CountDownLatch(1);
        AtomicReference<PeerId> clientPeer = new AtomicReference<>();
        connect(30, 100, 3_000, clientUp, serverUp, new CountDownLatch(1), clientPeer, (p, b) -> { });

        assertTrue(clientUp.await(3, TimeUnit.SECONDS), "client should complete the handshake");
        assertTrue(serverUp.await(3, TimeUnit.SECONDS), "server should complete the handshake");

        int rtt = client.rttMillis(clientPeer.get());
        assertTrue(rtt > 0 && rtt < 500, "handshake should yield a plausible RTT (~60ms); got " + rtt);
    }

    @Test
    void dataFlowsOnceConnected() throws Exception {
        CountDownLatch clientUp = new CountDownLatch(1);
        AtomicReference<PeerId> clientPeer = new AtomicReference<>();
        CountDownLatch gotData = new CountDownLatch(1);
        AtomicReference<byte[]> payload = new AtomicReference<>();
        connect(20, 100, 3_000, clientUp, new CountDownLatch(1), new CountDownLatch(1), clientPeer,
                (p, b) -> { payload.set(b); gotData.countDown(); });

        assertTrue(clientUp.await(3, TimeUnit.SECONDS), "client should connect");
        byte[] message = "hello reliable udp".getBytes(StandardCharsets.UTF_8);
        client.send(clientPeer.get(), ByteBuffer.wrap(message));

        assertTrue(gotData.await(3, TimeUnit.SECONDS), "server should receive the data frame");
        assertArrayEquals(message, payload.get());
    }

    @Test
    void silentLinkTimesOut() throws Exception {
        CountDownLatch clientUp = new CountDownLatch(1);
        CountDownLatch clientDown = new CountDownLatch(1);
        AtomicReference<PeerId> clientPeer = new AtomicReference<>();
        connect(20, 100, 600, clientUp, new CountDownLatch(1), clientDown, clientPeer, (p, b) -> { });

        assertTrue(clientUp.await(3, TimeUnit.SECONDS), "client should connect first");
        // The link goes dead in both directions: no keepalives cross, so the timeout must fire.
        clientSim.blackhole(true);
        serverSim.blackhole(true);

        assertTrue(clientDown.await(3, TimeUnit.SECONDS), "a silent peer should time out and disconnect");
    }
}
