package sibarum.elektro.queue.netcode.reliable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import sibarum.elektro.queue.message.PeerId;
import sibarum.elektro.queue.netcode.channel.DeliveryMode;
import sibarum.elektro.queue.netcode.sim.SimParams;
import sibarum.elektro.queue.netcode.sim.SimTransport;
import sibarum.elektro.queue.transport.local.LocalTransport;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for the channel layer over a genuinely impaired link (loss + reorder via the
 * simulator): reliable-ordered delivers everything in order despite drops, and unreliable-sequenced
 * only ever delivers forward (stale arrivals dropped). This proves the retransmit + reorder wiring,
 * not just the isolated logic.
 */
class ReliableChannelTest {

    private static final AtomicInteger ENDPOINTS = new AtomicInteger();

    private ReliableTransport client;
    private ReliableTransport server;

    private static byte[] intBytes(int v) {
        return ByteBuffer.allocate(4).putInt(v).array();
    }

    private static int toInt(byte[] b) {
        return ByteBuffer.wrap(b).getInt();
    }

    /**
     * Wires client+server over SimTransport(LocalTransport) with {@code params}, routing every
     * delivered message to {@code onServerMessage}. Blocks until both have completed the handshake.
     */
    private PeerId connect(SimParams params, ReliableTransport.NetcodeListener onServerMessage) throws Exception {
        String endpoint = "chan-ep-" + ENDPOINTS.incrementAndGet();
        long keepalive = 50;
        long timeout = 20_000; // long: never disconnect during the test

        server = ReliableTransport.of(new SimTransport(LocalTransport.listening(endpoint), params),
                ReliableTransport.Role.SERVER, keepalive, timeout);
        server.onNetcodeMessage(onServerMessage);
        server.onPeer(NO_OP);
        server.start().toCompletableFuture().get(5, TimeUnit.SECONDS);

        CountDownLatch clientUp = new CountDownLatch(1);
        AtomicReference<PeerId> serverPeer = new AtomicReference<>();
        client = ReliableTransport.of(new SimTransport(LocalTransport.connecting(endpoint), params),
                ReliableTransport.Role.CLIENT, keepalive, timeout);
        client.onPeer(new sibarum.elektro.queue.transport.PeerListener() {
            @Override public void onConnected(PeerId peer) { serverPeer.set(peer); clientUp.countDown(); }
            @Override public void onDisconnected(PeerId peer) { }
        });
        client.start().toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertTrue(clientUp.await(5, TimeUnit.SECONDS), "handshake should complete");
        return serverPeer.get();
    }

    private static final sibarum.elektro.queue.transport.PeerListener NO_OP =
            new sibarum.elektro.queue.transport.PeerListener() {
                @Override public void onConnected(PeerId peer) { }
                @Override public void onDisconnected(PeerId peer) { }
            };

    @AfterEach
    void tearDown() {
        if (client != null) client.close();
        if (server != null) server.close();
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void reliableOrderedSurvivesLossAndReorder() throws Exception {
        int n = 25;
        List<Integer> received = new CopyOnWriteArrayList<>();
        CountDownLatch allReceived = new CountDownLatch(n);

        // 25% loss, latency with heavy jitter -> frequent drops AND reordering.
        PeerId peer = connect(new SimParams(15, 10, 0.25, 0.0, 7),
                (p, channel, payload) -> { received.add(toInt(payload)); allReceived.countDown(); });

        for (int i = 0; i < n; i++) {
            client.send(peer, 0, DeliveryMode.RELIABLE_ORDERED, ByteBuffer.wrap(intBytes(i)));
        }

        assertTrue(allReceived.await(25, TimeUnit.SECONDS),
                "every reliable message must arrive despite loss; got " + received.size() + "/" + n);
        List<Integer> expected = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            expected.add(i);
        }
        assertEquals(expected, received, "reliable-ordered must deliver strictly in order");
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void unreliableSequencedNeverDeliversStale() throws Exception {
        int n = 30;
        List<Integer> received = new CopyOnWriteArrayList<>();
        CountDownLatch sawLast = new CountDownLatch(1);

        // No loss, but heavy jitter reorders arrivals; the last (29) is never stale, so it arrives.
        PeerId peer = connect(new SimParams(20, 30, 0.0, 0.0, 11),
                (p, channel, payload) -> {
                    int v = toInt(payload);
                    received.add(v);
                    if (v == 29) {
                        sawLast.countDown();
                    }
                });

        for (int i = 0; i < n; i++) {
            client.send(peer, 1, DeliveryMode.UNRELIABLE_SEQUENCED, ByteBuffer.wrap(intBytes(i)));
        }

        assertTrue(sawLast.await(20, TimeUnit.SECONDS), "the newest value should arrive");
        // Whatever was delivered must be strictly increasing — a stale (reordered-late) value is dropped.
        for (int i = 1; i < received.size(); i++) {
            assertTrue(received.get(i) > received.get(i - 1),
                    "sequenced delivery must be strictly forward; got " + received);
        }
    }
}
