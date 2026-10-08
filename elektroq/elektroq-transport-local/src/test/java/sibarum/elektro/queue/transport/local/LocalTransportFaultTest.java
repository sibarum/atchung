package sibarum.elektro.queue.transport.local;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import sibarum.elektro.queue.message.PeerId;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What the deliver loop does when the frame listener fails on it.
 *
 * <p>This case used to be silent: a listener that threw ended the transport's single deliver loop and the
 * throwable went into the {@code Future} of a submitted task nobody reads, so every later frame to that
 * endpoint simply never arrived. Mirrors the TCP transport's fault suite.
 *
 * <p>It prints a stack trace to stderr on the way past. That is the point of it; the noise is the fix.
 */
class LocalTransportFaultTest {

    private LocalTransport server;
    private LocalTransport client;

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
     * <p>Before the fix the first frame killed the deliver loop and the second was queued forever.
     */
    @Test
    void listenerFaultDoesNotKillTheDeliverLoop() throws Exception {
        String endpoint = "fault-" + UUID.randomUUID();
        List<String> seen = new CopyOnWriteArrayList<>();
        CountDownLatch second = new CountDownLatch(1);

        server = LocalTransport.listening(endpoint);
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

        client = LocalTransport.connecting(endpoint);
        client.start().toCompletableFuture().get(5, TimeUnit.SECONDS);

        client.send(PeerId.BROADCAST, ByteBuffer.wrap("boom".getBytes(StandardCharsets.UTF_8)));
        client.send(PeerId.BROADCAST, ByteBuffer.wrap("after".getBytes(StandardCharsets.UTF_8)));

        assertTrue(second.await(5, TimeUnit.SECONDS),
                "the frame after a throwing listener should still be delivered");
        assertEquals(List.of("boom", "after"), seen);
    }
}
