package sibarum.elektro.queue.transport.tcp;

import org.junit.jupiter.api.Test;
import sibarum.elektro.queue.ElektroException;
import sibarum.elektro.queue.message.PeerId;
import sibarum.elektro.queue.transport.PeerListener;

import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The TCP twin of {@code UdsHardeningTest}'s send-timeout case: a peer that connects and never reads cannot hold a
 * sender for longer than the timeout.
 */
class TcpSendTimeoutTest {

    @Test
    void aPeerThatStopsReadingIsDroppedAfterTheSendTimeout() throws Exception {
        AtomicReference<PeerId> peer = new AtomicReference<>();
        CountDownLatch connected = new CountDownLatch(1);
        CountDownLatch disconnected = new CountDownLatch(1);
        TcpTransport server = TcpTransport.listening(0).sendTimeout(Duration.ofMillis(300));
        server.onPeer(new PeerListener() {
            @Override
            public void onConnected(PeerId p) {
                peer.set(p);
                connected.countDown();
            }

            @Override
            public void onDisconnected(PeerId p) {
                disconnected.countDown();
            }
        });
        try {
            server.start().toCompletableFuture().get(5, TimeUnit.SECONDS);
            try (Socket stalled = new Socket(InetAddress.getLoopbackAddress(), server.boundPort())) {
                stalled.setReceiveBufferSize(4096);
                assertTrue(connected.await(5, TimeUnit.SECONDS));
                ElektroException e = assertTimeoutPreemptively(Duration.ofSeconds(15), () -> {
                    byte[] chunk = new byte[64 * 1024];
                    while (true) {
                        try {
                            server.send(peer.get(), ByteBuffer.wrap(chunk));
                        } catch (ElektroException thrown) {
                            return thrown;
                        }
                    }
                });
                assertTrue(e.getMessage().contains("timed out after 300 ms; the peer was dropped"), e.getMessage());
                assertTrue(disconnected.await(5, TimeUnit.SECONDS), "and the peer is reported gone");
            }
        } finally {
            server.close();
        }
    }
}
