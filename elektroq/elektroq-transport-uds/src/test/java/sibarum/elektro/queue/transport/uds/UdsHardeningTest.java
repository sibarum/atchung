package sibarum.elektro.queue.transport.uds;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import sibarum.elektro.queue.Conduit;
import sibarum.elektro.queue.DefaultConduit;
import sibarum.elektro.queue.ElektroException;
import sibarum.elektro.queue.message.ArrayMessageRegistry;
import sibarum.elektro.queue.message.PeerId;
import sibarum.elektro.queue.transport.PeerListener;

import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The three things a long-lived listener needs that a spike found missing (atchung
 * {@code docs/spikes/elektroq-inspector-stream.md} §4): a socket file a crash left behind does not block the next
 * bind, a peer that stops reading cannot hold a sender for ever, and an application hears peers come and go.
 */
class UdsHardeningTest {

    private Path dir;
    private Path socket;
    private final List<AutoCloseable> open = new CopyOnWriteArrayList<>();

    private Path socketPath() throws IOException {
        dir = Files.createTempDirectory("eq");   // short: AF_UNIX paths are length-limited
        socket = dir.resolve("s");
        return socket;
    }

    private <T extends AutoCloseable> T keep(T c) {
        open.add(c);
        return c;
    }

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable c : open) {
            c.close();
        }
        if (socket != null) {
            Files.deleteIfExists(socket);
        }
        if (dir != null) {
            try (var left = Files.list(dir)) {
                for (Path p : left.toList()) {
                    Files.deleteIfExists(p);
                }
            }
            Files.deleteIfExists(dir);
        }
    }

    @Test
    void aSocketFileACrashLeftBehindIsReplaced() throws Exception {
        Path path = socketPath();
        // What a crash leaves: a bound socket's file, with nobody listening on it any more. Closing the channel
        // does not unlink the file, which is exactly the state a killed process leaves.
        ServerSocketChannel crashed = ServerSocketChannel.open(StandardProtocolFamily.UNIX);
        crashed.bind(UnixDomainSocketAddress.of(path));
        crashed.close();
        assertTrue(Files.exists(path), "the stale file is there");

        UdsTransport server = keep(UdsTransport.listening(path));
        server.start().toCompletableFuture().get(5, TimeUnit.SECONDS);
        try (SocketChannel client = SocketChannel.open(StandardProtocolFamily.UNIX)) {
            client.connect(UnixDomainSocketAddress.of(path));
            assertTrue(client.isConnected(), "and the new listener answers on it");
        }
    }

    @Test
    void aLiveListenersFileIsNotTaken() throws Exception {
        Path path = socketPath();
        UdsTransport first = keep(UdsTransport.listening(path));
        first.start().toCompletableFuture().get(5, TimeUnit.SECONDS);
        UdsTransport second = keep(UdsTransport.listening(path));
        Throwable failed = assertThrows(Exception.class,
                () -> second.start().toCompletableFuture().get(5, TimeUnit.SECONDS));
        assertTrue(String.valueOf(failed.getCause()).contains("a listener answers"), String.valueOf(failed.getCause()));
        try (SocketChannel client = SocketChannel.open(StandardProtocolFamily.UNIX)) {
            client.connect(UnixDomainSocketAddress.of(path));
            assertTrue(client.isConnected(), "the first listener still has its socket");
        }
    }

    @Test
    void aRegularFileInTheWayIsNeverDeleted() throws Exception {
        Path path = socketPath();
        Files.writeString(path, "not a socket");
        UdsTransport server = keep(UdsTransport.listening(path));
        assertThrows(Exception.class, () -> server.start().toCompletableFuture().get(5, TimeUnit.SECONDS));
        assertEquals("not a socket", Files.readString(path));
    }

    @Test
    void aPeerThatStopsReadingIsDroppedAfterTheSendTimeout() throws Exception {
        Path path = socketPath();
        AtomicReference<PeerId> peer = new AtomicReference<>();
        CountDownLatch connected = new CountDownLatch(1);
        CountDownLatch disconnected = new CountDownLatch(1);
        UdsTransport server = keep(UdsTransport.listening(path).sendTimeout(Duration.ofMillis(300)));
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
        server.start().toCompletableFuture().get(5, TimeUnit.SECONDS);
        SocketChannel stalled = keep(SocketChannel.open(StandardProtocolFamily.UNIX));
        stalled.connect(UnixDomainSocketAddress.of(path));     // connects, and never reads a byte
        assertTrue(connected.await(5, TimeUnit.SECONDS));

        ElektroException e = assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
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

    @Test
    void aConduitTellsItsListenersAsPeersComeAndGoAndAFaultyOneDoesNotStopThem() throws Exception {
        Path path = socketPath();
        Conduit server = keep(new DefaultConduit("server", UdsTransport.listening(path), new ArrayMessageRegistry()));
        List<String> heard = new CopyOnWriteArrayList<>();
        CountDownLatch gone = new CountDownLatch(1);
        server.addPeerListener(new PeerListener() {
            @Override
            public void onConnected(PeerId p) {
                throw new IllegalStateException("a listener with a bug");
            }

            @Override
            public void onDisconnected(PeerId p) {
                throw new IllegalStateException("a listener with a bug");
            }
        });
        PeerListener good = new PeerListener() {
            @Override
            public void onConnected(PeerId p) {
                heard.add("connected " + p.handle() + " peers=" + server.peers().size());
            }

            @Override
            public void onDisconnected(PeerId p) {
                heard.add("disconnected " + p.handle() + " peers=" + server.peers().size());
                gone.countDown();
            }
        };
        server.addPeerListener(good);
        server.start().toCompletableFuture().get(5, TimeUnit.SECONDS);

        Conduit client = new DefaultConduit("client", UdsTransport.connecting(path), new ArrayMessageRegistry());
        client.start().toCompletableFuture().get(5, TimeUnit.SECONDS);
        long until = System.nanoTime() + 5_000_000_000L;
        while (heard.isEmpty() && System.nanoTime() < until) {
            Thread.sleep(10);
        }
        client.close();
        assertTrue(gone.await(5, TimeUnit.SECONDS));
        assertEquals(List.of("connected 1 peers=1", "disconnected 1 peers=0"), heard,
                "after peers() is updated, and the faulty listener before it changed nothing");

        server.removePeerListener(good);
        Conduit again = new DefaultConduit("again", UdsTransport.connecting(path), new ArrayMessageRegistry());
        again.start().toCompletableFuture().get(5, TimeUnit.SECONDS);
        Thread.sleep(200);
        again.close();
        assertEquals(2, heard.size(), "a removed listener hears nothing more");
    }

    @Test
    void startingAConduitOnATakenPathFailsWithTheReason() throws Exception {
        Path path = socketPath();
        keep(UdsTransport.listening(path)).start().toCompletableFuture().get(5, TimeUnit.SECONDS);
        Conduit second = keep(new DefaultConduit("second", UdsTransport.listening(path), new ArrayMessageRegistry()));
        CompletionException e = assertThrows(CompletionException.class,
                () -> second.start().toCompletableFuture().join());
        assertTrue(String.valueOf(e.getCause().getCause()).contains("a listener answers"),
                String.valueOf(e.getCause()));
    }
}
