package sibarum.elektro.queue.transport.udp;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import sibarum.elektro.queue.DefaultConduit;
import sibarum.elektro.queue.message.ArrayMessageRegistry;
import sibarum.elektro.queue.message.MessageRegistry;
import sibarum.elektro.queue.message.MessageType;
import sibarum.elektro.queue.wire.Codec;
import sibarum.elektro.queue.wire.WireReader;
import sibarum.elektro.queue.wire.WireWriter;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end over a real UDP loopback socket: a client emits a datagram that a server actor reacts
 * to, and the transport's counters reflect the traffic. Delivery is unreliable in general, but a
 * single small datagram over loopback is not dropped in practice, so this exercises the happy path
 * of layer 0.
 */
class UdpConduitTest {

    record Greeting(String text) { }

    private static final MessageType<Greeting> GREETING = new MessageType<>(
            10, 1, "Greeting", Greeting.class, new Codec<>() {
        @Override public void encode(Greeting v, WireWriter out) { out.putString(v.text()); }
        @Override public Greeting decode(WireReader in) { return new Greeting(in.getString()); }
    });

    private UdpTransport serverTransport;
    private DefaultConduit server;
    private DefaultConduit client;

    private static MessageRegistry registry() {
        MessageRegistry registry = new ArrayMessageRegistry();
        registry.register(GREETING);
        return registry;
    }

    @AfterEach
    void tearDown() {
        if (client != null) client.close();
        if (server != null) server.close();
    }

    @Test
    void emitReachesRemoteActorOverUdp() throws Exception {
        serverTransport = UdpTransport.listening(0);
        server = new DefaultConduit("server", serverTransport, registry());
        server.start().toCompletableFuture().get(5, TimeUnit.SECONDS);
        int port = serverTransport.boundPort();

        client = new DefaultConduit(
                "client", UdpTransport.connecting("127.0.0.1", port), registry());
        client.start().toCompletableFuture().get(5, TimeUnit.SECONDS);

        CountDownLatch received = new CountDownLatch(1);
        AtomicReference<Greeting> seen = new AtomicReference<>();
        server.subscribe(GREETING, (message, ctx) -> {
            seen.set(message);
            received.countDown();
        });

        client.action(GREETING).emit(new Greeting("hello over udp"));

        assertTrue(received.await(5, TimeUnit.SECONDS), "server actor should react to the datagram");
        assertEquals(new Greeting("hello over udp"), seen.get());
        assertTrue(serverTransport.stats().packetsReceived() >= 1, "server should count the datagram");
    }
}
