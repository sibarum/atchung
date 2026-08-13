package sibarum.elektro.queue.netcode.sim;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import sibarum.elektro.queue.DefaultConduit;
import sibarum.elektro.queue.message.ArrayMessageRegistry;
import sibarum.elektro.queue.message.MessageRegistry;
import sibarum.elektro.queue.message.MessageType;
import sibarum.elektro.queue.transport.local.LocalTransport;
import sibarum.elektro.queue.wire.Codec;
import sibarum.elektro.queue.wire.WireReader;
import sibarum.elektro.queue.wire.WireWriter;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the network simulator over the deterministic in-VM transport: a perfect link delivers, a
 * total-loss link drops, and a latency link delays &mdash; all without real sockets, so the behaviour
 * is controllable and the loss/duplicate draws are reproducible.
 */
class SimTransportTest {

    record Msg(String text) { }

    private static final MessageType<Msg> MSG = new MessageType<>(
            10, 1, "Msg", Msg.class, new Codec<>() {
        @Override public void encode(Msg v, WireWriter out) { out.putString(v.text()); }
        @Override public Msg decode(WireReader in) { return new Msg(in.getString()); }
    });

    private static final AtomicInteger ENDPOINTS = new AtomicInteger();

    private DefaultConduit server;
    private DefaultConduit client;
    private SimTransport clientSim;

    private static MessageRegistry registry() {
        MessageRegistry registry = new ArrayMessageRegistry();
        registry.register(MSG);
        return registry;
    }

    /** Wires server (plain in-VM) + client (in-VM behind a SimTransport with {@code params}). */
    private CountDownLatch connect(SimParams params) throws Exception {
        String endpoint = "sim-ep-" + ENDPOINTS.incrementAndGet();
        server = new DefaultConduit("server", LocalTransport.listening(endpoint), registry());
        server.start().toCompletableFuture().get(5, TimeUnit.SECONDS);

        clientSim = new SimTransport(LocalTransport.connecting(endpoint), params);
        client = new DefaultConduit("client", clientSim, registry());
        client.start().toCompletableFuture().get(5, TimeUnit.SECONDS);

        CountDownLatch received = new CountDownLatch(1);
        server.subscribe(MSG, (m, ctx) -> received.countDown());
        return received;
    }

    @AfterEach
    void tearDown() {
        if (client != null) client.close();
        if (server != null) server.close();
    }

    @Test
    void perfectLinkDelivers() throws Exception {
        CountDownLatch received = connect(SimParams.perfect());
        client.action(MSG).emit(new Msg("hi"));
        assertTrue(received.await(2, TimeUnit.SECONDS), "a perfect link should deliver");
        assertEquals(0, clientSim.stats().dropped());
    }

    @Test
    void totalLossDropsEverything() throws Exception {
        CountDownLatch received = connect(SimParams.lossy(1.0, 42L));
        client.action(MSG).emit(new Msg("gone"));
        assertFalse(received.await(500, TimeUnit.MILLISECONDS), "100% loss should drop the message");
        assertEquals(1, clientSim.stats().offered());
        assertEquals(1, clientSim.stats().dropped());
    }

    @Test
    void latencyDelaysDelivery() throws Exception {
        CountDownLatch received = connect(SimParams.latency(200));
        client.action(MSG).emit(new Msg("slow"));
        assertFalse(received.await(50, TimeUnit.MILLISECONDS), "should not arrive before the latency");
        assertTrue(received.await(2, TimeUnit.SECONDS), "should arrive after the latency");
    }
}
