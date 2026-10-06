package sibarum.elektro.queue.transport.local;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import sibarum.elektro.queue.DefaultConduit;
import sibarum.elektro.queue.message.ArrayMessageRegistry;
import sibarum.elektro.queue.message.MessageRegistry;
import sibarum.elektro.queue.message.MessageType;
import sibarum.elektro.queue.message.PeerId;
import sibarum.elektro.queue.wire.Codec;
import sibarum.elektro.queue.wire.WireReader;
import sibarum.elektro.queue.wire.WireWriter;
import sibarum.intrabuf.Compat;
import sibarum.intrabuf.annotation.Field;
import sibarum.intrabuf.annotation.IntrabufMessage;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Intrabuf-backed message types through real conduits: alongside {@code @Message}-style types in one registry, in
 * requests and replies, and between peers on different versions of a schema.
 */
class LocalIntrabufConduitTest {

    record Ask(String question) { }

    /** A hand-written elektro-Q codec type, standing in for a {@code @Message} record. */
    private static final MessageType<Ask> ASK = new MessageType<>(11, 1, "Ask", Ask.class, new Codec<>() {
        @Override public void encode(Ask v, WireWriter out) { out.putString(v.question()); }
        @Override public Ask decode(WireReader in) { return new Ask(in.getString()); }
    });

    @IntrabufMessage(name = "test.Telemetry", id = 20, compat = Compat.BACKWARD)
    record TelemetryV0(String node, List<Double> samples) { }

    @IntrabufMessage(name = "test.Telemetry", id = 20, minor = 1, compat = Compat.BACKWARD)
    record TelemetryV1(String node, List<Double> samples, @Field(since = 1, nonNull = true) Map<String, Long> counters) { }

    private static final MessageType<TelemetryV0> TELEMETRY_V0 =
            MessageType.of(LocalIntrabufConduitTest_TelemetryV0Intrabuf.CODEC, TelemetryV0.class);
    private static final MessageType<TelemetryV1> TELEMETRY_V1 =
            MessageType.of(LocalIntrabufConduitTest_TelemetryV1Intrabuf.CODEC, TelemetryV1.class);

    private static final AtomicInteger ENDPOINTS = new AtomicInteger();

    private DefaultConduit server;
    private DefaultConduit client;

    private void connect(MessageType<?> serverTelemetry, MessageType<?> clientTelemetry) throws Exception {
        String endpoint = "intrabuf-endpoint-" + ENDPOINTS.incrementAndGet();
        server = new DefaultConduit("server", LocalTransport.listening(endpoint), registry(serverTelemetry));
        server.start().toCompletableFuture().get(5, TimeUnit.SECONDS);
        client = new DefaultConduit("client", LocalTransport.connecting(endpoint), registry(clientTelemetry));
        client.start().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    private static MessageRegistry registry(MessageType<?> telemetry) {
        MessageRegistry registry = new ArrayMessageRegistry();
        registry.register(ASK);
        registry.register(telemetry);
        return registry;
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.close();
        }
    }

    @Test
    void intrabufMessageIsEmittedAndReceived() throws Exception {
        connect(TELEMETRY_V1, TELEMETRY_V1);
        CountDownLatch received = new CountDownLatch(1);
        AtomicReference<TelemetryV1> seen = new AtomicReference<>();
        server.subscribe(TELEMETRY_V1, (message, context) -> {
            seen.set(message);
            received.countDown();
        });

        TelemetryV1 sent = new TelemetryV1("node-7", List.of(1.5, -2.25), Map.of("frames", 1200L));
        client.action(TELEMETRY_V1).emit(sent);

        assertTrue(received.await(5, TimeUnit.SECONDS), "actor should have reacted");
        assertEquals(sent, seen.get());
    }

    @Test
    void requestOfOneKindIsAnsweredWithTheOther() throws Exception {
        connect(TELEMETRY_V1, TELEMETRY_V1);
        server.subscribe(ASK, (ask, context) ->
                context.reply(TELEMETRY_V1, new TelemetryV1(ask.question(), List.of(), Map.of())));

        PeerId serverPeer = client.peers().iterator().next();
        TelemetryV1 reply = client.action(ASK)
                .request(new Ask("status?"), serverPeer, TELEMETRY_V1)
                .toCompletableFuture()
                .get(5, TimeUnit.SECONDS);

        assertEquals(new TelemetryV1("status?", List.of(), Map.of()), reply);
    }

    @Test
    void newerPeerReadsAnOlderPeersMessage() throws Exception {
        // The client still runs v1.0 of test.Telemetry; the server has upgraded to v1.1. The envelope says which
        // version the client wrote, and intrabuf decodes it with the field the client never had defaulted.
        connect(TELEMETRY_V1, TELEMETRY_V0);
        CountDownLatch received = new CountDownLatch(1);
        AtomicReference<TelemetryV1> seen = new AtomicReference<>();
        server.subscribe(TELEMETRY_V1, (message, context) -> {
            seen.set(message);
            received.countDown();
        });

        client.action(TELEMETRY_V0).emit(new TelemetryV0("old-node", List.of(9.0)));

        assertTrue(received.await(5, TimeUnit.SECONDS), "actor should have reacted");
        assertEquals(new TelemetryV1("old-node", List.of(9.0), Map.of()), seen.get());
    }
}
