package sibarum.elektro.queue.transport.local;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import sibarum.elektro.queue.DefaultConduit;
import sibarum.elektro.queue.Subscription;
import sibarum.elektro.queue.message.ArrayMessageRegistry;
import sibarum.elektro.queue.message.MessageRegistry;
import sibarum.elektro.queue.message.MessageType;
import sibarum.elektro.queue.message.PeerId;
import sibarum.elektro.queue.wire.Codec;
import sibarum.elektro.queue.wire.WireReader;
import sibarum.elektro.queue.wire.WireWriter;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end tests over the in-VM {@link LocalTransport}: two conduits, real framing and codecs,
 * no sockets. Mirrors the TCP conduit suite to prove the transports are behaviourally
 * interchangeable behind the SPI.
 */
class LocalConduitTest {

    record Greeting(String text) { }

    record Ask(String question) { }

    record Answer(String value) { }

    private static final MessageType<Greeting> GREETING = new MessageType<>(
            10, 1, "Greeting", Greeting.class, new Codec<>() {
        @Override public void encode(Greeting v, WireWriter out) { out.putString(v.text()); }
        @Override public Greeting decode(WireReader in) { return new Greeting(in.getString()); }
    });

    private static final MessageType<Ask> ASK = new MessageType<>(
            11, 1, "Ask", Ask.class, new Codec<>() {
        @Override public void encode(Ask v, WireWriter out) { out.putString(v.question()); }
        @Override public Ask decode(WireReader in) { return new Ask(in.getString()); }
    });

    private static final MessageType<Answer> ANSWER = new MessageType<>(
            12, 1, "Answer", Answer.class, new Codec<>() {
        @Override public void encode(Answer v, WireWriter out) { out.putString(v.value()); }
        @Override public Answer decode(WireReader in) { return new Answer(in.getString()); }
    });

    // A fresh endpoint name per connect(), so the process-wide switch never collides across tests.
    private static final AtomicInteger ENDPOINTS = new AtomicInteger();

    private DefaultConduit server;
    private DefaultConduit client;

    private static MessageRegistry registry() {
        MessageRegistry registry = new ArrayMessageRegistry();
        registry.register(GREETING);
        registry.register(ASK);
        registry.register(ANSWER);
        return registry;
    }

    private void connect() throws Exception {
        connect(null);
    }

    private void connect(Duration clientRequestTimeout) throws Exception {
        String endpoint = "test-endpoint-" + ENDPOINTS.incrementAndGet();
        server = new DefaultConduit("server", LocalTransport.listening(endpoint), registry());
        server.start().toCompletableFuture().get(5, TimeUnit.SECONDS);

        client = new DefaultConduit(
                "client", LocalTransport.connecting(endpoint), registry(), clientRequestTimeout);
        client.start().toCompletableFuture().get(5, TimeUnit.SECONDS);
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
    void dialingUnboundEndpointFails() {
        DefaultConduit lonely = new DefaultConduit(
                "client", LocalTransport.connecting("nobody-here"), registry());
        assertThrows(ExecutionException.class,
                () -> lonely.start().toCompletableFuture().get(5, TimeUnit.SECONDS));
    }

    @Test
    void emitReachesRemoteActor() throws Exception {
        connect();
        CountDownLatch received = new CountDownLatch(1);
        AtomicReference<Greeting> seen = new AtomicReference<>();
        server.subscribe(GREETING, (message, context) -> {
            seen.set(message);
            received.countDown();
        });

        client.action(GREETING).emit(new Greeting("hello in-vm"));

        assertTrue(received.await(5, TimeUnit.SECONDS), "actor should have reacted");
        assertEquals(new Greeting("hello in-vm"), seen.get());
    }

    @Test
    void emitReachesServerToClientToo() throws Exception {
        connect();
        CountDownLatch received = new CountDownLatch(1);
        AtomicReference<Greeting> seen = new AtomicReference<>();
        client.subscribe(GREETING, (message, context) -> {
            seen.set(message);
            received.countDown();
        });

        server.action(GREETING).emit(new Greeting("downstream"));

        assertTrue(received.await(5, TimeUnit.SECONDS), "client actor should react to server emit");
        assertEquals(new Greeting("downstream"), seen.get());
    }

    @Test
    void requestGetsCorrelatedReply() throws Exception {
        connect();
        server.subscribe(ASK, (ask, context) ->
                context.reply(ANSWER, new Answer("re: " + ask.question())));

        PeerId serverPeer = client.peers().iterator().next();
        CompletableFuture<Answer> reply =
                client.action(ASK).request(new Ask("status?"), serverPeer, ANSWER).toCompletableFuture();

        assertEquals(new Answer("re: status?"), reply.get(5, TimeUnit.SECONDS));
    }

    @Test
    void unsubscribedActorStopsReacting() throws Exception {
        connect();
        CountDownLatch first = new CountDownLatch(1);
        Subscription subscription = server.subscribe(GREETING, (message, context) -> first.countDown());

        client.action(GREETING).emit(new Greeting("one"));
        assertTrue(first.await(5, TimeUnit.SECONDS));

        subscription.close();
        assertTrue(!subscription.isActive());
    }

    @Test
    void pendingRequestFailsWhenPeerDisconnects() throws Exception {
        connect(); // no timeout: the failure must come from the disconnect
        server.subscribe(ASK, (ask, context) -> { }); // never replies

        PeerId serverPeer = client.peers().iterator().next();
        CompletableFuture<Answer> reply =
                client.action(ASK).request(new Ask("still there?"), serverPeer, ANSWER).toCompletableFuture();

        server.close(); // drop the link out from under the in-flight request

        assertThrows(ExecutionException.class, () -> reply.get(5, TimeUnit.SECONDS));
    }

    @Test
    void requestToUnknownPeerFailsFast() throws Exception {
        connect();
        CompletableFuture<Answer> reply = client.action(ASK)
                .request(new Ask("hi"), new PeerId(9999), ANSWER)
                .toCompletableFuture();

        assertThrows(ExecutionException.class, () -> reply.get(5, TimeUnit.SECONDS));
    }
}
