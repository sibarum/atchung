package sibarum.elektro.queue.transport.uds;

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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end tests over a real Unix-domain-socket connection: two conduits, an actual {@code AF_UNIX}
 * socket, real framing and codecs. Mirrors the TCP conduit tests to prove the transport is a true
 * drop-in behind the SPI.
 */
class UdsConduitTest {

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

    private Path socketDir;
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
        socketDir = Files.createTempDirectory("eq");   // short path — AF_UNIX paths are length-limited
        Path socket = socketDir.resolve("s");
        UdsTransport serverTransport = UdsTransport.listening(socket);
        server = new DefaultConduit("server", serverTransport, registry());
        server.start().toCompletableFuture().get(5, TimeUnit.SECONDS);

        client = new DefaultConduit("client", UdsTransport.connecting(socket), registry());
        client.start().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (client != null) client.close();
        if (server != null) server.close();
        if (socketDir != null) Files.deleteIfExists(socketDir);
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

        client.action(GREETING).emit(new Greeting("hello over uds"));

        assertTrue(received.await(5, TimeUnit.SECONDS), "actor should have reacted");
        assertEquals(new Greeting("hello over uds"), seen.get());
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
}
