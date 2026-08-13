package sibarum.atchung.elektroq;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import sibarum.atchung.Atchung;
import sibarum.atchung.Topic;
import sibarum.elektro.queue.Conduit;
import sibarum.elektro.queue.message.ArrayMessageRegistry;
import sibarum.elektro.queue.message.MessageRegistry;
import sibarum.elektro.queue.message.MessageType;
import sibarum.elektro.queue.transport.local.ElektroLocal;
import sibarum.elektro.queue.wire.Codec;
import sibarum.elektro.queue.wire.WireReader;
import sibarum.elektro.queue.wire.WireWriter;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ElektroBridgeTest {

    /** A minimal message with a hand-written codec, so the test needs no annotation processor. */
    record Note(String text) {}

    private static final Codec<Note> NOTE_CODEC = new Codec<>() {
        @Override public void encode(Note value, WireWriter out) {
            out.putString(value.text());
        }
        @Override public Note decode(WireReader in) {
            return new Note(in.getString());
        }
    };

    private static final MessageType<Note> NOTE_TYPE =
            new MessageType<>(1, 1, "Note", Note.class, NOTE_CODEC);
    private static final Topic<Note> NOTE_TOPIC = Topic.of("note", Note.class);

    private Conduit server;
    private Conduit client;
    private ElektroBridge serverBridge;
    private ElektroBridge clientBridge;

    private static MessageRegistry registry() {
        MessageRegistry registry = new ArrayMessageRegistry();
        registry.register(NOTE_TYPE);
        return registry;
    }

    private void connect(String endpoint) {
        server = ElektroLocal.server("server", endpoint, registry());
        server.start().toCompletableFuture().join();
        client = ElektroLocal.client("client", endpoint, registry());
        client.start().toCompletableFuture().join();
    }

    @AfterEach
    void tearDown() {
        if (clientBridge != null) clientBridge.close();
        if (serverBridge != null) serverBridge.close();
        if (client != null) client.close();
        if (server != null) server.close();
    }

    @Test
    void localPublishCrossesTheWireToTheRemoteBus() throws InterruptedException {
        connect("bridge-forward");
        Atchung clientBus = Atchung.create();
        Atchung serverBus = Atchung.create();

        clientBridge = new ElektroBridge(clientBus, client).outbound(NOTE_TOPIC, NOTE_TYPE);
        serverBridge = new ElektroBridge(serverBus, server).inbound(NOTE_TOPIC, NOTE_TYPE);

        BlockingQueue<Note> received = new ArrayBlockingQueue<>(4);
        serverBus.subscribe(NOTE_TOPIC, received::add);

        clientBus.publish(NOTE_TOPIC, new Note("hello over the wire"));

        Note delivered = received.poll(2, TimeUnit.SECONDS);
        assertNotNull(delivered, "event published locally should arrive on the remote bus");
        assertEquals("hello over the wire", delivered.text());
    }

    @Test
    void bidirectionalBridgeDoesNotEchoBack() throws InterruptedException {
        connect("bridge-loopguard");
        Atchung clientBus = Atchung.create();
        Atchung serverBus = Atchung.create();

        // Both ends bridge both directions — the classic setup that would loop without a guard.
        clientBridge = new ElektroBridge(clientBus, client).bridge(NOTE_TOPIC, NOTE_TYPE);
        serverBridge = new ElektroBridge(serverBus, server).bridge(NOTE_TOPIC, NOTE_TYPE);

        AtomicInteger originCount = new AtomicInteger();
        BlockingQueue<Note> remote = new ArrayBlockingQueue<>(4);
        clientBus.subscribe(NOTE_TOPIC, n -> originCount.incrementAndGet());
        serverBus.subscribe(NOTE_TOPIC, remote::add);

        clientBus.publish(NOTE_TOPIC, new Note("no echoes please"));

        // Forward delivery happens exactly once...
        Note delivered = remote.poll(2, TimeUnit.SECONDS);
        assertNotNull(delivered, "the event should reach the remote bus once");
        assertEquals("no echoes please", delivered.text());

        // ...and nothing bounces back: give any echo time to arrive, then confirm it never did.
        Thread.sleep(200);
        assertEquals(1, originCount.get(), "origin saw only its own publish, no echo from the wire");
        assertEquals(0, remote.size(), "remote received the message once, with no duplicates");
    }
}
