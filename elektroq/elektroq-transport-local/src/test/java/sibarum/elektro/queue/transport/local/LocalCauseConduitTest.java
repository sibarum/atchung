package sibarum.elektro.queue.transport.local;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import sibarum.elektro.queue.DefaultConduit;
import sibarum.elektro.queue.message.ArrayMessageRegistry;
import sibarum.elektro.queue.message.MessageRegistry;
import sibarum.elektro.queue.message.MessageType;
import sibarum.elektro.queue.message.PeerId;
import sibarum.elektro.queue.message.Protocol;
import sibarum.elektro.queue.transport.PeerListener;
import sibarum.elektro.queue.wire.Codec;
import sibarum.elektro.queue.wire.WireBufferReader;
import sibarum.elektro.queue.wire.WireBufferWriter;
import sibarum.elektro.queue.wire.WireReader;
import sibarum.elektro.queue.wire.WireWriter;
import sibarum.intrabuf.Compat;
import sibarum.intrabuf.Header;
import sibarum.intrabuf.IbReader;
import sibarum.intrabuf.IbWriter;
import sibarum.probe.Cause;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A cause crosses a conduit with its frame, both ways, and is never sent to a peer that could not read it: a frame is
 * delivered inside the cause it was sent in, and a peer from before causes gets exactly the frames it always did.
 */
class LocalCauseConduitTest {

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

    private static final Cause.Source COMMAND = Cause.source("test.elektroq.command", Cause.Kind.COMMAND);
    private static final AtomicInteger ENDPOINTS = new AtomicInteger();

    private DefaultConduit server;
    private DefaultConduit client;
    private LocalTransport old;

    private static MessageRegistry registry() {
        MessageRegistry registry = new ArrayMessageRegistry();
        registry.register(GREETING);
        registry.register(ASK);
        registry.register(ANSWER);
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
        if (old != null) {
            old.close();
        }
    }

    @Test
    void aCauseCrossesTheConduitAndComesBack() throws Exception {
        String endpoint = "cause-" + ENDPOINTS.incrementAndGet();
        server = new DefaultConduit("server", LocalTransport.listening(endpoint), registry());
        server.start().toCompletableFuture().get(5, TimeUnit.SECONDS);
        client = new DefaultConduit("client", LocalTransport.connecting(endpoint), registry());
        client.start().toCompletableFuture().get(5, TimeUnit.SECONDS);

        AtomicLong onServer = new AtomicLong();
        server.subscribe(ASK, (ask, context) -> {
            onServer.set(Cause.current());
            // A reply, and a further emit, both made inside the cause the request brought.
            context.reply(ANSWER, new Answer("re: " + ask.question()));
            server.action(GREETING).emit(new Greeting("done"));
        });
        AtomicLong onClient = new AtomicLong();
        CountDownLatch greeted = new CountDownLatch(1);
        client.subscribe(GREETING, (greeting, context) -> {
            onClient.set(Cause.current());
            greeted.countDown();
        });

        // The hellos cross as the two connect, and the local transport keeps each direction in order, so after one
        // round trip both ends know the other reads causes. Before that, a frame goes without.
        client.action(ASK).request(new Ask("warm up"), PeerId.BROADCAST, ANSWER).toCompletableFuture()
                .get(5, TimeUnit.SECONDS);
        assertTrue(greeted.await(5, TimeUnit.SECONDS));
        CountDownLatch again = new CountDownLatch(1);
        client.subscribe(GREETING, (greeting, context) -> again.countDown());

        long cause = COMMAND.next();
        AtomicReference<Answer> answer = new AtomicReference<>();
        Cause.within(cause, () -> {
            try {
                answer.set(client.action(ASK).request(new Ask("q"), PeerId.BROADCAST, ANSWER)
                        .toCompletableFuture().get(5, TimeUnit.SECONDS));
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
        assertTrue(again.await(5, TimeUnit.SECONDS));

        assertEquals(new Answer("re: q"), answer.get());
        assertEquals(cause, onServer.get(), "the server's actor ran inside the client's cause");
        assertEquals(cause, onClient.get(), "and what it sent back in that cause arrived in it");
        assertEquals(Cause.NONE, Cause.current());
    }

    /**
     * A peer from before causes: a bare transport that reads frames as version 2 did, with nothing of this build's
     * envelope. It must read every frame a conduit sends it, the hello included, without one byte out of place.
     */
    @Test
    void aPeerFromBeforeCausesIsNeverSentOne() throws Exception {
        String endpoint = "old-" + ENDPOINTS.incrementAndGet();
        LinkedBlockingQueue<byte[]> received = new LinkedBlockingQueue<>();
        AtomicReference<PeerId> conduitSeenFromOld = new AtomicReference<>();
        old = LocalTransport.listening(endpoint);
        old.onFrame((source, frame) -> {
            byte[] bytes = new byte[frame.remaining()];
            frame.get(bytes);
            received.add(bytes);
        });
        old.onPeer(new PeerListener() {
            @Override public void onConnected(PeerId peer) { conduitSeenFromOld.set(peer); }
            @Override public void onDisconnected(PeerId peer) { }
        });
        old.start().toCompletableFuture().get(5, TimeUnit.SECONDS);

        client = new DefaultConduit("client", LocalTransport.connecting(endpoint), registry());
        List<Long> seen = new CopyOnWriteArrayList<>();
        CountDownLatch fromOld = new CountDownLatch(1);
        client.subscribe(GREETING, (greeting, context) -> {
            seen.add(Cause.current());
            fromOld.countDown();
        });
        client.start().toCompletableFuture().get(5, TimeUnit.SECONDS);

        // The old peer speaks first, as version 2 wrote a frame: its envelope says nothing about causes.
        old.send(conduitSeenFromOld.get(), ByteBuffer.wrap(Before.frame(GREETING, new Greeting("from before"))));
        assertTrue(fromOld.await(5, TimeUnit.SECONDS));
        assertEquals(List.of(Cause.NONE), seen, "a frame that brings no cause is delivered in none");

        Cause.within(COMMAND.next(), () -> {
            client.action(GREETING).emit(new Greeting("broadcast"));
            client.action(GREETING).emit(new Greeting("direct"), firstPeer(client));
        });

        // The hello: a type the old peer has nothing under, so it drops it, having read it correctly.
        IbReader hello = new IbReader(take(received));
        assertEquals(Protocol.HELLO_TYPE, Before.readTypeId(hello));
        assertEquals(0, hello.remaining(), "the hello has no payload");
        assertEquals(new Greeting("broadcast"), Before.read(GREETING, take(received)));
        assertEquals(new Greeting("direct"), Before.read(GREETING, take(received)));
    }

    private static PeerId firstPeer(DefaultConduit conduit) {
        return conduit.peers().iterator().next();
    }

    private static byte[] take(LinkedBlockingQueue<byte[]> frames) throws InterruptedException {
        byte[] frame = frames.poll(5, TimeUnit.SECONDS);
        assertNotNull(frame, "a frame should have arrived");
        return frame;
    }

    /** Version 2's envelope before causes, written out by hand from the generated codec of that time. */
    private static final class Before {

        static <T> byte[] frame(MessageType<T> type, T message) {
            IbWriter w = new IbWriter();
            w.writeBits(0L, 1);
            w.writeInt16(Protocol.MAGIC);
            w.writeInt8((byte) Protocol.VERSION);
            w.writeInt8((byte) Header.of(type.schema()).wireVersion());
            w.writeInt32(type.id());
            w.writeInt16((short) type.schema().major());
            w.writeInt16((short) type.schema().minor());
            w.writeEnum(type.schema().compat());
            w.writeInt64(0L);
            WireBufferWriter payload = new WireBufferWriter();
            type.codec().encode(message, payload);
            w.writeRaw(payload.toByteArray());
            return w.toByteArray();
        }

        static int readTypeId(IbReader r) {
            int at = r.beginBits(1);
            long bits = r.bitsWord(at, 1, 0);
            r.readInt16();
            r.readInt8();
            r.readInt8();
            int typeId = r.readInt32();
            r.readInt16();
            r.readInt16();
            r.readEnum(Compat.values().length, "Compat");
            if ((bits & 0x2) != 0) {
                r.readFixed32();
            }
            r.readInt64();
            return typeId;
        }

        /** A whole frame as an old peer decodes it: the payload must be exactly the rest, as it always was. */
        static <T> T read(MessageType<T> type, byte[] frame) {
            IbReader r = new IbReader(frame);
            assertEquals(type.id(), readTypeId(r));
            WireBufferReader in = new WireBufferReader(frame, r.position(), r.remaining());
            T message = type.codec().decode(in);
            assertEquals(0, in.remaining(), "nothing after the payload for an old peer to misread");
            return message;
        }
    }
}
