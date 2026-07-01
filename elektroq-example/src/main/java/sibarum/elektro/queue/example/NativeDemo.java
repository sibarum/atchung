package sibarum.elektro.queue.example;

import sibarum.elektro.queue.DefaultConduit;
import sibarum.elektro.queue.generated.ElektroRegistrar;
import sibarum.elektro.queue.message.ArrayMessageRegistry;
import sibarum.elektro.queue.message.MessageRegistry;
import sibarum.elektro.queue.message.PeerId;
import sibarum.elektro.queue.transport.tcp.TcpTransport;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Self-verifying end-to-end demo that doubles as the native-image smoke test.
 *
 * <p>It stands up a server and a client {@link DefaultConduit} over a real loopback TCP
 * connection, using codecs and a registrar generated at compile time by the elektro-Q
 * annotation processor (so this binary proves the whole stack &mdash; codegen, wire,
 * conduit, transport &mdash; composes with no reflection). Every step is checked; the
 * process exits {@code 0} only after printing {@code ELEKTROQ-NATIVE-OK}, and exits
 * {@code 1} on the first failed assertion.
 */
public final class NativeDemo {

    public static void main(String[] args) throws Exception {
        MessageRegistry serverRegistry = new ArrayMessageRegistry();
        ElektroRegistrar.registerAll(serverRegistry);
        check(serverRegistry.size() == 3, "registrar registered all 3 message types");

        MessageRegistry clientRegistry = new ArrayMessageRegistry();
        ElektroRegistrar.registerAll(clientRegistry);

        TcpTransport serverTransport = TcpTransport.listening(0);
        DefaultConduit server = new DefaultConduit("server", serverTransport, serverRegistry);
        server.start().toCompletableFuture().get(5, TimeUnit.SECONDS);
        int port = serverTransport.boundPort();

        DefaultConduit client = new DefaultConduit(
                "client", TcpTransport.connecting("127.0.0.1", port), clientRegistry, Duration.ofSeconds(2));
        client.start().toCompletableFuture().get(5, TimeUnit.SECONDS);

        try {
            runEmit(server, client);
            runRequestReply(server, client);
        } finally {
            client.close();
            server.close();
        }

        System.out.println("ELEKTROQ-NATIVE-OK");
    }

    private static void runEmit(DefaultConduit server, DefaultConduit client) throws Exception {
        CountDownLatch delivered = new CountDownLatch(1);
        AtomicReference<Greeting> seen = new AtomicReference<>();
        server.subscribe(GreetingCodec.TYPE, (message, context) -> {
            seen.set(message);
            delivered.countDown();
        });

        client.action(GreetingCodec.TYPE).emit(new Greeting("hello from native"));

        check(delivered.await(5, TimeUnit.SECONDS), "emit reached the remote actor");
        check(new Greeting("hello from native").equals(seen.get()), "emitted payload survived the wire");
    }

    private static void runRequestReply(DefaultConduit server, DefaultConduit client) throws Exception {
        server.subscribe(PingCodec.TYPE, (ping, context) ->
                context.reply(PongCodec.TYPE, new Pong(ping.seq(), "ack")));

        PeerId serverPeer = client.peers().iterator().next();
        Pong pong = client.action(PingCodec.TYPE)
                .request(new Ping(42L), serverPeer, PongCodec.TYPE)
                .toCompletableFuture()
                .get(5, TimeUnit.SECONDS);

        check(pong.seq() == 42L, "reply correlated to the request sequence");
        check("ack".equals(pong.note()), "reply payload survived the wire");
    }

    private static void check(boolean condition, String what) {
        if (!condition) {
            System.err.println("FAIL: " + what);
            System.exit(1);
        }
        System.out.println("ok: " + what);
    }

    private NativeDemo() {
    }
}
