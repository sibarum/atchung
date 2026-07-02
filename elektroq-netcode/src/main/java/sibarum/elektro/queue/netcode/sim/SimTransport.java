package sibarum.elektro.queue.netcode.sim;

import sibarum.elektro.queue.message.PeerId;
import sibarum.elektro.queue.transport.FrameListener;
import sibarum.elektro.queue.transport.PeerListener;
import sibarum.elektro.queue.transport.Transport;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link Transport} decorator that impairs a link &mdash; latency, jitter, loss, and duplication
 * &mdash; so netcode can be exercised against adverse conditions deterministically (docs/netcode-design.md).
 *
 * <p>Netcode is untestable without a controllable network. Wrapping the in-VM transport with a
 * {@code SimTransport} yields a link with exactly the loss and delay you dial in, with reproducible
 * draws from a seeded RNG &mdash; no real sockets, no flaky timing on the decision path. It is
 * transport-agnostic: it can equally sit in front of the real {@code UdpTransport} to add impairment
 * on top of the genuine network.
 *
 * <p>Impairment is applied on the <b>egress</b> (send) side: each frame is delayed, maybe dropped,
 * maybe duplicated, then handed to the wrapped transport. Inbound frames and peer events pass through
 * untouched. Two endpoints each wrapping their own transport therefore impair their own send path,
 * modelling an asymmetric link naturally.
 */
public final class SimTransport implements Transport {

    private final Transport delegate;
    private final SimParams params;
    private final Random random;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> Thread.ofVirtual().unstarted(r));

    private final AtomicLong offered = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();
    private final AtomicLong duplicated = new AtomicLong();

    private volatile boolean closed;
    private volatile boolean blackhole;

    public SimTransport(Transport delegate, SimParams params) {
        this.delegate = delegate;
        this.params = params;
        this.random = new Random(params.seed());
    }

    /**
     * Drops <em>all</em> outbound frames while enabled &mdash; simulates the link going dead, so a
     * higher layer's keepalive/timeout can be exercised. Distinct from {@code lossProbability}, which
     * is a steady random drop rate.
     */
    public void blackhole(boolean enabled) {
        this.blackhole = enabled;
    }

    /** A point-in-time snapshot of what impairment this link applied. */
    public SimStats stats() {
        return new SimStats(offered.get(), dropped.get(), duplicated.get());
    }

    @Override
    public CompletionStage<Void> start() {
        return delegate.start();
    }

    @Override
    public void send(PeerId destination, ByteBuffer frame) {
        offered.incrementAndGet();
        byte[] bytes = new byte[frame.remaining()];
        frame.get(bytes);

        if (blackhole) {
            dropped.incrementAndGet();
            return;
        }

        boolean loss;
        boolean duplicate;
        long delay;
        long dupDelay;
        synchronized (random) {
            loss = random.nextDouble() < params.lossProbability();
            duplicate = random.nextDouble() < params.duplicateProbability();
            delay = nextDelay();
            dupDelay = nextDelay();
        }

        if (loss) {
            dropped.incrementAndGet();
            return;
        }
        schedule(destination, bytes, delay);
        if (duplicate) {
            duplicated.incrementAndGet();
            schedule(destination, bytes, dupDelay);
        }
    }

    @Override
    public void onFrame(FrameListener listener) {
        delegate.onFrame(listener);
    }

    @Override
    public void onPeer(PeerListener listener) {
        delegate.onPeer(listener);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        scheduler.shutdownNow();
        delegate.close();
    }

    // --- internals --------------------------------------------------------------

    private long nextDelay() {
        long jitter = params.jitterMillis() == 0
                ? 0
                : (long) ((random.nextDouble() * 2.0 - 1.0) * params.jitterMillis());
        return Math.max(0, params.latencyMillis() + jitter);
    }

    private void schedule(PeerId destination, byte[] bytes, long delayMillis) {
        Runnable deliver = () -> {
            if (!closed) {
                delegate.send(destination, ByteBuffer.wrap(bytes));
            }
        };
        if (delayMillis <= 0) {
            deliver.run();
        } else {
            scheduler.schedule(deliver, delayMillis, TimeUnit.MILLISECONDS);
        }
    }
}
