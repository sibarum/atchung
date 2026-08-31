package sibarum.atchung;

import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A per-thread drain point for pumped subscriptions. Register handlers with {@link #subscribe}, then
 * call {@link #drain()} on one owner thread (e.g. a render/UI thread, once per frame) to deliver all
 * queued events there. This is how a single-threaded consumer pulls realtime events produced by any
 * number of publisher threads without giving up thread affinity.
 *
 * <p>{@code drain()} must be called from a single thread. {@code subscribe}/{@code close} are safe
 * from any thread.
 */
public final class Pump {

    private final Atchung bus;
    private final CopyOnWriteArrayList<PumpedReg<?>> regs = new CopyOnWriteArrayList<>();

    Pump(Atchung bus) {
        this.bus = bus;
    }

    /**
     * Subscribe with pumped delivery and the default policy, {@link Backpressure#FAIL}: events queue into a
     * bounded mailbox, are delivered on the thread that calls {@link #drain()}, and an overflow stops the
     * process rather than losing one.
     *
     * <p>This is the overload to reach for. Losing events is a decision, and a decision should have to be
     * written down — so the lossy policies are available, and they are available by naming one.
     */
    public <T> Subscription subscribe(Topic<T> topic, Subscriber<T> subscriber, int capacity) {
        return subscribe(topic, subscriber, capacity, Backpressure.FAIL);
    }

    /**
     * Subscribe with pumped delivery: events queue into a bounded mailbox and are delivered on the
     * thread that calls {@link #drain()}.
     *
     * @param capacity     mailbox bound (ignored for {@link Backpressure#COALESCE_LATEST}, which is 1)
     * @param backpressure what happens when the mailbox is full at publish time
     */
    public <T> Subscription subscribe(Topic<T> topic, Subscriber<T> subscriber,
                                      int capacity, Backpressure backpressure) {
        return subscribe(topic, subscriber, capacity, backpressure, null);
    }

    /**
     * Subscribe with pumped delivery and a {@link Fold}: the mailbox holds cells rather than events, so a write
     * supersedes the queued write to the same cell instead of queueing beside it.
     *
     * <p>The bound then counts <em>cells</em>, which is the number worth bounding — how much has changed since
     * the last drain, rather than how many times the producer said so. Read {@link Fold} before using this: it
     * carries the condition on the consumer that makes folding lossless, and a channel that does not meet it
     * must not fold.
     *
     * @param fold which cell each event writes, or {@code null} for an ordinary mailbox
     */
    public <T> Subscription subscribe(Topic<T> topic, Subscriber<T> subscriber,
                                      int capacity, Backpressure backpressure, Fold<T> fold) {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(subscriber, "subscriber");
        Objects.requireNonNull(backpressure, "backpressure");

        PumpedReg<T> reg = new PumpedReg<>(topic, subscriber, capacity, backpressure, fold);
        regs.add(reg);
        Subscription busSub = bus.register(reg);
        return new PumpSubscription(reg, busSub);
    }

    /**
     * Deliver all queued events across this pump's subscriptions, on the calling thread.
     *
     * @return the total number of events delivered this drain
     */
    public int drain() {
        int delivered = 0;
        for (PumpedReg<?> reg : regs) {
            delivered += reg.drain();
        }
        return delivered;
    }

    /** @return whether any subscription still holds queued events. */
    public boolean hasPending() {
        for (PumpedReg<?> reg : regs) {
            if (reg.hasQueued()) {
                return true;
            }
        }
        return false;
    }

    private final class PumpSubscription implements Subscription {
        private final PumpedReg<?> reg;
        private final Subscription busSub;
        private volatile boolean active = true;

        PumpSubscription(PumpedReg<?> reg, Subscription busSub) {
            this.reg = reg;
            this.busSub = busSub;
        }

        @Override
        public boolean isActive() {
            return active;
        }

        @Override
        public void pause() {
            busSub.pause();
        }

        @Override
        public void resume() {
            busSub.resume();
        }

        @Override
        public boolean isPaused() {
            return busSub.isPaused();
        }

        @Override
        public void close() {
            if (!active) {
                return;
            }
            active = false;
            busSub.close();
            regs.remove(reg);
        }
    }
}
