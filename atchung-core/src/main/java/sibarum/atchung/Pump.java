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
     * Subscribe with pumped delivery: events queue into a bounded mailbox and are delivered on the
     * thread that calls {@link #drain()}.
     *
     * @param capacity     mailbox bound (ignored for {@link Backpressure#COALESCE_LATEST}, which is 1)
     * @param backpressure what happens when the mailbox is full at publish time
     */
    public <T> Subscription subscribe(Topic<T> topic, Subscriber<T> subscriber,
                                      int capacity, Backpressure backpressure) {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(subscriber, "subscriber");
        Objects.requireNonNull(backpressure, "backpressure");

        PumpedReg<T> reg = new PumpedReg<>(topic, subscriber, capacity, backpressure);
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
