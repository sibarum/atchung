package sibarum.atchung;

import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

import sibarum.probe.Lane;
import sibarum.probe.Probe;
import sibarum.probe.Zone;

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

    /**
     * The monitor a thread parks on in {@link #drain(long)}.
     *
     * <p>Pump-level rather than per mailbox, because the question being waited on is <i>has anything at all
     * arrived</i> and a pump holds many mailboxes. One condition answers it; waiting on each mailbox's own
     * lock could not.
     */
    private final Object idle = new Object();

    /**
     * Whether a thread is parked right now. Read on every publish, so it is a plain field rather than
     * anything that has to be taken: a bus whose publish path pays for a feature nobody is using is a bus
     * that has made everyone pay for one caller.
     */
    private volatile boolean parked;

    /** Set by {@link #wake()} to end a park that nothing arrived for. Guarded by {@link #idle}. */
    private boolean woken;

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

        PumpedReg<T> reg = new PumpedReg<>(topic, subscriber, capacity, backpressure, fold, this::arrived);
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
        // One span for the whole drain, on the thread that owns it — which for a GUI is the render thread,
        // once per frame. A frame that ran long and a drain that ran long are then two rows in the same
        // table, and which contains which is answered by the self-time column rather than by guesswork.
        try (Zone z = Probe.zone(Lane.BUS, "pump drain")) {
            int delivered = 0;
            for (PumpedReg<?> reg : regs) {
                delivered += reg.drain();
            }
            Probe.count(Lane.BUS, "pump delivered", delivered);
            return delivered;
        }
    }

    /**
     * Deliver queued events, or <b>wait up to {@code timeoutNanos} for some to arrive</b> and then deliver
     * them, on the calling thread.
     *
     * <p>This is what a consumer that has no loop of its own calls: a component whose only reason to wake is
     * its own mailbox. {@link #drain()} is for a consumer that already has a wake — a frame loop, which has
     * somewhere else to be and must never park here.
     *
     * <p><b>Waiting here does not block publishers.</b> The park releases the monitor it waits on and holds
     * no mailbox lock at all, so a publisher never queues behind a parked consumer. What a publisher can
     * briefly contend with is the drain's own copy of the queue, which is the same short critical section it
     * contends with today and which ends before any handler runs.
     *
     * <p><b>There is no untimed form, and that is deliberate.</b> A drain that waits forever on the wrong
     * thread — one that also publishes to this pump, or a frame loop that took the wrong overload — is a hung
     * application rather than a slow one. A timeout turns that mistake into a stall a profile can see. For a
     * shutdown that must not wait out the timeout, {@link #wake()}.
     *
     * <p><b>Do not park a virtual thread here.</b> A desktop application following Kronometer's advice runs
     * its timeline kernel on a single carrier, and blocking work placed on that carrier deadlocks against the
     * serialisation that makes the baton fast. A component belongs on a platform thread.
     *
     * @param timeoutNanos how long to wait when nothing is queued; zero or less waits not at all, which makes
     *                     this exactly {@link #drain()}
     * @return the number of events delivered
     */
    public int drain(long timeoutNanos) {
        int delivered = drain();
        if (delivered > 0 || timeoutNanos <= 0) {
            return delivered;
        }
        // Parked outside drain()'s probe zone, on purpose. Inside it, an idle component would be recorded as
        // a drain that took a second, and the BUS lane is the instrument that answers "why did we miss a
        // frame" — an instrument that reports waiting as work is worse than none.
        park(timeoutNanos);
        return drain();
    }

    /**
     * End a park now, whether or not anything arrived.
     *
     * <p>The shutdown half of {@link #drain(long)}: a component is stopped by saying so and waking it, and
     * without this a {@code Disposer} would have to wait out whatever timeout the component happened to pass.
     */
    public void wake() {
        synchronized (idle) {
            woken = true;
            idle.notifyAll();
        }
    }

    /** Told by a mailbox that something was queued — from the publisher's thread, holding no mailbox lock. */
    private void arrived() {
        if (!parked) {
            return;                 // nobody to wake, and nothing taken to find that out
        }
        synchronized (idle) {
            idle.notifyAll();
        }
    }

    private void park(long timeoutNanos) {
        long deadline = System.nanoTime() + timeoutNanos;
        synchronized (idle) {
            parked = true;
            try {
                // Re-checked rather than trusted: a publisher that read `parked` as false a moment before it
                // was set never signalled, and the event it queued is visible here through the mailbox lock
                // that hasPending takes. That is the case a bare wait would sleep through.
                while (!woken && !hasPending()) {
                    long left = deadline - System.nanoTime();
                    if (left <= 0) {
                        break;
                    }
                    idle.wait(Math.max(1, left / 1_000_000L));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                woken = false;
                parked = false;
            }
        }
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
