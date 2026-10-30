package sibarum.atchung;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import sibarum.probe.Cause;
import sibarum.probe.Lane;
import sibarum.probe.Probe;
import sibarum.probe.Zone;

/**
 * A realtime, multithreaded broadcast/subscribe event bus. Publish an event on a {@link Topic} and
 * every subscriber to that topic is notified. <em>Attention! Something happened.</em>
 *
 * <p><b>Publishing never blocks</b> (except a {@link Backpressure#BLOCK} pumped subscriber): a fast
 * producer is never stalled by a slow consumer. Delivery is chosen per subscriber:
 * <ul>
 *   <li>{@link #subscribe(Topic, Subscriber)} — <b>inline</b>: run on the publisher's thread.
 *       Lowest latency; keep the handler cheap and non-reentrant.</li>
 *   <li>{@link #subscribeAsync(Topic, Subscriber, Executor)} — <b>async</b>: run on an executor.</li>
 *   <li>{@link #pump()} then {@link Pump#subscribe} — <b>pumped</b>: queued into a bounded mailbox
 *       and delivered on the thread that calls {@link Pump#drain()} (e.g. a render thread, once per
 *       frame). This is the thread-affine, batched path for UI/engine consumers.</li>
 * </ul>
 *
 * <p>Ordering is per-topic FIFO from a single publisher. The bus itself is pure in-VM and passes
 * event references without copying or serialization; ship events across processes or machines by
 * bridging the bus to a transport (a separate module), never by burdening this core.
 *
 * <p>Instances are independent. {@link #global()} offers one shared bus for the common single-bus case.
 */
public final class Atchung {

    private static final class Holder {
        static final Atchung GLOBAL = new Atchung();
    }

    /**
     * Each topic's registrations, as an array that is never changed: registering or unregistering swaps in a new one. A
     * publish reads it once and walks a snapshot by index, so it neither allocates an iterator nor sees a registration
     * set that changed under it — the snapshot a {@code CopyOnWriteArrayList}'s iterator gave, without the iterator.
     * A topic whose last subscriber has gone keeps an empty array, as the list it replaced stayed empty: a publish to
     * it is a delivery to nobody, not the no-subscriber count, exactly as before.
     */
    private final ConcurrentMap<Topic<?>, Reg<?>[]> registry = new ConcurrentHashMap<>();

    private static final Reg<?>[] NONE = new Reg<?>[0];

    /**
     * What the process does when the bus detects a fault it cannot honestly continue past — see {@link Fatal}.
     * Process-wide rather than per-bus, because "should this process still be running" is not a question two
     * buses can answer differently. Volatile and settable once at startup; a test that needs to observe a
     * fault without dying installs {@link Fatal#THROW} and restores this afterwards.
     */
    private static volatile Fatal fatal = Fatal.HALT;

    private Atchung() {
    }

    /** The process-wide fault policy. {@link Fatal#HALT} unless something has said otherwise. */
    public static Fatal fatal() {
        return fatal;
    }

    /**
     * Install the process-wide fault policy; {@code null} restores {@link Fatal#HALT}. Call it once, at
     * startup, before any subscription exists — installing a policy that lets faults through, after a fault
     * has already been survived, tells you nothing about the state you are now in.
     */
    public static void onFatal(Fatal policy) {
        fatal = policy == null ? Fatal.HALT : policy;
    }

    /** Create an independent bus. */
    public static Atchung create() {
        return new Atchung();
    }

    /** The process-wide shared bus, created on first use. */
    public static Atchung global() {
        return Holder.GLOBAL;
    }

    /**
     * Publish {@code event} to every subscriber of {@code topic}. Non-blocking, save a
     * {@link Backpressure#BLOCK} mailbox that is full — and save a {@link Backpressure#FAIL} one, which
     * throws {@link MailboxOverflow} here after the {@link Fatal} policy has run. Delivery to the remaining
     * subscribers does not happen in that case, which is the correct behaviour for a process that has been
     * told to stop.
     */
    public <T> void publish(Topic<T> topic, T event) {
        Objects.requireNonNull(topic, "topic");
        // A publish nobody gave a cause: counted under its thread, so a boundary nobody wrapped is a named gap in the
        // report rather than rows that seem to have no history. Composed only when profiling.
        if (Probe.ON && Cause.current() == Cause.NONE) {
            Probe.count(Lane.BUS, "uncaused " + Thread.currentThread().getName());
        }
        Reg<?>[] regs = registry.get(topic);
        if (regs == null) {
            // Counted even though nothing happens: a publish with no subscriber is the shape of a whole class
            // of "the event never arrived" bug, and it is invisible from either end without this line. It
            // lands in the probe's counter table under the plain topic name, while a delivered publish lands
            // in the span table under the same name — so the two are told apart by which table they are in,
            // and neither call site has to build a string on the hottest path in the stack to say which.
            Probe.count(Lane.BUS, topic.name());
            return;
        }
        // The span covers the whole fan-out, so inline subscribers — which run right here, on this thread —
        // are inside it. That is deliberate: an inline handler's cost is a publisher's cost, and the report
        // should say so rather than attributing it to whoever happened to call publish.
        try (Zone z = Probe.zone(Lane.BUS, topic.name())) {
            for (Reg<?> reg : regs) {
                @SuppressWarnings("unchecked")
                Reg<T> typed = (Reg<T>) reg;
                typed.deliver(event);
            }
        }
    }

    /** Subscribe with <b>inline</b> delivery on the publisher's thread. */
    public <T> Subscription subscribe(Topic<T> topic, Subscriber<T> subscriber) {
        return register(new InlineReg<>(topic, requireSub(subscriber)));
    }

    /** Subscribe with <b>async</b> delivery on {@code executor}. */
    public <T> Subscription subscribeAsync(Topic<T> topic, Subscriber<T> subscriber, Executor executor) {
        Objects.requireNonNull(executor, "executor");
        return register(new AsyncReg<>(topic, requireSub(subscriber), executor));
    }

    /** Create a pump — a per-thread drain point for pumped subscriptions. */
    public Pump pump() {
        return new Pump(this);
    }

    /**
     * Create a {@link Tick}: components' inboxes, drained together in rounds, in one order. See {@link Tick}.
     *
     * @param arrival called after a message from outside the tick has been staged for the next one, on the
     *                publisher's thread and holding no lock: where a frame loop is woken. May be null.
     */
    public Tick tick(Runnable arrival) {
        return new Tick(this, arrival, Tick.DEFAULT_MAX_HOPS);
    }

    /** As {@link #tick(Runnable)}, with the longest chain of messages a tick allows before calling it a loop. */
    public Tick tick(Runnable arrival, int maxHops) {
        return new Tick(this, arrival, maxHops);
    }

    /** @return the number of active subscribers on {@code topic}. */
    public int subscriberCount(Topic<?> topic) {
        Reg<?>[] regs = registry.get(topic);
        return regs == null ? 0 : regs.length;
    }

    // --- internal registration --------------------------------------------

    <T> Subscription register(Reg<T> reg) {
        registry.compute(reg.topic(), (topic, regs) -> {
            Reg<?>[] now = regs == null ? NONE : regs;
            Reg<?>[] next = Arrays.copyOf(now, now.length + 1);
            next[now.length] = reg;
            return next;
        });
        return new RegSubscription(reg);
    }

    /** The registrations on {@code topic}, or null when there are none: for a {@link Tick} fanning out a round. */
    Reg<?>[] registered(Topic<?> topic) {
        return registry.get(topic);
    }

    void unregister(Reg<?> reg) {
        registry.computeIfPresent(reg.topic(), (topic, regs) -> {
            for (int i = 0; i < regs.length; i++) {
                if (regs[i] == reg) {
                    if (regs.length == 1) {
                        return NONE;   // kept, empty: see the registry's javadoc
                    }
                    Reg<?>[] next = new Reg<?>[regs.length - 1];
                    System.arraycopy(regs, 0, next, 0, i);
                    System.arraycopy(regs, i + 1, next, i, regs.length - i - 1);
                    return next;
                }
            }
            return regs;
        });
    }

    private static <T> Subscriber<T> requireSub(Subscriber<T> subscriber) {
        return Objects.requireNonNull(subscriber, "subscriber");
    }

    /**
     * A routing entry: knows its topic and how to deliver one event. Delivery is gated by a paused
     * flag — pausing is lossy, so a paused entry simply drops the event (it is never buffered here).
     */
    abstract static class Reg<T> {
        private final Topic<T> topic;
        private volatile boolean paused;

        Reg(Topic<T> topic) {
            this.topic = topic;
        }

        final Topic<T> topic() {
            return topic;
        }

        final void deliver(T event) {
            if (!paused) {
                doDeliver(event);
            }
        }

        final void setPaused(boolean paused) {
            this.paused = paused;
        }

        final boolean isPaused() {
            return paused;
        }

        abstract void doDeliver(T event);
    }

    private static final class InlineReg<T> extends Reg<T> {
        private final Subscriber<T> subscriber;

        InlineReg(Topic<T> topic, Subscriber<T> subscriber) {
            super(topic);
            this.subscriber = subscriber;
        }

        @Override
        void doDeliver(T event) {
            subscriber.on(event);
        }
    }

    private static final class AsyncReg<T> extends Reg<T> {
        private final Subscriber<T> subscriber;
        private final Executor executor;

        AsyncReg(Topic<T> topic, Subscriber<T> subscriber, Executor executor) {
            super(topic);
            this.subscriber = subscriber;
            this.executor = executor;
        }

        /** The handler runs on the executor's thread inside the cause the event was published in. */
        @Override
        void doDeliver(T event) {
            long cause = Cause.current();
            executor.execute(() -> {
                long previous = Cause.enter(cause);
                try {
                    subscriber.on(event);
                } finally {
                    Cause.exit(previous);
                }
            });
        }
    }

    private final class RegSubscription implements Subscription {
        private final Reg<?> reg;
        private volatile boolean active = true;

        RegSubscription(Reg<?> reg) {
            this.reg = reg;
        }

        @Override
        public boolean isActive() {
            return active;
        }

        @Override
        public void pause() {
            reg.setPaused(true);
        }

        @Override
        public void resume() {
            reg.setPaused(false);
        }

        @Override
        public boolean isPaused() {
            return reg.isPaused();
        }

        @Override
        public void close() {
            if (!active) {
                return;
            }
            active = false;
            unregister(reg);
        }
    }
}
