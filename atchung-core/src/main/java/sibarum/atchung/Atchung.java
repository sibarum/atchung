package sibarum.atchung;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;

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

    private final ConcurrentMap<Topic<?>, CopyOnWriteArrayList<Reg<?>>> registry = new ConcurrentHashMap<>();

    private Atchung() {
    }

    /** Create an independent bus. */
    public static Atchung create() {
        return new Atchung();
    }

    /** The process-wide shared bus, created on first use. */
    public static Atchung global() {
        return Holder.GLOBAL;
    }

    /** Publish {@code event} to every subscriber of {@code topic}. Non-blocking (save a BLOCK mailbox). */
    public <T> void publish(Topic<T> topic, T event) {
        Objects.requireNonNull(topic, "topic");
        CopyOnWriteArrayList<Reg<?>> regs = registry.get(topic);
        if (regs == null) {
            return;
        }
        for (Reg<?> reg : regs) {
            @SuppressWarnings("unchecked")
            Reg<T> typed = (Reg<T>) reg;
            typed.deliver(event);
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

    /** @return the number of active subscribers on {@code topic}. */
    public int subscriberCount(Topic<?> topic) {
        CopyOnWriteArrayList<Reg<?>> regs = registry.get(topic);
        return regs == null ? 0 : regs.size();
    }

    // --- internal registration --------------------------------------------

    <T> Subscription register(Reg<T> reg) {
        registry.computeIfAbsent(reg.topic(), k -> new CopyOnWriteArrayList<>()).add(reg);
        return new RegSubscription(reg);
    }

    void unregister(Reg<?> reg) {
        CopyOnWriteArrayList<Reg<?>> regs = registry.get(reg.topic());
        if (regs != null) {
            regs.remove(reg);
        }
    }

    private static <T> Subscriber<T> requireSub(Subscriber<T> subscriber) {
        return Objects.requireNonNull(subscriber, "subscriber");
    }

    /** A routing entry: knows its topic and how to deliver one event. */
    interface Reg<T> {
        Topic<T> topic();

        void deliver(T event);
    }

    private record InlineReg<T>(Topic<T> topic, Subscriber<T> subscriber) implements Reg<T> {
        @Override
        public void deliver(T event) {
            subscriber.on(event);
        }
    }

    private record AsyncReg<T>(Topic<T> topic, Subscriber<T> subscriber, Executor executor) implements Reg<T> {
        @Override
        public void deliver(T event) {
            executor.execute(() -> subscriber.on(event));
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
        public void close() {
            if (!active) {
                return;
            }
            active = false;
            unregister(reg);
        }
    }
}
