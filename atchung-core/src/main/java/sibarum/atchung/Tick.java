package sibarum.atchung;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import sibarum.probe.Cause;
import sibarum.probe.Lane;
import sibarum.probe.Probe;
import sibarum.probe.Zone;

/**
 * Components' inboxes, drained together, in rounds, in one order: the bus's half of a <em>tick</em>.
 *
 * <p>A {@link Pump} is a drain point for one consumer. A tick is a drain point for a set of them that must agree on
 * order: each {@link Inbox} is a component's, holding every message for every topic it subscribes to in one queue,
 * and a call to {@link #run} delivers to all of them until there is nothing left or the caller's deadline has passed.
 * What follows is the mechanism. Why a framework wants it, and the rules it is held to, are in
 * vexelray-framework's {@code docs/threading.md}, §0 and §5.
 *
 * <h2>Rounds</h2>
 *
 * <ol>
 *   <li><b>Arrival.</b> A message published from outside a tick (by {@link Atchung#publish}, or by a {@link Sender}
 *       whose component is not being delivered to) is <em>staged</em>, in arrival order, and enters the inboxes when
 *       the next tick starts: never in the middle of one. That is what makes "between ticks" a moment rather than a
 *       race.</li>
 *   <li><b>A round.</b> Every inbox with anything queued is handed all of it, in its order, one message at a time.
 *       What a component sends through its {@link Sender}s while it is being delivered to goes into its own outbox,
 *       not into anybody's inbox.</li>
 *   <li><b>The end of a round.</b> Outboxes are emptied in the order the inboxes were made, each in the order its
 *       component sent. A message for an inbox of this tick is queued there for the next round; a message for any
 *       other subscriber is held and handed over when the tick ends, in the same order. <b>Another tick receives a
 *       tick's share in one step</b>, so it runs on all of one tick's sends or none of them: that is what keeps a
 *       frame from drawing half of what the logic wrote. A {@link Pump}, an inline or an async subscriber is handed
 *       its messages one at a time, as a publish would, and behaves exactly as it always has.</li>
 *   <li><b>The end of a tick.</b> When a round leaves nothing queued, or the deadline has passed, the tick is over.
 *       A delivery is never interrupted: the deadline is checked between rounds. What is still queued stays queued,
 *       ahead of whatever is staged by the next tick, so carrying over keeps the order.</li>
 * </ol>
 *
 * <p>So the order is fixed by the rounds and by the order of the inboxes, and by nothing that depends on timing. Lanes
 * ({@link Lanes}) may run a round's inboxes on several threads; the result is the same as running them on one, because
 * nothing a round sends is delivered within that round. The same inputs, staged in the same order, produce the same
 * deliveries.
 *
 * <h2>State and edges</h2>
 *
 * <p>Each message is queued according to its topic's {@link Topic.Kind}. An edge takes a place of its own and is
 * delivered, whatever else arrives. A state message names a {@link Topic.Key}, and replaces whatever is queued for
 * that key in the same subscription, moving to the back as {@link Fold} describes. Carry-over does not change this: a
 * state message carried into the next tick is replaced by a newer write to its key like any other.
 *
 * <h2>Loops</h2>
 *
 * <p>Every message carries its <em>hops</em>: zero when it was staged, and one more than the message whose delivery
 * sent it. A message with more hops than the tick allows ({@link #DEFAULT_MAX_HOPS} by default) is a chain that keeps
 * feeding itself, which is a bug, so the {@link Fatal} policy runs and a {@link TickLoop} naming the topic is thrown.
 * Hops are counted per message rather than rounds per tick, because a deadline that cut every tick short would
 * otherwise hide a loop forever.
 *
 * <h2>Threads</h2>
 *
 * <p>{@link #run} is called by one thread at a time: the tick's <em>coordinator</em>. Inboxes may be added only before
 * the first run. Publishing, and therefore staging, is safe from any thread. Within a run, each inbox is touched by its
 * lane's thread during a round and by the coordinator between rounds, never both at once, which {@link Lanes} must
 * guarantee by returning only once every lane has. Nothing on the delivery path allocates in steady state, save a map
 * entry per new key on a state subscription.
 */
public final class Tick {

    /**
     * The longest chain of messages a tick allows, by default. A request, its reply and the view's update are three;
     * nothing an application means to do is sixty-four, and a loop gets there in microseconds.
     */
    public static final int DEFAULT_MAX_HOPS = 64;

    /**
     * How many messages may be staged between two ticks before the bus calls it a fault. Far more than input or replies
     * produce between two frames; what reaches it is a publisher in a loop, or ticks that have stopped running.
     */
    static final int STAGING_LIMIT = 1 << 20;

    /**
     * Runs one round's lanes: calls {@link LaneWork#lane} once for each index below {@code count}, on whatever threads
     * it likes, and returns only once every call has returned. A throw from one lane is not this interface's business:
     * {@link #run} collects what each inbox threw itself.
     *
     * <p>Returning after every lane has returned is also what makes the round's writes visible to the coordinator and
     * the coordinator's to the next round, so an implementation hands work over by something that says so (an
     * executor's submit and a join, a barrier), never by a plain field.
     */
    @FunctionalInterface
    public interface Lanes {
        void run(int count, LaneWork work);
    }

    /** One lane's share of a round. */
    @FunctionalInterface
    public interface LaneWork {
        void lane(int index);
    }

    /** Every lane on the calling thread, in index order. Gives exactly the deliveries any other {@link Lanes} does. */
    public static final Lanes ONE_THREAD = (count, work) -> {
        for (int i = 0; i < count; i++) {
            work.lane(i);
        }
    };

    final Atchung bus;
    private final Runnable arrival;
    final int maxHops;

    /** Every inbox, in the order it was made, which is the order outboxes are emptied in. */
    private final List<Inbox> inboxes = new ArrayList<>();
    /** The inboxes of each lane, built at the first run. */
    private Inbox[][] byLane;
    /** {@link #inboxes} as an array, built at the first run: walked several times a round, and an iterator is an allocation. */
    private Inbox[] all;
    private final LaneWork work = this::deliverLane;

    /** Set by the first run; an inbox made after it would have no place in the order already used. */
    private boolean started;

    // Staged messages: published from outside a tick, waiting for the next one. Guarded by the lock; swapped whole
    // at the start of a run, so taking them is O(1) under the lock and walking them is outside it.
    private final Object lock = new Object();
    private TickReg<?>[] stagedRegs = new TickReg<?>[64];
    private Object[] stagedEvents = new Object[64];
    private long[] stagedCauses = new long[64];
    private long[] stagedRuns = new long[64];
    private int staged;
    private TickReg<?>[] spareRegs = new TickReg<?>[64];
    private Object[] spareEvents = new Object[64];
    private long[] spareCauses = new long[64];
    private long[] spareRuns = new long[64];

    // Sends for subscribers outside this tick, held until it ends. The coordinator's alone.
    private Atchung.Reg<?>[] outRegs = new Atchung.Reg<?>[64];
    private Object[] outEvents = new Object[64];
    private long[] outCauses = new long[64];
    private long[] outRuns = new long[64];
    private int out;

    private int lastRounds;
    private boolean lastCarried;
    /** How many runs have delivered anything: the number of the last, as its {@code tick.ran} row says it. */
    private long number;

    /**
     * How long a {@link Chain}'s wait may last when neither it nor its chain says, by default: long enough that reaching
     * it means something is wrong rather than slow (vexelray-framework's {@code docs/plans/chains.md}, answered
     * 2026-11-02). A wait that is slow by nature says {@code within(...)} or {@code forever()}.
     */
    public static final Duration DEFAULT_BOUND = Duration.ofSeconds(5);

    /** What a default bound running out is said to: a bug, since nobody stated a bound (chains.md, ruling 7). */
    @FunctionalInterface
    public interface Overdue {
        void overdue(String what);

        /** Traced, and nothing else: the run has already gone on with its failure, and the window stays live. */
        Overdue TRACE = what -> {
            if (Probe.ON) {
                Probe.count(Lane.BUS, "overdue");
            }
        };

        /** Thrown, so the tick's run throws it: what a test wants, as a frame that changed in one frame fails one. */
        Overdue FAIL = what -> {
            throw new IllegalStateException(what);
        };
    }

    private volatile Alarm alarm;
    private volatile Duration defaultBound = DEFAULT_BOUND;
    private volatile Overdue overdue = Overdue.TRACE;

    Tick(Atchung bus, Runnable arrival, int maxHops) {
        if (maxHops < 1) {
            throw new IllegalArgumentException("maxHops must be >= 1, was " + maxHops);
        }
        this.bus = bus;
        this.arrival = arrival;
        this.maxHops = maxHops;
    }

    /** A component's inbox, on lane 0. */
    public Inbox inbox(String name) {
        return inbox(name, 0);
    }

    /**
     * A component's inbox. {@code lane} says which share of a round it is delivered in ({@link Lanes}); inboxes on one
     * lane are delivered one after another, in the order they were made.
     *
     * @throws IllegalStateException after the first {@link #run}
     */
    public Inbox inbox(String name, int lane) {
        Objects.requireNonNull(name, "name");
        if (lane < 0) {
            throw new IllegalArgumentException("lane must be >= 0, was " + lane);
        }
        if (started) {
            throw new IllegalStateException("inbox '" + name + "' made after the tick first ran: its place in the "
                    + "order would differ from the order already used. Make every inbox before the first run.");
        }
        Inbox in = new Inbox(this, name, lane, inboxes.size());
        inboxes.add(in);
        return in;
    }

    /**
     * The time this tick's chains keep their bounds with: the framework's timer, or a {@link HandClock} in a test.
     * Without one, a chain that has to keep a bound refuses, by name, the first time it would.
     */
    public Tick alarm(Alarm alarm) {
        this.alarm = Objects.requireNonNull(alarm, "alarm");
        return this;
    }

    /** The alarm this tick's chains keep their bounds with, or null. */
    public Alarm alarm() {
        return alarm;
    }

    /** Bound every wait that states nothing, and whose chain states nothing, by {@code bound} ({@link #DEFAULT_BOUND}). */
    public Tick defaultBound(Duration bound) {
        Objects.requireNonNull(bound, "bound");
        if (bound.isZero() || bound.isNegative()) {
            throw new IllegalArgumentException("a bound is some time: " + bound);
        }
        this.defaultBound = bound;
        return this;
    }

    /** How long a wait lasts that states nothing, and whose chain states nothing. */
    public Duration defaultBound() {
        return defaultBound;
    }

    /** Who hears that a default bound ran out ({@link Overdue#TRACE} until told). */
    public Tick whenOverdue(Overdue overdue) {
        this.overdue = Objects.requireNonNull(overdue, "overdue");
        return this;
    }

    Overdue overdue() {
        return overdue;
    }

    /** A tick on one thread: {@link #run(long, Lanes)} with {@link #ONE_THREAD}. */
    public int run(long deadlineNanos) {
        return run(deadlineNanos, ONE_THREAD);
    }

    /**
     * Run one tick: take what was staged, then deliver in rounds until nothing is queued or {@code deadlineNanos}
     * (a {@link System#nanoTime()} reading) has passed. The first round always runs, so a deadline already past still
     * makes progress. Called by the coordinator only.
     *
     * @return how many messages were delivered
     * @throws RuntimeException whatever a subscriber threw, rethrown once the round it was in has ended and the
     *                          tick's sends have been handed over; the subscriber's remaining messages stay queued
     */
    public int run(long deadlineNanos, Lanes lanes) {
        Objects.requireNonNull(lanes, "lanes");
        if (!started) {
            start();
        }
        takeStaged();
        lastRounds = 0;
        lastCarried = false;
        if (!anyQueued()) {
            return 0;
        }
        int delivered = 0;
        Throwable failure = null; // only ever a RuntimeException or an Error: Inbox.deliver catches nothing else
        try (Zone z = Probe.zone(Lane.BUS, "tick")) {
            while (true) {
                lastRounds++;
                lanes.run(byLane.length, work);
                for (Inbox in : all) {
                    delivered += in.deliveredThisRound;
                    in.deliveredThisRound = 0;
                    if (in.failure != null && failure == null) {
                        failure = in.failure;
                    }
                    in.failure = null;
                }
                for (Inbox in : all) {
                    if (in.betweenSize > 0) {
                        Throwable t = in.runBetween();
                        if (t != null && failure == null) {
                            failure = t;
                        }
                    }
                }
                flush();
                if (failure != null || !anyQueued()) {
                    break;
                }
                if (System.nanoTime() - deadlineNanos >= 0) {
                    lastCarried = true;
                    break;
                }
            }
        } finally {
            handOver();
        }
        Probe.count(Lane.BUS, "tick rounds", lastRounds);
        Probe.count(Lane.BUS, "tick delivered", delivered);
        // The tick's own row, beside the zone that timed it: its number, so a tool can name a tick (the zone is
        // written as it closes, so this row follows it), and what it did. A run with nothing queued is no tick.
        number++;
        if (Probe.ON) {
            Probe.mark(Lane.BUS, "tick.ran", "#" + number + " " + lastRounds + " rounds " + delivered + " delivered"
                    + (lastCarried ? " carried" : ""));
        }
        if (failure instanceof RuntimeException r) {
            throw r;
        }
        if (failure instanceof Error e) {
            throw e;
        }
        return delivered;
    }

    /**
     * The number of the last tick that delivered anything, counting from one; zero before the first. A run with nothing
     * queued is not counted. Each counted tick writes a {@code tick.ran} row on the bus lane, after its {@code tick}
     * zone: {@code #<number> <rounds> rounds <delivered> delivered}, and {@code carried} when it stopped at its
     * deadline with messages still queued. Coordinator only.
     */
    public long number() {
        return number;
    }

    /** How many rounds the last {@link #run} took; zero when it had nothing to deliver. Coordinator only. */
    public int lastRounds() {
        return lastRounds;
    }

    /** Whether the last {@link #run} stopped at its deadline with messages still queued. Coordinator only. */
    public boolean carried() {
        return lastCarried;
    }

    /** Whether anything is queued or staged: whether a {@link #run} now would deliver anything. Coordinator only. */
    public boolean hasPending() {
        if (anyQueued()) {
            return true;
        }
        synchronized (lock) {
            return staged > 0;
        }
    }

    // ------------------------------------------------------------------------------------------- the coordinator

    private void start() {
        started = true;
        all = inboxes.toArray(new Inbox[0]);
        int lanes = 0;
        for (Inbox in : inboxes) {
            lanes = Math.max(lanes, in.lane + 1);
        }
        int[] counts = new int[lanes];
        for (Inbox in : inboxes) {
            counts[in.lane]++;
        }
        byLane = new Inbox[lanes][];
        for (int i = 0; i < lanes; i++) {
            byLane[i] = new Inbox[counts[i]];
            counts[i] = 0;
        }
        for (Inbox in : inboxes) {
            byLane[in.lane][counts[in.lane]++] = in;
        }
    }

    private void deliverLane(int lane) {
        for (Inbox in : byLane[lane]) {
            in.deliver();
        }
    }

    private boolean anyQueued() {
        if (all == null) {
            return false;   // before the first run nothing has left staging, so no inbox holds anything
        }
        for (Inbox in : all) {
            if (in.head != null) {
                return true;
            }
        }
        return false;
    }

    /** Move what was staged into the inboxes, behind what was carried over, in arrival order. */
    private void takeStaged() {
        TickReg<?>[] regs;
        Object[] events;
        long[] causes;
        long[] runs;
        int n;
        synchronized (lock) {
            n = staged;
            if (n == 0) {
                return;
            }
            regs = stagedRegs;
            events = stagedEvents;
            causes = stagedCauses;
            runs = stagedRuns;
            stagedRegs = spareRegs;
            stagedEvents = spareEvents;
            stagedCauses = spareCauses;
            stagedRuns = spareRuns;
            spareRegs = null;
            spareEvents = null;
            spareCauses = null;
            spareRuns = null;
            staged = 0;
        }
        try {
            for (int i = 0; i < n; i++) {
                enqueue(regs[i], events[i], causes[i], runs[i], 0);
            }
        } finally {
            java.util.Arrays.fill(regs, 0, n, null);
            java.util.Arrays.fill(events, 0, n, null);
            synchronized (lock) {
                spareRegs = regs;
                spareEvents = events;
                spareCauses = causes;
                spareRuns = runs;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> void enqueue(TickReg<T> reg, Object event, long cause, long run, int hops) {
        reg.enqueue((T) event, cause, run, hops);
    }

    /** Empty every outbox, in inbox order: into the inboxes for the next round, or out, for when the tick ends. */
    private void flush() {
        for (Inbox in : all) {
            int n = in.outSize;
            for (int i = 0; i < n; i++) {
                fanOut(in.outTopics[i], in.outEvents[i], in.outCauses[i], in.outRuns[i], in.outHops[i]);
                in.outTopics[i] = null;
                in.outEvents[i] = null;
            }
            in.outSize = 0;
        }
    }

    private void fanOut(Topic<?> topic, Object event, long cause, long run, int hops) {
        Atchung.Reg<?>[] regs = bus.registered(topic);
        if (regs == null) {
            // As Atchung.publish counts it: a send nobody hears is the shape of "the message never arrived".
            Probe.count(Lane.BUS, topic.name());
            return;
        }
        // The bus's snapshot, as a publish walks it: one registration set for the whole fan-out, and no iterator.
        for (Atchung.Reg<?> reg : regs) {
            if (reg instanceof TickReg<?> t && t.inbox.tick == this) {
                enqueue(t, event, cause, run, hops);
            } else {
                holdOut(reg, event, cause, run);
            }
        }
    }

    private void holdOut(Atchung.Reg<?> reg, Object event, long cause, long run) {
        if (out == outRegs.length) {
            outRegs = java.util.Arrays.copyOf(outRegs, out * 2);
            outEvents = java.util.Arrays.copyOf(outEvents, out * 2);
            outCauses = java.util.Arrays.copyOf(outCauses, out * 2);
            outRuns = java.util.Arrays.copyOf(outRuns, out * 2);
        }
        outRegs[out] = reg;
        outEvents[out] = event;
        outCauses[out] = cause;
        outRuns[out] = run;
        out++;
    }

    /**
     * Hand this tick's sends to the subscribers outside it, in the order they were sent, each in its own cause.
     *
     * <p><b>Another tick gets all of this tick's sends at once, or none of them.</b> Its share is staged in one step,
     * under its lock, so its next run takes the whole of this tick or waits for the next: a frame drained by a tick
     * can never show half of what one logic tick wrote. A {@link Pump}, an inline or an async subscriber is handed
     * its messages one at a time, exactly as a publish would.
     */
    private void handOver() {
        int n = out;
        out = 0;
        long coordinator = Cause.current();
        long coordinatorRun = Runs.current();
        try {
            for (int i = 0; i < n; i++) {
                if (outRegs[i] instanceof TickReg<?> t) {
                    t.inbox.tick.stageShare(outRegs, outEvents, outCauses, outRuns, n);
                }
            }
            for (int i = 0; i < n; i++) {
                if (outRegs[i] == null) {
                    continue;   // staged into another tick, with the rest of that tick's share
                }
                Cause.enter(outCauses[i]);
                Runs.enter(outRuns[i]);
                deliverOut(outRegs[i], outEvents[i]);
            }
        } finally {
            Cause.exit(coordinator);
            Runs.exit(coordinatorRun);
            java.util.Arrays.fill(outRegs, 0, n, null);
            java.util.Arrays.fill(outEvents, 0, n, null);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> void deliverOut(Atchung.Reg<T> reg, Object event) {
        reg.deliver((T) event);
    }

    // ------------------------------------------------------------------------------------------- any thread

    /** A message from outside a tick, for {@code reg}: staged for the next one. On the publisher's thread. */
    <T> void stage(TickReg<T> reg, T event, long cause, long run) {
        synchronized (lock) {
            stageLocked(reg, event, cause, run);
        }
        // Outside the lock, as Pump's arrival is: what it wakes may take the lock to ask what is staged.
        if (arrival != null) {
            arrival.run();
        }
    }

    /**
     * Stage every message among the first {@code n} of {@code regs} that is for an inbox of this tick, in order and in
     * one step, and clear each one staged out of {@code regs}. Called by another tick handing over; see
     * {@link #handOver}. A paused subscription is skipped, as {@code deliver} skips it: pausing is lossy.
     */
    void stageShare(Atchung.Reg<?>[] regs, Object[] events, long[] causes, long[] runs, int n) {
        boolean any = false;
        synchronized (lock) {
            for (int i = 0; i < n; i++) {
                if (regs[i] instanceof TickReg<?> t && t.inbox.tick == this) {
                    if (!t.isPaused() && t.acceptsAny(events[i])) {
                        stageLocked(t, events[i], causes[i], runs[i]);
                        any = true;
                    }
                    regs[i] = null;
                }
            }
        }
        if (any && arrival != null) {
            arrival.run();
        }
    }

    /** Stage one message. Caller holds the lock. */
    private void stageLocked(TickReg<?> reg, Object event, long cause, long run) {
        if (staged == STAGING_LIMIT) {
            MailboxOverflow overflow = new MailboxOverflow(reg.topic(), STAGING_LIMIT, reg.delivered,
                    reg.subscribedMillis, reg.lastDeliveredMillis);
            Atchung.fatal().fault(overflow);
            throw overflow;
        }
        if (staged == stagedRegs.length) {
            stagedRegs = java.util.Arrays.copyOf(stagedRegs, staged * 2);
            stagedEvents = java.util.Arrays.copyOf(stagedEvents, staged * 2);
            stagedCauses = java.util.Arrays.copyOf(stagedCauses, staged * 2);
            stagedRuns = java.util.Arrays.copyOf(stagedRuns, staged * 2);
        }
        stagedRegs[staged] = reg;
        stagedEvents[staged] = event;
        stagedCauses[staged] = cause;
        stagedRuns[staged] = run;
        staged++;
    }
}
