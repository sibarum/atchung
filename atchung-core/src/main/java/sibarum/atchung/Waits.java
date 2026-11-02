package sibarum.atchung;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import sibarum.probe.Lane;
import sibarum.probe.Probe;

/**
 * One inbox's waiting runs: the runs its steps hold, the waits open on it, and the timers that bound them. Everything
 * here is touched only inside a delivery to its inbox, on that inbox's lane, so none of it is shared and none of it is
 * locked: a timer that comes due, a run entering a wait and a child run arriving each come in as a {@link Signal} on
 * the inbox's own topic, in order with its other messages.
 *
 * <p>Made for an inbox by the first chain built over it, before the tick runs, because that is when an inbox may
 * subscribe without racing the lane that drains it.
 */
final class Waits {

    /** How many of each message an inbox's waits may have queued: generous, since a wait hears every message on its topic. */
    private static final int CAPACITY = 4096;

    final Inbox inbox;
    /** Timers coming due, runs entering waits, child runs arriving: this inbox's own channel for them. */
    final Topic<Signal> signals;

    private long tokens;
    /** What waits here and may hear a signal for itself, by token. */
    private final HashMap<Long, Pending> pending = new HashMap<>();
    /** The open waits listening on each topic, with which branch each hears it as. */
    private final HashMap<Topic<?>, List<Ear>> listening = new HashMap<>();

    private Waits(Inbox inbox) {
        this.inbox = inbox;
        this.signals = Topic.of("atchung.waits " + inbox.name, Signal.class);
        inbox.subscribe(signals, s -> s.apply(this), CAPACITY);
    }

    /** This inbox's waits, made the first time they are asked for. */
    static Waits of(Inbox inbox) {
        Waits w = inbox.waits;
        if (w == null) {
            w = new Waits(inbox);
            inbox.waits = w;
        }
        return w;
    }

    /** Hear {@code topic} here, once whatever number of waits listen for it. */
    <E> void listen(Topic<E> topic) {
        if (!listening.containsKey(topic)) {
            listening.put(topic, new ArrayList<>());
            inbox.subscribe(topic, event -> heard(topic, event), CAPACITY);
        }
    }

    // ------------------------------------------------------------------------------------------- signals

    /** What reaches an inbox's waits on its own topic. */
    interface Signal {
        void apply(Waits w);
    }

    /** A timer came due. */
    record Due(long token) implements Signal {
        @Override
        public void apply(Waits w) {
            Pending p = w.pending.remove(token);
            if (p != null) {
                p.cancel = null;
                p.expire(w);
            }
        }
    }

    /** A run reached a wait: open it, in this round, after what was queued before it. */
    record Enter(Chain.Wait link, Chain.Run run, Object outcome) implements Signal {
        @Override
        public void apply(Waits w) {
            if (run.life().isCut()) {
                return;
            }
            link.open(run).begin(w, outcome);
        }
    }

    /** A child run ended, with {@code value}: it arrives at the wait that started it, if that still waits. */
    record Arrived(long token, int branch, Object value) implements Signal {
        @Override
        public void apply(Waits w) {
            if (w.pending.get(token) instanceof Open open && !open.closed) {
                open.arrive(w, branch, value);
            } else if (Probe.ON) {
                Probe.mark(Lane.BUS, "chain arrived late", String.valueOf(value));
            }
        }
    }

    /** A message on a topic some open wait listens for: each that it belongs to hears it (ruling 9). */
    private void heard(Topic<?> topic, Object event) {
        List<Ear> ears = listening.get(topic);
        if (ears.isEmpty()) {
            return;
        }
        long run = Runs.current();
        for (Ear ear : ears.toArray(new Ear[0])) {
            if (!ear.open.closed && (run == Runs.NONE || run == ear.open.run.id())) {
                ear.open.arrive(this, ear.branch, event);
            }
        }
    }

    private record Ear(Open open, int branch) {
    }

    // ------------------------------------------------------------------------------------------- timers

    /** Give {@code p} a token, so a signal can find it, and a timer for {@code bound} when it is a time. */
    private void arm(Pending p, Bound bound) {
        p.token = ++tokens;
        pending.put(p.token, p);
        if (bound.timed()) {
            Alarm alarm = inbox.tick.alarm();
            if (alarm == null) {
                throw new IllegalStateException("inbox '" + inbox.name + "' keeps a bound of " + bound + ", and its "
                        + "tick has no alarm to keep it with: give the tick one (Tick.alarm), as the framework does");
            }
            p.cancel = alarm.after(bound.duration(), signals, new Due(p.token));
        }
    }

    private void disarm(Pending p) {
        pending.remove(p.token);
        if (p.cancel != null) {
            p.cancel.cancel();
            p.cancel = null;
        }
    }

    /**
     * The bound a link keeps: what it states, else its chain's default, else the tick's when {@code defaultable}, else
     * none. Only the tick's default counts as {@code byDefault}: a chain's own default is one its author wrote.
     */
    private Kept kept(Bound stated, boolean defaultable) {
        if (stated != null) {
            return new Kept(stated, false);
        }
        if (defaultable) {
            return new Kept(Bound.of(inbox.tick.defaultBound()), true);
        }
        return new Kept(Bound.FOREVER, false);
    }

    private record Kept(Bound bound, boolean byDefault) {
    }

    /** A default bound ran out: a bug, said to the tick's policy (ruling 7). */
    private void overdue(Chain.Run run, String at, Bound bound) {
        if (Probe.ON) {
            Probe.mark(Lane.BUS, run.chain().stepName(), "overdue", run.id());
        }
        inbox.tick.overdue().overdue("chain '" + run.chain().name() + "' waited " + bound + " at " + at + ", the "
                + "tick's default bound, and it ran out: say how long it may take with within(...), or forever()");
    }

    /** Something waiting here, which a signal may find by its token. */
    abstract static class Pending {
        long token;
        Alarm.Cancel cancel;

        abstract void expire(Waits w);
    }

    // ------------------------------------------------------------------------------------------- holds

    /** A run a step of this inbox holds by hand ({@link Out#hold}), until a later step resumes it or its bound runs out. */
    static final class Hold extends Pending {
        final Chain.Run run;
        final Inbox inbox;
        final boolean person;
        Kept kept;
        boolean done;

        Hold(Chain.Run run, Inbox inbox, boolean person) {
            this.run = run;
            this.inbox = inbox;
            this.person = person;
        }

        @Override
        void expire(Waits w) {
            if (done) {
                return;
            }
            done = true;
            Chain<?, ?, ?> chain = run.chain();
            String at = chain.labelAt(run.position());
            chain.advance(run, new TimedOut(chain.name(), at, kept.bound.duration(), kept.byDefault), w.inbox);
            if (kept.byDefault) {
                w.overdue(run, at, kept.bound);
            }
        }
    }

    /** The step that held {@code h} returned: keep its bound. */
    void held(Hold h) {
        h.kept = h.person ? new Kept(Bound.PERSON, false) : kept(h.run.chain().stated(h.run.position()), true);
        arm(h, h.kept.bound);
    }

    /** The step that resumed {@code h} returned: it waits no more. */
    void resumed(Hold h) {
        h.done = true;
        disarm(h);
    }

    // ------------------------------------------------------------------------------------------- open waits

    /** One run in one wait: what it has heard, its timer, and the child runs it started. */
    abstract static class Open extends Pending {
        final Chain.Wait link;
        final Chain.Run run;
        Kept kept;
        boolean closed;
        private Chain.Life[] children;
        private Pending[] deadlines;

        Open(Chain.Wait link, Chain.Run run) {
            this.link = link;
            this.run = run;
        }

        /** The bound this kind keeps when nothing else is stated. */
        Kept bound(Waits w) {
            return w.kept(run.chain().stated(run.position()), link.defaultable());
        }

        /** Open: listen for the message branches, start the chain branches, and start the timer. */
        void begin(Waits w, Object outcome) {
            kept = bound(w);
            w.arm(this, kept.bound);
            Branch<?>[] branches = link.branches;
            for (int i = 0; i < branches.length; i++) {
                if (branches[i] instanceof Branch.Message<?> m) {
                    w.listening.get(m.topic).add(new Ear(this, i));
                } else {
                    start(w, i, (Branch.OfChain<?>) branches[i], outcome);
                }
            }
            listenMore(w);
        }

        /** Anything else this kind listens for. */
        void listenMore(Waits w) {
        }

        private void start(Waits w, int i, Branch.OfChain<?> branch, Object outcome) {
            if (children == null) {
                children = new Chain.Life[link.branches.length];
                deadlines = new Pending[link.branches.length];
            }
            Chain.Life life = Chain.Life.child(new Chain.Life.Parent(w, token, i, run.life()));
            children[i] = life;
            w.inbox.post(branch.chain.topic(), branch.chain.child(outcome, life));
            if (branch.bound != null) {
                Deadline d = new Deadline(this, i, branch);
                deadlines[i] = d;
                w.arm(d, branch.bound);
            }
        }

        /** Branch {@code branch} arrived with {@code value}: a message, or what a child run ended with. */
        abstract void arrive(Waits w, int branch, Object value);

        /** Stop: no more listening, no timer, and every child still running cut off. */
        void close(Waits w) {
            closed = true;
            w.disarm(this);
            Branch<?>[] branches = link.branches;
            for (int i = 0; i < branches.length; i++) {
                if (branches[i] instanceof Branch.Message<?> m) {
                    w.listening.get(m.topic).removeIf(ear -> ear.open == this);
                }
            }
            unlistenMore(w);
            if (children != null) {
                for (int i = 0; i < children.length; i++) {
                    if (children[i] != null) {
                        children[i].cutOff();
                    }
                    if (deadlines[i] != null) {
                        w.disarm(deadlines[i]);
                    }
                }
            }
        }

        void unlistenMore(Waits w) {
        }

        /** Close, and go on with {@code outcome}. */
        void finish(Waits w, Object outcome) {
            close(w);
            run.chain().advance(run, outcome, w.inbox);
        }

        /** Close, go on with a failure, and tell the tick if a default bound is what ran out. */
        void fail(Waits w, Object failure, boolean byDefault) {
            finish(w, failure);
            if (byDefault) {
                w.overdue(run, link.label(), kept.bound);
            }
        }

        String chainName() {
            return run.chain().name();
        }
    }

    /** A chain used as a branch ran out of its own deadline: cut it, and it arrives with a {@link TimedOut}. */
    private static final class Deadline extends Pending {
        final Open open;
        final int branch;
        final Branch.OfChain<?> of;

        Deadline(Open open, int branch, Branch.OfChain<?> of) {
            this.open = open;
            this.branch = branch;
            this.of = of;
        }

        @Override
        void expire(Waits w) {
            if (open.closed) {
                return;
            }
            open.children[branch].cutOff();
            open.deadlines[branch] = null;
            open.arrive(w, branch, new TimedOut(of.chain.name(), "the whole of it", of.bound.duration(), false));
        }
    }

    /** ANY: the first arrival wins. */
    static final class AnyOpen extends Open {
        AnyOpen(Chain.Wait link, Chain.Run run) {
            super(link, run);
        }

        @Override
        void arrive(Waits w, int branch, Object value) {
            finish(w, value);
        }

        @Override
        void expire(Waits w) {
            fail(w, new TimedOut(chainName(), link.label(), kept.bound.duration(), kept.byDefault), kept.byDefault);
        }
    }

    /** ALL: every branch, as what it waited for, then the record; anything else, or the bound, is {@link Missing}. */
    static final class AllOpen extends Open {
        private final Object[] values;
        private final boolean[] got;
        private int count;

        AllOpen(Chain.Wait link, Chain.Run run) {
            super(link, run);
            this.values = new Object[link.branches.length];
            this.got = new boolean[link.branches.length];
        }

        @Override
        void arrive(Waits w, int branch, Object value) {
            if (got[branch]) {
                return;   // a branch arrives once; a second message on its topic is not this wait's
            }
            if (!link.branches[branch].takes(value)) {
                finish(w, new Missing(chainName(), link.label(), missing(branch), false));
                return;
            }
            got[branch] = true;
            values[branch] = value;
            if (++count == values.length) {
                finish(w, ((Chain.AllWait) link).join.of(values.clone()));
            }
        }

        @Override
        void expire(Waits w) {
            fail(w, new Missing(chainName(), link.label(), missing(-1), kept.byDefault), kept.byDefault);
        }

        private List<String> missing(int also) {
            List<String> out = new ArrayList<>();
            for (int i = 0; i < got.length; i++) {
                if (!got[i] || i == also) {
                    out.add(link.branches[i].label());
                }
            }
            return out;
        }
    }

    /** NONE: going on is the bound closing; anything it waits not to see breaks it. */
    static final class NoneOpen extends Open {
        /** The branch number a NONE's closing message is heard as. */
        private static final int BEFORE = -1;

        NoneOpen(Chain.Wait link, Chain.Run run) {
            super(link, run);
        }

        private Chain.NoneWait none() {
            return (Chain.NoneWait) link;
        }

        /** A window of time states its own bound; a NONE before a message is capped as any wait is. */
        @Override
        Kept bound(Waits w) {
            if (none().before == null) {
                return new Kept(link.bound, false);
            }
            Bound stated = link.bound != null ? link.bound : run.chain().stated(run.position());
            return w.kept(stated, true);
        }

        @Override
        void listenMore(Waits w) {
            if (none().before != null) {
                w.listening.get(none().before).add(new Ear(this, BEFORE));
            }
        }

        @Override
        void unlistenMore(Waits w) {
            if (none().before != null) {
                w.listening.get(none().before).removeIf(ear -> ear.open == this);
            }
        }

        @Override
        void arrive(Waits w, int branch, Object value) {
            finish(w, branch == BEFORE ? value : new Broken(chainName(), link.label(), value));
        }

        @Override
        void expire(Waits w) {
            if (none().before == null) {
                finish(w, new Quiet(chainName(), link.label(), kept.bound.duration()));
            } else {
                fail(w, new TimedOut(chainName(), link.label(), kept.bound.duration(), kept.byDefault),
                        kept.byDefault);
            }
        }
    }
}
