package sibarum.atchung;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import sibarum.probe.Lane;
import sibarum.probe.Probe;

/**
 * The path from a cause to its outcome, written once: a line of links, each a {@link Step} on its own {@link Ledger}
 * or a wait, where what one link gives as its outcome is what the next one takes.
 *
 * <pre>{@code
 * Chain<ViewState, Click, Object> close = Chain.start("close", view, closeClicked)   // → CloseAsked
 *         .then(CloseAsked.class,   editor,  closeRequested)                         // → PromptToSave, or Closed
 *         .then(PromptToSave.class, dialogs, askSave)                                // holds until the person answers
 *         .then(Save.class,         editor,  saveAndClose)                           // → Closed
 *         .or(DontSave.class,       editor,  discardAndClose)                        // → Closed; Cancel ends the run
 *         .then(Closed.class,       view,    closed)
 *         .build();
 * bus.publish(close.topic(), close.with(click));
 * }</pre>
 *
 * <h2>Steps</h2>
 *
 * <ul>
 *   <li><b>A step gives its outcome</b> through its {@link Out} ({@link Out#result}), since what it returns is its
 *       ledger's next state. A step that gives none ends the run.</li>
 *   <li><b>The outcome's type picks the next step.</b> Each step link after the first names the type it takes, and may
 *       have alternatives ({@link Builder#or}), matched in the order declared, as a {@code catch} is. An outcome none of
 *       them takes ends the run there.</li>
 *   <li><b>Branches join again.</b> Whichever alternative ran, its outcome goes on to the next link.</li>
 *   <li><b>Going on is a send</b>, held in the owner's outbox with the step's other sends and taken back if the step
 *       throws. Each step is atomic on its own ledger; the run as a whole is a saga.</li>
 *   <li><b>Every run has an id</b> ({@link Out#run()}), the same in all of its steps, and every message a step sends
 *       carries it, so a reply finds its way back to the run that asked.</li>
 * </ul>
 *
 * <h2>Waits</h2>
 *
 * <p>A wait is a link where the run stops for things to arrive, instead of a step holding it by hand. Its branches are
 * what it waits for ({@link Branch}): a message on a topic, or a whole chain, which runs as a child of this run. Three
 * kinds:
 *
 * <ul>
 *   <li><b>{@link Builder#all ALL}</b> goes on when every branch has arrived, with all of them as one record; it fails
 *       with {@link Missing}.</li>
 *   <li><b>{@link Builder#any ANY}</b> goes on with the first branch to arrive, and cuts off the rest; it fails with
 *       {@link TimedOut}.</li>
 *   <li><b>{@link Builder#none NONE}</b> goes on when nothing in its branches arrives before its bound, a time
 *       ({@link Quiet}) or another message; what does arrive breaks it ({@link Broken}).</li>
 * </ul>
 *
 * <p>A wait hears only what arrives after it is entered, and a message that belongs to another run is never its. A
 * failure is an outcome like any other, which the next link may take by type; an outcome no link takes ends the run.
 *
 * <h2>Bounds</h2>
 *
 * <p><b>Nothing waits forever unless the chain says so.</b> Every wait, and every step that {@link Out#hold holds} its
 * run, is bounded: by {@link Builder#within} on the link, else the chain's {@link Builder#boundedBy}, else the tick's
 * {@link Tick#defaultBound}. {@link Builder#forever} says it may wait forever. A bound that runs out is the wait's
 * failure; one that was the tick's default is also a bug, which the tick's {@link Tick.Overdue} policy hears of. A
 * chain used as a branch, and a wait over nothing but chains, has no bound of its own unless one is stated: each of its
 * own waits is bounded, so it ends.
 *
 * <p>Why the stack wants all of this is vexelray-framework's {@code docs/plans/chains.md}; the rulings this follows are
 * its section <i>Ruled 2026-11-02: chains as trees, with bounded waits</i>.
 *
 * @param <T> the state type of the first step's ledger
 * @param <A> the type of the arguments a run starts with
 * @param <R> what a run ends with, when it ends at its last link: what it arrives with as a {@link Branch}
 */
public final class Chain<T, A, R> {

    /** Run ids, unique across chains so a trace never confuses two. Zero means "not in a run". */
    private static final AtomicLong RUNS = new AtomicLong();

    private final String name;
    private final Link[] links;
    private final Ledger<T> first;
    private final Class<R> ends;
    /** The chain's own default bound ({@link Builder#boundedBy}), or null for the tick's. */
    private final Bound byDefault;
    private final String stepName; // the probe's mark for a step taken, built once

    private Chain(String name, Ledger<T> first, List<Link> links, Class<R> ends, Bound byDefault) {
        this.name = name;
        this.first = first;
        this.links = links.toArray(new Link[0]);
        this.ends = ends;
        this.byDefault = byDefault;
        this.stepName = Probe.ON ? "chain " + name : null;
    }

    /** A fresh run id: a chain run's, or the group of a change made in none, so the two never collide. */
    static long nextRunId() {
        return RUNS.incrementAndGet();
    }

    /** Begin a chain whose runs start with {@code step} on {@code ledger}. */
    public static <T, A> Builder<T, A> start(String name, Ledger<T> ledger, Step<T, A> step) {
        Builder<T, A> b = new Builder<>(Objects.requireNonNull(name, "name"));
        b.links.add(new Steps());
        return b.alternative(Object.class, ledger, step);
    }

    /** This chain's name, for logs and traces. */
    public String name() {
        return name;
    }

    /** What a run ends with, when it reaches the end: what it arrives with as a branch. */
    public Class<R> ends() {
        return ends;
    }

    /** Where a run is started: the first step's ledger's topic. */
    public Topic<Step.Transaction<T>> topic() {
        return first.topic();
    }

    /** A new run, starting with {@code args}: the message that, sent to {@link #topic()}, starts it. */
    public Step.Transaction<T> with(A args) {
        long id = nextRunId();
        return start(args, new Run(this, id, 0, Life.root(id)));
    }

    /** This chain as a branch of another chain's wait: see {@link Branch.OfChain}. */
    public Branch.OfChain<R> asBranch() {
        return new Branch.OfChain<>(this, null);
    }

    /** A run of this chain as a child of a waiting run, starting with what that wait was entered with. */
    Step.Transaction<T> child(Object args, Life life) {
        return start(args, new Run(this, nextRunId(), 0, life));
    }

    @SuppressWarnings("unchecked")
    private Step.Transaction<T> start(Object args, Run run) {
        Steps opening = (Steps) links[0];
        return new Step.Transaction<>((Step<T, ?>) opening.alternatives.get(0).step, args, run);
    }

    /**
     * Carry {@code run} on with {@code outcome}, from the link at its position: into the next link, from inside a
     * delivery to {@code owner}, or end the run if there is no next link or it does not take the outcome. A run that
     * has been cut goes nowhere.
     */
    void advance(Run run, Object outcome, Inbox owner) {
        if (run.life().isCut()) {
            if (Probe.ON) {
                Probe.mark(Lane.BUS, stepName, "cut", run.id());
            }
            return;
        }
        int next = run.position() + 1;
        if (next < links.length && links[next].enter(this, run.at(next), outcome, owner)) {
            return;
        }
        ended(run, outcome, owner);
    }

    /** The run ended with {@code outcome}: a child run's arrives at the wait that started it. */
    private void ended(Run run, Object outcome, Inbox owner) {
        if (Probe.ON) {
            Probe.mark(Lane.BUS, stepName, "ends", run.id());
        }
        Life.Parent parent = run.life().parent;
        if (parent != null) {
            owner.post(parent.waits().signals, new Waits.Arrived(parent.token(), parent.branch(), outcome));
        }
    }

    /** The bound stated for the link at {@code position}, or the chain's default, or null for the tick's. */
    Bound stated(int position) {
        Bound b = links[position].bound;
        return b != null ? b : byDefault;
    }

    /** What the link at {@code position} is called in a {@link TimedOut} or a trace. */
    String labelAt(int position) {
        return links[position].label();
    }

    String stepName() {
        return stepName;
    }

    @Override
    public String toString() {
        return "Chain[" + name + ", " + links.length + " links]";
    }

    // --- runs -------------------------------------------------------------

    /** Where a transaction stands in a run: which chain, which run, which link, and the run's life. */
    record Run(Chain<?, ?, ?> chain, long id, int position, Life life) {
        Run at(int p) {
            return new Run(chain, id, p, life);
        }
    }

    /**
     * What every link of one run shares: the undo group it changes in (the root run's id, so a child's changes go
     * with its parent's), the wait it reports to when it is a child, and whether it has been cut off.
     */
    static final class Life {
        final long group;
        final Parent parent;
        /** Set by the lane of the wait that cut it; read by later rounds, which the tick's rounds order after it. */
        private volatile boolean cut;

        private Life(long group, Parent parent) {
            this.group = group;
            this.parent = parent;
        }

        static Life root(long id) {
            return new Life(id, null);
        }

        static Life child(Parent parent) {
            return new Life(parent.life().group, parent);
        }

        /** Whether this run, or any run it is a child of, has been cut off. */
        boolean isCut() {
            for (Life l = this; l != null; l = l.parent == null ? null : l.parent.life()) {
                if (l.cut) {
                    return true;
                }
            }
            return false;
        }

        void cutOff() {
            cut = true;
        }

        /** The wait a child run reports to: its inbox's waits, which wait, which branch, and the waiting run's life. */
        record Parent(Waits waits, long token, int branch, Life life) {
        }
    }

    /**
     * A held run, kept by the step that held it and handed to {@link Out#resume} by the step that resumes it. It names
     * the run and where it stands; it goes on once, and a resume after its bound ran out, or a second one, is refused
     * and traced rather than taken twice.
     */
    public static final class Resume {

        final Run run;
        final Waits.Hold hold;

        Resume(Run run, Waits.Hold hold) {
            this.run = run;
            this.hold = hold;
        }

        /** The run this resumes. */
        public long run() {
            return run.id();
        }

        @Override
        public String toString() {
            return "Resume[" + run.chain().name + " run " + run.id() + " after link " + run.position() + "]";
        }
    }

    /** Make the record an {@link Builder#all ALL} goes on with, from its two branches' arrivals. */
    @FunctionalInterface
    public interface Join2<X, Y, J> {
        J join(X x, Y y);
    }

    /** Make the record an {@link Builder#all ALL} goes on with, from its three branches' arrivals. */
    @FunctionalInterface
    public interface Join3<X, Y, Z, J> {
        J join(X x, Y y, Z z);
    }

    // --- links ------------------------------------------------------------

    /** One link: a step with its alternatives, or a wait. */
    abstract static class Link {
        /** The bound stated with within(...) or forever(), or null. */
        Bound bound;

        /** Take {@code run} into this link with {@code outcome}, or say it does not take it. In a delivery to owner. */
        abstract boolean enter(Chain<?, ?, ?> chain, Run run, Object outcome, Inbox owner);

        abstract String label();
    }

    private record Alternative(Class<?> on, Ledger<?> ledger, Step<?, ?> step) {
    }

    /** A step link: the first alternative whose type takes the outcome runs, on its own ledger, in a later round. */
    static final class Steps extends Link {
        final List<Alternative> alternatives = new ArrayList<>();

        @Override
        boolean enter(Chain<?, ?, ?> chain, Run run, Object outcome, Inbox owner) {
            for (Alternative alt : alternatives) {
                if (alt.on.isInstance(outcome)) {
                    if (Probe.ON) {
                        Probe.mark(Lane.BUS, chain.stepName, alt.step.name(), run.id());
                    }
                    owner.post(alt.ledger.topic(), new Step.Transaction<>(alt.step, outcome, run));
                    return true;
                }
            }
            return false;
        }

        @Override
        String label() {
            StringBuilder s = new StringBuilder();
            for (Alternative alt : alternatives) {
                s.append(s.length() == 0 ? "" : " or ").append(alt.step.name());
            }
            return s.toString();
        }
    }

    /**
     * A wait: entered by any outcome of the link before it, on the chain's first ledger's inbox, in the round after
     * (which is what makes a wait hear only what arrives after it is entered). Each kind opens its own
     * {@link Waits.Open}.
     */
    abstract static class Wait extends Link {
        final Branch<?>[] branches;
        /** The first ledger's inbox's waits, where this wait listens and its runs wait. Set when the chain is built. */
        Waits anchor;

        Wait(Branch<?>[] branches) {
            this.branches = branches;
        }

        @Override
        boolean enter(Chain<?, ?, ?> chain, Run run, Object outcome, Inbox owner) {
            owner.post(anchor.signals, new Waits.Enter(this, run, outcome));
            return true;
        }

        /** Whether the tick's default bounds this wait when nothing is stated: a wait on any message does. */
        boolean defaultable() {
            for (Branch<?> b : branches) {
                if (b instanceof Branch.Message<?>) {
                    return true;
                }
            }
            return false;
        }

        /** Listen, on the anchor, for every topic this wait may hear. At build, before the tick runs. */
        void listen() {
            for (int i = 0; i < branches.length; i++) {
                if (branches[i] instanceof Branch.Message<?> m) {
                    anchor.listen(m.topic);
                }
            }
        }

        abstract Waits.Open open(Run run);

        String branchList() {
            StringBuilder s = new StringBuilder();
            for (Branch<?> b : branches) {
                s.append(s.length() == 0 ? "" : ", ").append(b.label());
            }
            return s.toString();
        }

        void check(String chain) {
            Set<Object> seen = new HashSet<>();
            for (Branch<?> b : branches) {
                Object key = b instanceof Branch.Message<?> m ? m.topic : ((Branch.OfChain<?>) b).chain;
                if (!seen.add(key)) {
                    throw new IllegalArgumentException("chain '" + chain + "': " + label() + " waits for "
                            + b.label() + " twice; a wait's branches are distinct");
                }
            }
        }
    }

    static final class AnyWait extends Wait {
        AnyWait(Branch<?>[] branches) {
            super(branches);
        }

        @Override
        Waits.Open open(Run run) {
            return new Waits.AnyOpen(this, run);
        }

        @Override
        String label() {
            return "any(" + branchList() + ")";
        }
    }

    static final class AllWait extends Wait {
        final Join join;

        AllWait(Branch<?>[] branches, Join join) {
            super(branches);
            this.join = join;
        }

        @Override
        Waits.Open open(Run run) {
            return new Waits.AllOpen(this, run);
        }

        @Override
        String label() {
            return "all(" + branchList() + ")";
        }
    }

    static final class NoneWait extends Wait {
        /** The message that closes the window, when it is bounded by one rather than by a time. */
        Topic<?> before;

        NoneWait(Branch<?>[] branches) {
            super(branches);
        }

        @Override
        void listen() {
            super.listen();
            if (before != null) {
                anchor.listen(before);
            }
        }

        @Override
        boolean defaultable() {
            return true;
        }

        @Override
        Waits.Open open(Run run) {
            return new Waits.NoneOpen(this, run);
        }

        @Override
        String label() {
            return "none(" + branchList() + ")" + (before == null ? "" : " before " + before.name());
        }

        @Override
        void check(String chain) {
            super.check(chain);
            for (Branch<?> b : branches) {
                if (!(b instanceof Branch.Message<?>)) {
                    throw new IllegalArgumentException("chain '" + chain + "': " + label() + " waits for a chain "
                            + "not to arrive, which says nothing a bound would not; a NONE's branches are messages");
                }
            }
            if (before == null && (bound == null || !bound.timed())) {
                throw new IllegalArgumentException("chain '" + chain + "': " + label() + " has no bound, and that "
                        + "nothing happened is only seen when a window closes: say within(time) or before(topic)");
            }
        }
    }

    /** An ALL's record, made from its branches' arrivals in order. */
    @FunctionalInterface
    interface Join {
        Object of(Object[] values);
    }

    // --- Builder ----------------------------------------------------------

    public static final class Builder<T, A> {
        private final String name;
        private final List<Link> links = new ArrayList<>();
        private Ledger<T> first;
        private Topic<? extends A> heard;
        private Bound byDefault;

        private Builder(String name) {
            this.name = name;
        }

        /**
         * Also start a run on every message published on {@code topic}, with the message as the first step's
         * arguments, as {@link Ledger.Builder#on} runs a step: for a cause whose sender does not know the chain, such
         * as a view that is not a ledger. The first step's ledger's owner hears it, so the run starts in the same
         * delivery, in order with the owner's other messages.
         */
        public Builder<T, A> on(Topic<? extends A> topic) {
            this.heard = Objects.requireNonNull(topic, "topic");
            return this;
        }

        /**
         * Then, when the previous link's outcome is an {@code on}, run {@code step} on {@code ledger} with it: a new
         * link. An outcome this link does not take ends the run.
         *
         * @throws IllegalArgumentException if {@code step} is not declared on {@code ledger}
         */
        public <U, E> Builder<T, A> then(Class<E> on, Ledger<U> ledger, Step<U, ? super E> step) {
            links.add(new Steps());
            return alternative(Objects.requireNonNull(on, "on"), ledger, step);
        }

        /**
         * Or, when it is an {@code on} instead, run {@code step} on {@code ledger} with it: an alternative in the last
         * link, tried after the ones declared before it. The choice is by the type of one outcome; nothing races.
         *
         * @throws IllegalStateException    if the last link is not a step link made by then(...)
         * @throws IllegalArgumentException if {@code step} is not declared on {@code ledger}, or an earlier alternative
         *                                  in this link already takes every {@code on}, so this one could never run
         */
        public <U, E> Builder<T, A> or(Class<E> on, Ledger<U> ledger, Step<U, ? super E> step) {
            if (links.size() < 2 || !(last() instanceof Steps)) {
                throw new IllegalStateException("chain '" + name + "': or(...) adds to a link made by then(...); a "
                        + "run's first step is the one it starts with, and a wait has branches, not alternatives");
            }
            return alternative(Objects.requireNonNull(on, "on"), ledger, step);
        }

        /** Wait for the first message on any of {@code topics}: {@link #any(Branch[])}, each a {@link Branch#of}. */
        public Builder<T, A> any(Topic<?>... topics) {
            return any(messages(topics));
        }

        /**
         * Wait for the first of {@code branches} to arrive, and go on with it; the rest are cut off. When the bound
         * runs out first it goes on with a {@link TimedOut}.
         */
        public Builder<T, A> any(Branch<?>... branches) {
            links.add(new AnyWait(branches(branches)));
            return this;
        }

        /**
         * Wait for none of {@code topics} to arrive before the bound, which must be stated: {@link #within} (and then
         * it goes on with {@link Quiet}) or {@link #before} (and then with that message). What arrives first breaks it,
         * and it goes on with {@link Broken}, which no link need take: the run then ends, as a debounce wants.
         */
        public Builder<T, A> none(Topic<?>... topics) {
            links.add(new NoneWait(messages(topics)));
            return this;
        }

        /** Wait for both: {@link #all(Branch, Branch)}, each a {@link Branch#of}. */
        public <X, Y> Joining2<T, A, X, Y> all(Topic<X> x, Topic<Y> y) {
            return all(Branch.of(x), Branch.of(y));
        }

        /** Wait for every one of two branches, then go on with the record {@link Joining2#as} makes of them. */
        public <X, Y> Joining2<T, A, X, Y> all(Branch<X> x, Branch<Y> y) {
            return new Joining2<>(this, branches(new Branch<?>[] {x, y}));
        }

        /** Wait for every one of three branches, then go on with the record {@link Joining3#as} makes of them. */
        public <X, Y, Z> Joining3<T, A, X, Y, Z> all(Branch<X> x, Branch<Y> y, Branch<Z> z) {
            return new Joining3<>(this, branches(new Branch<?>[] {x, y, z}));
        }

        /**
         * Bound the last link: a wait (for a NONE, its window), or how long a step of it may {@link Out#hold hold} the
         * run. When it runs out the link fails as its kind says.
         */
        public Builder<T, A> within(Duration bound) {
            needLink("within(...)").bound = Bound.of(bound);
            return this;
        }

        /** Close the last link's window with a message on {@code topic} rather than a time: a NONE only. */
        public Builder<T, A> before(Topic<?> topic) {
            if (!(last() instanceof NoneWait none)) {
                throw new IllegalStateException("chain '" + name + "': before(...) bounds a none(...)");
            }
            none.before = Objects.requireNonNull(topic, "topic");
            return this;
        }

        /** Let the last link wait forever: no default bounds it. Visible as a choice, which is the point. */
        public Builder<T, A> forever() {
            needLink("forever()").bound = Bound.FOREVER;
            return this;
        }

        /** Bound every link of this chain that states nothing by {@code bound}, rather than by the tick's default. */
        public Builder<T, A> boundedBy(Duration bound) {
            this.byDefault = Bound.of(bound);
            return this;
        }

        /** Build a chain whose runs end with anything: as a branch it arrives with whatever it ended with. */
        public Chain<T, A, Object> build() {
            return build(Object.class);
        }

        /** Build a chain whose runs end with an {@code ends}: as a branch, a {@code Branch<R>}. */
        public <R> Chain<T, A, R> build(Class<R> ends) {
            Objects.requireNonNull(ends, "ends");
            Waits anchor = Waits.of(first.owner());
            for (Link link : links) {
                if (link instanceof Wait wait) {
                    wait.check(name);
                    wait.anchor = anchor;
                    wait.listen();
                } else {
                    for (Alternative alt : ((Steps) link).alternatives) {
                        Waits.of(alt.ledger.owner());   // where a step that holds keeps its bound
                    }
                }
            }
            Chain<T, A, R> chain = new Chain<>(name, first, links, ends, byDefault);
            if (heard != null) {
                first.owner().subscribe(heard, event -> first.run(chain.with(event)), first.capacity);
            }
            return chain;
        }

        void addAll(Branch<?>[] branches, Join join) {
            links.add(new AllWait(branches, join));
        }

        private Link last() {
            return links.get(links.size() - 1);
        }

        private Link needLink(String what) {
            if (links.size() < 2) {
                throw new IllegalStateException("chain '" + name + "': " + what + " bounds a link after the first; "
                        + "the first step is where a run starts, and starting is not waiting");
            }
            return last();
        }

        private Branch<?>[] messages(Topic<?>[] topics) {
            Branch<?>[] out = new Branch<?>[topics.length];
            for (int i = 0; i < topics.length; i++) {
                out[i] = Branch.of(topics[i]);
            }
            return branches(out);
        }

        private Branch<?>[] branches(Branch<?>[] branches) {
            if (branches.length == 0) {
                throw new IllegalArgumentException("chain '" + name + "': a wait waits for something");
            }
            if (links.isEmpty()) {
                throw new IllegalStateException("chain '" + name + "': a run starts with a step");
            }
            for (Branch<?> b : branches) {
                Objects.requireNonNull(b, "branch");
            }
            return branches.clone();
        }

        @SuppressWarnings("unchecked")
        private Builder<T, A> alternative(Class<?> on, Ledger<?> ledger, Step<?, ?> step) {
            Objects.requireNonNull(ledger, "ledger");
            Objects.requireNonNull(step, "step");
            if (!ledger.declares(step)) {
                throw new IllegalArgumentException("chain '" + name + "': step '" + step.name()
                        + "' is not declared on the ledger at '" + ledger.topic().name() + "'");
            }
            Steps link = (Steps) last();
            for (Alternative earlier : link.alternatives) {
                if (earlier.on.isAssignableFrom(on)) {
                    throw new IllegalArgumentException("chain '" + name + "': step '" + step.name() + "' takes "
                            + on.getSimpleName() + ", which step '" + earlier.step.name() + "' before it in the same "
                            + "link already takes as " + earlier.on.getSimpleName() + ", so it could never run");
                }
            }
            if (first == null) {
                first = (Ledger<T>) ledger;
            }
            link.alternatives.add(new Alternative(on, ledger, step));
            return this;
        }
    }

    /** An ALL over two branches, waiting for the record it goes on with. */
    public static final class Joining2<T, A, X, Y> {
        private final Builder<T, A> builder;
        private final Branch<?>[] branches;

        Joining2(Builder<T, A> builder, Branch<?>[] branches) {
            this.builder = builder;
            this.branches = branches;
        }

        /** Go on with {@code join} of the two arrivals: a record's constructor reference, which captures nothing. */
        @SuppressWarnings("unchecked")
        public Builder<T, A> as(Join2<? super X, ? super Y, ?> join) {
            Objects.requireNonNull(join, "join");
            builder.addAll(branches, v -> join.join((X) v[0], (Y) v[1]));
            return builder;
        }
    }

    /** An ALL over three branches, waiting for the record it goes on with. */
    public static final class Joining3<T, A, X, Y, Z> {
        private final Builder<T, A> builder;
        private final Branch<?>[] branches;

        Joining3(Builder<T, A> builder, Branch<?>[] branches) {
            this.builder = builder;
            this.branches = branches;
        }

        /** Go on with {@code join} of the three arrivals: a record's constructor reference, which captures nothing. */
        @SuppressWarnings("unchecked")
        public Builder<T, A> as(Join3<? super X, ? super Y, ? super Z, ?> join) {
            Objects.requireNonNull(join, "join");
            builder.addAll(branches, v -> join.join((X) v[0], (Y) v[1], (Z) v[2]));
            return builder;
        }
    }
}
