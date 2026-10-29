package sibarum.atchung;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import sibarum.probe.Lane;
import sibarum.probe.Probe;

/**
 * The path from a cause to its outcome, written once: a sequence of links, each a {@link Step} on its own
 * {@link Ledger}, where what one step gives as its outcome is the arguments of the next.
 *
 * <pre>{@code
 * Chain<ViewState, Click> close = Chain.start("close", view, closeClicked)      // → CloseAsked
 *         .then(CloseAsked.class,   editor,  closeRequested)                    // → PromptToSave, or Closed
 *         .then(PromptToSave.class, dialogs, askSave)                           // holds until the person answers
 *         .then(Save.class,         editor,  saveAndClose)                      // → Closed
 *         .or(DontSave.class,       editor,  discardAndClose)                   // → Closed; Cancel ends the run
 *         .then(Closed.class,       view,    closed)
 *         .build();
 * bus.publish(close.topic(), close.with(click));
 * }</pre>
 *
 * <ul>
 *   <li><b>A step gives its outcome</b> through its {@link Out} ({@link Out#result}), since what it returns is its
 *       ledger's next state. A step that gives none ends the run.</li>
 *   <li><b>The outcome's type picks the next step.</b> Each link after the first names the type it takes, and may have
 *       alternatives ({@link Builder#or}). An outcome is matched against a link's alternatives in the order they were
 *       declared, as a {@code catch} is, and the first it is an instance of runs. An outcome none of them takes ends
 *       the run there: {@code Closed} above ends a close that needed no prompt, and {@code Cancel} one the person
 *       called off.</li>
 *   <li><b>Branches join again.</b> Whichever alternative ran, its outcome goes on to the next link. A branch that
 *       should end there gives no outcome, or one the next link does not take.</li>
 *   <li><b>Going on is a send</b>, held in the owner's outbox with the step's other sends: the next step runs on its
 *       own ledger in a later round, and a step that throws takes its outcome back with everything else. Each step is
 *       atomic on its own ledger; the run as a whole is not, and other transactions run on each ledger between steps.</li>
 *   <li><b>A step that waits</b> (for a person, for a job) does not give its outcome. It {@link Out#hold holds} the
 *       run, keeps the {@link Resume} it gets in its state, and a later step on the same ledger
 *       {@link Out#resume resumes} it with the outcome when it arrives. A step never waits; the run does.</li>
 *   <li><b>Every run has an id</b> ({@link Out#run()}), the same in all of its steps, which is what tracing groups a
 *       cause's steps by and what undo will group a cause's changes by.</li>
 * </ul>
 *
 * <p>Why the stack wants this is vexelray-framework's {@code docs/plans/chains.md}.
 *
 * @param <T> the state type of the first step's ledger
 * @param <A> the type of the arguments a run starts with
 */
public final class Chain<T, A> {

    /** Run ids, unique across chains so a trace never confuses two. Zero means "not in a run". */
    private static final AtomicLong RUNS = new AtomicLong();

    private final String name;
    /** Each link's alternatives, in the order they were declared. The first link has one, which takes anything. */
    private final Alternative[][] links;
    private final Ledger<T> first;
    private final String stepName; // the probe's mark for a step taken, built once

    private Chain(String name, Ledger<T> first, List<List<Alternative>> links) {
        this.name = name;
        this.first = first;
        this.links = new Alternative[links.size()][];
        for (int i = 0; i < this.links.length; i++) {
            this.links[i] = links.get(i).toArray(new Alternative[0]);
        }
        this.stepName = Probe.ON ? "chain " + name : null;
    }

    /** A fresh run id: a chain run's, or the group of a change made in none, so the two never collide. */
    static long nextRunId() {
        return RUNS.incrementAndGet();
    }

    /** Begin a chain whose runs start with {@code step} on {@code ledger}. */
    public static <T, A> Builder<T, A> start(String name, Ledger<T> ledger, Step<T, A> step) {
        Builder<T, A> b = new Builder<>(Objects.requireNonNull(name, "name"));
        b.links.add(new ArrayList<>());
        return b.alternative(Object.class, ledger, step);
    }

    /** This chain's name, for logs and traces. */
    public String name() {
        return name;
    }

    /** Where a run is started: the first step's ledger's topic. */
    public Topic<Step.Transaction<T>> topic() {
        return first.topic();
    }

    /** A new run, starting with {@code args}: the message that, sent to {@link #topic()}, starts it. */
    @SuppressWarnings("unchecked")
    public Step.Transaction<T> with(A args) {
        Step<T, ?> step = (Step<T, ?>) links[0][0].step;
        return new Step.Transaction<>(step, args, new Run(this, nextRunId(), 0));
    }

    /**
     * Carry {@code run} on with {@code outcome}, from the step at its position: post the first of the next link's
     * alternatives that takes it into {@code owner}'s outbox, or end the run if there is no next link or none takes it.
     */
    void advance(Run run, Object outcome, Inbox owner) {
        int next = run.position() + 1;
        if (next < links.length) {
            for (Alternative alt : links[next]) {
                if (alt.on.isInstance(outcome)) {
                    if (Probe.ON) {
                        Probe.mark(Lane.BUS, stepName, alt.step.name(), run.id());
                    }
                    owner.post(alt.ledger.topic(),
                            new Step.Transaction<>(alt.step, outcome, new Run(this, run.id(), next)));
                    return;
                }
            }
        }
        if (Probe.ON) {
            Probe.mark(Lane.BUS, stepName, "ends", run.id());
        }
    }

    @Override
    public String toString() {
        return "Chain[" + name + ", " + links.length + " links]";
    }

    /** Where a transaction stands in a run: which chain, which run, which link. */
    record Run(Chain<?, ?> chain, long id, int position) {
    }

    /**
     * A held run, kept by the step that held it and handed to {@link Out#resume} by the step that resumes it. A value:
     * it names the run and where it stands, and holds no code. Resume a run once; resumed twice, it goes on twice.
     */
    public static final class Resume {

        final Run run;

        Resume(Run run) {
            this.run = run;
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

    private record Alternative(Class<?> on, Ledger<?> ledger, Step<?, ?> step) {
    }

    // --- Builder ----------------------------------------------------------

    public static final class Builder<T, A> {
        private final String name;
        private final List<List<Alternative>> links = new ArrayList<>();
        private Ledger<T> first;
        private Topic<? extends A> heard;

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
         * Then, when the previous step's outcome is an {@code on}, run {@code step} on {@code ledger} with it: a new
         * link. An outcome this link does not take ends the run.
         *
         * @throws IllegalArgumentException if {@code step} is not declared on {@code ledger}
         */
        public <U, E> Builder<T, A> then(Class<E> on, Ledger<U> ledger, Step<U, ? super E> step) {
            links.add(new ArrayList<>());
            return alternative(Objects.requireNonNull(on, "on"), ledger, step);
        }

        /**
         * Or, when it is an {@code on} instead, run {@code step} on {@code ledger} with it: an alternative in the last
         * link, tried after the ones declared before it.
         *
         * @throws IllegalStateException    if there is no link to add to: a run's first step is the one it starts with
         * @throws IllegalArgumentException if {@code step} is not declared on {@code ledger}, or an earlier alternative
         *                                  in this link already takes every {@code on}, so this one could never run
         */
        public <U, E> Builder<T, A> or(Class<E> on, Ledger<U> ledger, Step<U, ? super E> step) {
            if (links.size() < 2) {
                throw new IllegalStateException("chain '" + name + "': or(...) adds to a link made by then(...); a "
                        + "run's first step is the one it starts with");
            }
            return alternative(Objects.requireNonNull(on, "on"), ledger, step);
        }

        @SuppressWarnings("unchecked")
        private Builder<T, A> alternative(Class<?> on, Ledger<?> ledger, Step<?, ?> step) {
            Objects.requireNonNull(ledger, "ledger");
            Objects.requireNonNull(step, "step");
            if (!ledger.declares(step)) {
                throw new IllegalArgumentException("chain '" + name + "': step '" + step.name()
                        + "' is not declared on the ledger at '" + ledger.topic().name() + "'");
            }
            List<Alternative> link = links.get(links.size() - 1);
            for (Alternative earlier : link) {
                if (earlier.on.isAssignableFrom(on)) {
                    throw new IllegalArgumentException("chain '" + name + "': step '" + step.name() + "' takes "
                            + on.getSimpleName() + ", which step '" + earlier.step.name() + "' before it in the same "
                            + "link already takes as " + earlier.on.getSimpleName() + ", so it could never run");
                }
            }
            if (first == null) {
                first = (Ledger<T>) ledger;
            }
            link.add(new Alternative(on, ledger, step));
            return this;
        }

        public Chain<T, A> build() {
            Chain<T, A> chain = new Chain<>(name, first, links);
            if (heard != null) {
                first.owner().subscribe(heard, event -> first.run(chain.with(event)), first.capacity);
            }
            return chain;
        }
    }
}
