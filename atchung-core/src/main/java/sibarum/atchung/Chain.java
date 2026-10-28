package sibarum.atchung;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import sibarum.probe.Lane;
import sibarum.probe.Probe;

/**
 * The path from a cause to its outcome, written once: a sequence of {@link Step steps}, each on its own
 * {@link Ledger}, where what one step gives as its outcome is the arguments of the next.
 *
 * <pre>{@code
 * Chain<ViewState, Click> close = Chain.start("close", view, closeClicked)      // → CloseAsked
 *         .then(CloseAsked.class,   editor,  closeRequested)                    // → PromptToSave, or Closed
 *         .then(PromptToSave.class, dialogs, askSave)                           // holds until the person answers
 *         .then(Answer.class,       editor,  finishClose)
 *         .build();
 * bus.publish(close.topic(), close.with(click));
 * }</pre>
 *
 * <ul>
 *   <li><b>A step gives its outcome</b> through its {@link Out} ({@link Out#result}), since what it returns is its
 *       ledger's next state. A step that gives none ends the run.</li>
 *   <li><b>The outcome's type picks whether the run goes on.</b> Each link after the first names the type it takes; an
 *       outcome of any other type ends the run there. So {@code Closed} above ends a close that needed no prompt.</li>
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
    private final Link[] links;
    private final Ledger<T> first;
    private final String stepName; // the probe's mark for a step taken, built once

    private Chain(String name, Ledger<T> first, List<Link> links) {
        this.name = name;
        this.first = first;
        this.links = links.toArray(new Link[0]);
        this.stepName = Probe.ON ? "chain " + name : null;
    }

    /** Begin a chain whose runs start with {@code step} on {@code ledger}. */
    public static <T, A> Builder<T, A> start(String name, Ledger<T> ledger, Step<T, A> step) {
        return new Builder<T, A>(Objects.requireNonNull(name, "name")).link(null, ledger, step);
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
        Step<T, ?> step = (Step<T, ?>) links[0].step;
        return new Step.Transaction<>(step, args, new Run(this, RUNS.incrementAndGet(), 0));
    }

    /**
     * Carry {@code run} on with {@code outcome}, from the step at its position: post the next step into
     * {@code owner}'s outbox, or end the run if there is none or the outcome is not its type.
     */
    void advance(Run run, Object outcome, Inbox owner) {
        int next = run.position() + 1;
        if (next >= links.length || !links[next].on.isInstance(outcome)) {
            if (Probe.ON) {
                Probe.mark(Lane.BUS, stepName, "ends", run.id());
            }
            return;
        }
        Link link = links[next];
        if (Probe.ON) {
            Probe.mark(Lane.BUS, stepName, link.step.name(), run.id());
        }
        owner.post(link.ledger.topic(), new Step.Transaction<>(link.step, outcome, new Run(this, run.id(), next)));
    }

    @Override
    public String toString() {
        return "Chain[" + name + ", " + links.length + " steps]";
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
            return "Resume[" + run.chain().name + " run " + run.id() + " after step " + run.position() + "]";
        }
    }

    private record Link(Class<?> on, Ledger<?> ledger, Step<?, ?> step) {
    }

    // --- Builder ----------------------------------------------------------

    public static final class Builder<T, A> {
        private final String name;
        private final List<Link> links = new ArrayList<>();
        private Ledger<T> first;

        private Builder(String name) {
            this.name = name;
        }

        /**
         * Then, when the previous step's outcome is an {@code on}, run {@code step} on {@code ledger} with it. An
         * outcome of another type ends the run.
         *
         * @throws IllegalArgumentException if {@code step} is not declared on {@code ledger}
         */
        public <U, E> Builder<T, A> then(Class<E> on, Ledger<U> ledger, Step<U, ? super E> step) {
            return link(Objects.requireNonNull(on, "on"), ledger, step);
        }

        @SuppressWarnings("unchecked")
        private Builder<T, A> link(Class<?> on, Ledger<?> ledger, Step<?, ?> step) {
            Objects.requireNonNull(ledger, "ledger");
            Objects.requireNonNull(step, "step");
            if (!ledger.declares(step)) {
                throw new IllegalArgumentException("chain '" + name + "': step '" + step.name()
                        + "' is not declared on the ledger at '" + ledger.topic().name() + "'");
            }
            if (first == null) {
                first = (Ledger<T>) ledger;
            }
            links.add(new Link(on, ledger, step));
            return this;
        }

        public Chain<T, A> build() {
            return new Chain<>(name, first, links);
        }
    }
}
