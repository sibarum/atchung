package sibarum.atchung;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A {@link State} owned by one component, changed only by {@link Step steps} sent to it as messages.
 *
 * <p>A {@code State} is changed by whoever calls {@link State#commit}, on their thread, by a CAS that may run the
 * mutation twice. A ledger turns that around: the change is a message, a {@link Step.Transaction} on the ledger's
 * {@link #topic()}, and the owner runs it. So:
 *
 * <ul>
 *   <li><b>One at a time, in order.</b> Transactions arrive in the owner's {@link Inbox} with its other messages, and
 *       each runs to the end before the next starts, on the owner's lane. A step reads the state the previous one
 *       left, and nothing changes it underneath.</li>
 *   <li><b>All or nothing.</b> What a step sends through its {@link Out} waits in the owner's outbox for the end of the
 *       round. If the step throws, its sends are discarded and nothing is committed; the throw then goes where any
 *       subscriber's throw goes ({@link Tick#run}).</li>
 *   <li><b>Reads cost no message.</b> Each step that changes the state commits a new {@link Versioned} snapshot, read
 *       from any thread through {@link #state()}.</li>
 *   <li><b>Only declared steps.</b> A ledger runs the steps declared on its builder and refuses any other, as a
 *       {@code State} refuses an undeclared committer.</li>
 * </ul>
 *
 * <p>The state's own committer is the ledger's and nobody else's, so nothing but a step can change it. Why the stack
 * wants this, and what is built on it (chains of steps across ledgers, undo by snapshot), is vexelray-framework's
 * {@code docs/plans/chains.md}.
 *
 * @param <T> the state type (should be immutable)
 */
public final class Ledger<T> {

    private final State<T> state;
    private final Committer<T, T> replace;
    private final Set<Step<T, ?>> declared;
    private final Topic<Step.Transaction<T>> topic;
    private final Inbox owner;
    /** How many messages each topic this ledger hears may hold: its own, and those it and its chains hear. */
    final int capacity;
    private final Out out;
    /** Where this ledger's changes are recorded to be undone, or null. */
    private final Undo undo;

    private Ledger(Builder<T> builder, Inbox owner, Topic<Step.Transaction<T>> topic, int capacity) {
        State.Builder<T> sb = State.of(builder.initial);
        if (builder.maxDepth > 0) {
            sb.history(builder.maxDepth);
        }
        this.replace = sb.mutation("ledger " + topic.name(), (prev, next) -> next);
        this.state = sb.build();
        this.declared = new HashSet<>(builder.steps);
        this.topic = topic;
        this.owner = owner;
        this.capacity = capacity;
        this.out = new Out(owner);
        this.undo = builder.undo;
        owner.subscribe(topic, this::run, capacity);
        for (Heard<T, ?> heard : builder.heard) {
            heard.subscribe(this, owner, capacity);
        }
    }

    /** Begin building a ledger with its initial value. */
    public static <T> Builder<T> of(T initial) {
        return new Builder<>(Objects.requireNonNull(initial, "initial"));
    }

    /** The state, for reading from any thread. Only this ledger's steps change it. */
    public State<T> state() {
        return state;
    }

    /** Where a transaction is sent to run here: publish it, or send it through a {@link Sender} for this topic. */
    public Topic<Step.Transaction<T>> topic() {
        return topic;
    }

    /** The inbox whose component owns this ledger, and on whose lane its steps run. */
    public Inbox owner() {
        return owner;
    }

    /** One transaction, on the owner's lane, inside a delivery to its inbox. */
    void run(Step.Transaction<T> transaction) {
        run(transaction.step(), transaction.args(), transaction.run);
    }

    /** One step, with its arguments and the run it is in (null for none). */
    private void run(Step<T, ?> step, Object args, Chain.Run run) {
        if (!declared.contains(step)) {
            throw new IllegalArgumentException("step not declared on the ledger at '" + topic.name() + "': "
                    + step.name());
        }
        int mark = owner.outSize;
        int betweenMark = owner.betweenSize;
        T next;
        long group;
        out.begin(run);
        try {
            next = step.applyTo(state.value(), args, out);
            if (next == null) {
                throw new NullPointerException("step '" + step.name() + "' returned no state; a step that changes "
                        + "nothing returns the state it was given");
            }
            if (out.outcome != null) {
                run.chain().advance(run, out.outcome, owner);
            }
            group = out.group();
        } catch (RuntimeException | Error e) {
            owner.discardFrom(mark);
            owner.discardBetweenFrom(betweenMark);
            throw e;
        } finally {
            out.end();
        }
        if (undo == null) {
            state.commitIfChanged(replace, next);
            return;
        }
        Versioned<T> before = state.current();
        if (state.commitIfChanged(replace, next)) {
            undo.record(this, group == 0 ? Chain.nextRunId() : group, before, state.current(), owner);
        }
    }

    /**
     * Set the state to {@code value}, as undo and redo do: on the coordinator, between rounds, when no step of this
     * ledger is running. Not recorded, since it is the record being walked.
     */
    @SuppressWarnings("unchecked")
    void restore(Object value) {
        state.commit(replace, (T) value);
    }

    /** Whether {@code step} was declared on this ledger's builder. */
    boolean declares(Step<?, ?> step) {
        return declared.contains(step);
    }

    @Override
    public String toString() {
        return "Ledger[" + topic.name() + " on " + owner.name + "]";
    }

    /** A topic the owner hears, and the step each message on it runs. */
    private record Heard<T, E>(Topic<E> topic, Step<T, ? super E> step) {
        void subscribe(Ledger<T> ledger, Inbox owner, int capacity) {
            owner.subscribe(topic, event -> ledger.run(step, event, null), capacity);
        }
    }

    // --- Builder ----------------------------------------------------------

    public static final class Builder<T> {
        private final T initial;
        private final List<Step<T, ?>> steps = new ArrayList<>();
        private final List<Heard<T, ?>> heard = new ArrayList<>();
        private int maxDepth;
        private Undo undo;

        private Builder(T initial) {
            this.initial = initial;
        }

        /** Declare a step. Hold the returned handle, and send {@link Step#with} its arguments to run it. */
        public <P> Step<T, P> step(String name, Step.Body<T, P> body) {
            Step<T, P> step = new Step<>(Objects.requireNonNull(name, "name"), Objects.requireNonNull(body, "body"));
            steps.add(step);
            return step;
        }

        /**
         * Run {@code step} on every message published on {@code topic}, with the message as its arguments: for a
         * message whose sender does not know this ledger, such as a job's reply or a view's report. The receiver
         * declares what it hears, as a subscription does, so the sender stays a plain publisher. The step runs as one
         * sent on its own would, in no chain run; a step that answers a held run {@link Out#resume resumes} it.
         *
         * @throws IllegalArgumentException if {@code step} was not declared on this builder
         */
        public <E> Builder<T> on(Topic<E> topic, Step<T, ? super E> step) {
            Objects.requireNonNull(topic, "topic");
            if (!steps.contains(Objects.requireNonNull(step, "step"))) {
                throw new IllegalArgumentException("step '" + step.name() + "' heard on '" + topic.name()
                        + "' is not declared on this builder");
            }
            heard.add(new Heard<>(topic, step));
            return this;
        }

        /** Keep the last {@code maxDepth} versions, as {@link State.Builder#history(int)}. 0 (default) keeps none. */
        public Builder<T> history(int maxDepth) {
            this.maxDepth = maxDepth;
            return this;
        }

        /**
         * Record this ledger's changes in {@code undo}, so they can be undone: each change under the chain run it was
         * part of, or on its own when it was in none.
         */
        public Builder<T> undoable(Undo undo) {
            this.undo = Objects.requireNonNull(undo, "undo");
            return this;
        }

        /**
         * Build the ledger, owned by {@code owner}: its transactions arrive on a topic named {@code topicName}, which
         * holds at most {@code capacity} of them, as {@link Inbox#subscribe(Topic, Subscriber, int)} describes.
         */
        public Ledger<T> build(Inbox owner, String topicName, int capacity) {
            Objects.requireNonNull(owner, "owner");
            @SuppressWarnings({"unchecked", "rawtypes"})
            Class<Step.Transaction<T>> type = (Class) Step.Transaction.class;
            return new Ledger<>(this, owner, Topic.edge(Objects.requireNonNull(topicName, "topicName"), type),
                    capacity);
        }
    }
}
