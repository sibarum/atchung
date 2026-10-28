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
    private final Out out;

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
        this.out = new Out(owner);
        owner.subscribe(topic, this::run, capacity);
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
    private void run(Step.Transaction<T> transaction) {
        Step<T, ?> step = transaction.step();
        if (!declared.contains(step)) {
            throw new IllegalArgumentException("step not declared on the ledger at '" + topic.name() + "': "
                    + step.name());
        }
        int mark = owner.outSize;
        T next;
        out.begin(transaction.run);
        try {
            next = step.applyTo(state.value(), transaction.args(), out);
            if (next == null) {
                throw new NullPointerException("step '" + step.name() + "' returned no state; a step that changes "
                        + "nothing returns the state it was given");
            }
            if (out.outcome != null) {
                transaction.run.chain().advance(transaction.run, out.outcome, owner);
            }
        } catch (RuntimeException | Error e) {
            owner.discardFrom(mark);
            throw e;
        } finally {
            out.end();
        }
        state.commitIfChanged(replace, next);
    }

    /** Whether {@code step} was declared on this ledger's builder. */
    boolean declares(Step<?, ?> step) {
        return declared.contains(step);
    }

    @Override
    public String toString() {
        return "Ledger[" + topic.name() + " on " + owner.name + "]";
    }

    // --- Builder ----------------------------------------------------------

    public static final class Builder<T> {
        private final T initial;
        private final List<Step<T, ?>> steps = new ArrayList<>();
        private int maxDepth;

        private Builder(T initial) {
            this.initial = initial;
        }

        /** Declare a step. Hold the returned handle, and send {@link Step#with} its arguments to run it. */
        public <P> Step<T, P> step(String name, Step.Body<T, P> body) {
            Step<T, P> step = new Step<>(Objects.requireNonNull(name, "name"), Objects.requireNonNull(body, "body"));
            steps.add(step);
            return step;
        }

        /** Keep the last {@code maxDepth} versions, as {@link State.Builder#history(int)}. 0 (default) keeps none. */
        public Builder<T> history(int maxDepth) {
            this.maxDepth = maxDepth;
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
