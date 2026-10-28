package sibarum.atchung;

import java.util.Objects;

/**
 * A typed, pre-declared step: the only way to change a {@link Ledger}'s state. Obtained from
 * {@link Ledger.Builder#step(String, Body)}, and sent to the ledger as a message with {@link #with}.
 *
 * <p>A step is to a ledger what a {@link Committer} is to a {@link State}, with two differences that make it a
 * message rather than a call. It runs on the ledger's owner, during a delivery to the owner's {@link Inbox}, so it
 * runs once and never races; and it may send, through the {@link Out} it is handed, which the round carries on only if
 * the step returns.
 *
 * <p>The {@code name} is the step's identity in a log or a replay: a transaction is a step's name and its arguments,
 * both of which can be written down.
 *
 * @param <T> the ledger's state type
 * @param <P> the type of the arguments a transaction of this step carries
 */
public final class Step<T, P> {

    /**
     * What a step does: the state before, the transaction's arguments and the ledger's {@link Out}, to the state
     * after. Must not mutate {@code state} (a ledger's values are immutable), and should have no effect but its return
     * and its sends: a file written or a thread started here happens even when a later send in the same step throws.
     */
    @FunctionalInterface
    public interface Body<T, P> {
        T apply(T state, P args, Out out);
    }

    private final String name;
    private final Body<T, P> body;

    Step(String name, Body<T, P> body) {
        this.name = name;
        this.body = body;
    }

    /** @return the stable declaration name (for diagnostics, logs and replay). */
    public String name() {
        return name;
    }

    /** This step with {@code args}: the message that, sent to the ledger's {@link Ledger#topic()}, runs it. */
    public Transaction<T> with(P args) {
        return new Transaction<>(this, args);
    }

    T applyTo(T state, Object args, Out out) {
        @SuppressWarnings("unchecked")
        P typed = (P) args;
        return body.apply(state, typed, out);
    }

    @Override
    public String toString() {
        return "Step[" + name + "]";
    }

    /**
     * One step with its arguments: the message a {@link Ledger} runs. Made by {@link Step#with}, never directly, so the
     * arguments are always of the step's type.
     *
     * @param <T> the ledger's state type
     */
    public static final class Transaction<T> {

        private final Step<T, ?> step;
        private final Object args;

        Transaction(Step<T, ?> step, Object args) {
            this.step = Objects.requireNonNull(step, "step");
            this.args = args;
        }

        /** @return the step this runs. */
        public Step<T, ?> step() {
            return step;
        }

        /** @return the arguments it runs with. */
        public Object args() {
            return args;
        }

        @Override
        public String toString() {
            return step.name + "(" + args + ")";
        }
    }
}
